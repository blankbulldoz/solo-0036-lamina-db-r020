#!/usr/bin/env python3
"""Bounded production walkthroughs for row expiry and retention choices.

Each workflow uses only the public HTTP service. Run after building:
    go build -o bin/laminad ./cmd/laminad
    python3 examples/expiry_walkthrough.py
"""
import argparse, json, pathlib, tempfile
from walkthrough import Session, require

SCHEMA = {'name': 'spectra', 'primary': 'sample', 'columns': [
    {'name': 'sample', 'type': 'int'},
    {'name': 'band', 'type': 'text'},
    {'name': 'amplitude', 'type': 'int', 'nullable': True}]}


def row(n, amplitude=None):
    return {'sample': n, 'band': f'b{n}', 'amplitude': n * 10 if amplitude is None else amplitude}


def seed(s):
    s.call('POST', '/relations', SCHEMA, 201)
    tx = s.begin()
    for n in range(1, 4):
        s.put(tx, row(n))
    return s.commit(tx)['sequence']


def put_expiring(s, n, ttl=None, expires_at=None, amplitude=None):
    body = {'relation': 'spectra', 'row': row(n, amplitude=amplitude)}
    if ttl is not None:
        body['expiry'] = {'ttl': ttl}
    elif expires_at is not None:
        body['expiry'] = {'expires_at': expires_at}
    tx = s.begin()
    s.call('POST', f'/transactions/{tx}/put', body)
    return s.commit(tx)['sequence']


def advance(s, n, amplitude=None):
    tx = s.begin(); s.put(tx, row(n, amplitude=amplitude)); return s.commit(tx)['sequence']


def answer(s, at=None):
    """Complete visible result for a snapshot, stable across restarts."""
    return sorted((r['sample'], r['amplitude']) for r in s.query(at=at)['rows'])


def expiry_visibility(s):
    """Expiry is decided per snapshot in commit-sequence space."""
    base = seed(s)
    committed = put_expiring(s, 10, ttl=2)
    # Advance the head so snapshots after the commit sequence can be queried.
    advance(s, 20); advance(s, 21)

    def present(sample, at):
        return any(r['sample'] == sample for r in s.query(at=at)['rows'])

    # Readable at the committing sequence and through sequence committed+1.
    require(present(10, committed), 'row missing at its commit sequence')
    require(present(10, committed + 1), 'row expired too early')
    require(not present(10, committed + 2), 'row did not expire')
    # A newer write resurrects the key permanently; an older expiry must not
    # hide it, and history before the expiry is unchanged.
    later = advance(s, 10, amplitude=777)
    newest = next(r for r in s.query()['rows'] if r['sample'] == 10)
    require(newest['amplitude'] == 777, 'newer version hidden by old expiry')
    historical = next(r for r in s.query(at=committed)['rows'] if r['sample'] == 10)
    require(historical['amplitude'] == 100, 'history changed by later expiry')
    return {'base': base, 'committed': committed, 'later': later, 'expired': not present(10, committed + 2)}


def expiry_restart(s):
    """The same historical queries answer identically across restart."""
    seed(s)
    put_expiring(s, 9, expires_at=999)      # far-future absolute expiry
    put_expiring(s, 10, ttl=2)              # already expired shortly after
    advance(s, 11)
    head = s.stats()['sequence']
    snapshots = {at: answer(s, at) for at in range(2, head + 1)}
    stats = s.stats()
    s.call('POST', '/checkpoint')
    s.stop(); s.start()
    again = {at: answer(s, at) for at in range(2, head + 1)}
    require(snapshots == again, 'restart changed historical expiry answers')
    require(s.stats()['expired_versions'] == stats['expired_versions'], 'expiry facts changed on restart')
    return {'snapshots': len(snapshots), 'expired_versions': stats['expired_versions'], 'stable': True}


def expiry_vacuum_split(s):
    """Expiry state and physical reclaimability are separately observable."""
    seed(s)
    put_expiring(s, 5, ttl=1, amplitude=50)
    advance(s, 5, amplitude=60)             # permanent overwrite
    require(s.query()['matched'] == 4, 'seed rows missing')
    stats = s.stats()
    # The first version is expired, yet the newer permanent version is current.
    require(stats['expired_versions'] >= 1, 'expired version not observable')
    require(stats['reclaimable_versions'] >= 1, 'reclaimable version not observable')
    preview = s.call('POST', '/vacuum', {'dry_run': True})
    require(preview['dry_run'] and preview['removed'] == stats['reclaimable_versions'],
            'dry-run diverges from stats')
    require(s.query()['matched'] == 4, 'vacuum preview changed data')
    applied = s.call('POST', '/vacuum', {})
    require(applied['removed'] >= 1, 'vacuum did not reclaim')
    # Dry run is an exact preview of applying the same vacuum.
    require({k: v for k, v in applied.items() if k != 'dry_run'} ==
            {k: v for k, v in preview.items() if k != 'dry_run'}, 'dry-run and applied differ')
    require(s.stats()['reclaimable_versions'] == 0, 'reclaimable space not released')
    return {'stats': stats, 'preview': preview, 'applied': applied}


def expiry_retention_pin(s):
    """A long reader and a retention window each lower the reclaimable floor."""
    seed(s)
    reader = s.begin()
    advance(s, 10); advance(s, 11)
    requested = s.stats()['sequence']
    pinned = s.call('POST', '/vacuum', {'horizon': requested})
    require(pinned['pinned'] and pinned['pinned_by_reader'], 'reader did not pin vacuum')
    require(pinned['pinned_by_snapshot'] == 2, 'pinning snapshot not reported')
    require(s.query(transaction=reader)['matched'] == 3, 'reader lost its snapshot')
    s.call('POST', f'/transactions/{reader}/rollback')
    retained = s.call('POST', '/vacuum', {'horizon': requested, 'retain': requested})
    require(retained['limited_by_retention'], 'retention window not honoured')
    require(retained['horizon'] < requested, 'retention window did not lower floor')
    require(s.query(at=2)['matched'] == 3, 'retention window broke history')
    released = s.call('POST', '/vacuum', {'horizon': requested})
    require(not released['pinned'] and released['horizon'] == requested, 'floor did not advance')
    return {'pinned': pinned, 'retained': retained, 'released': released}


def expiry_index_consistency(s):
    """Indexed reads agree with scans across an expiry boundary."""
    seed(s)
    s.call('POST', '/indexes', {'name': 'by_amp', 'relation': 'spectra', 'column': 'amplitude'}, 201)
    at = put_expiring(s, 6, ttl=1, amplitude=10)
    # Advance the head past the expiry boundary; the version is still retained.
    advance(s, 7, amplitude=70)
    where = [{'column': 'amplitude', 'op': '<=', 'value': 10}]
    before = s.query(at=at, where=where)
    before_scan = s.query(at=at, where=where, force_scan=True)
    require(before['rows'] == before_scan['rows'], 'index and scan disagree before expiry')
    later = s.query(where=where)
    later_scan = s.query(where=where, force_scan=True)
    require(later['rows'] == later_scan['rows'], 'index and scan disagree after expiry')
    require(before['plan']['access'] == 'index', 'historical query did not use the index')
    require(len(before['rows']) == len(later['rows']) + 1, 'expiry not reflected in index path')
    return {'before': before['plan'], 'later': later['plan'], 'at': at}


def expiry_changes(s):
    """Change records carry resolved expiry so consumers replay the same state."""
    seed(s)
    committed = put_expiring(s, 8, ttl=3)
    page = s.call('GET', f'/changes?after={committed - 1}&limit=1')
    record = page['records'][0]
    require(record['format'] == 2, 'change record format not advanced')
    require(record['writes'][0]['expires_at'] == committed + 3, 'change feed lost resolved expiry')
    # Replaying the record reproduces visibility at each snapshot without any clock.
    expires = record['writes'][0]['expires_at']
    while s.stats()['sequence'] < expires:
        advance(s, 100 + s.stats()['sequence'])

    def present(at):
        return any(r['sample'] == 8 for r in s.query(at=at)['rows'])

    require(present(expires - 1), 'row missing before expiry')
    require(not present(expires), 'row still visible at expiry sequence')
    return {'record': record, 'committed': committed}


def expiry_txn_snapshot(s):
    """A transaction snapshot pins the expiry answer it started with."""
    seed(s)
    reader = s.begin(True)                  # snapshot 2, before the expiring write
    committed = put_expiring(s, 9, ttl=1)   # valid at sequence 3, expired from 4
    pinned = s.begin(True)                  # snapshot 3
    advance(s, 50); advance(s, 51)          # head moves past the expiry boundary
    require(not any(r['sample'] == 9 for r in s.query(transaction=reader)['rows']),
            'snapshot before insert saw the expiring row')
    require(any(r['sample'] == 9 for r in s.query(transaction=pinned)['rows']),
            'transaction snapshot lost a row valid at its start')
    require(not any(r['sample'] == 9 for r in s.query()['rows']), 'head did not expire the row')
    vacuum = s.call('POST', '/vacuum', {})
    require(vacuum['pinned'] and vacuum['horizon'] <= 2, 'reader did not pin reclamation')
    require(any(r['sample'] == 9 for r in s.query(transaction=pinned)['rows']),
            'vacuum removed a version still visible to a snapshot')
    s.call('POST', f'/transactions/{pinned}/rollback')
    s.call('POST', f'/transactions/{reader}/rollback')
    released = s.call('POST', '/vacuum', {})
    require(released['removed'] >= 1, 'released floor did not reclaim the expired version')
    return {'committed': committed, 'pinned': vacuum, 'released': released}


WORKFLOWS = {
    'expiry-visibility': expiry_visibility,
    'expiry-restart': expiry_restart,
    'expiry-vacuum-split': expiry_vacuum_split,
    'expiry-retention-pin': expiry_retention_pin,
    'expiry-index-consistency': expiry_index_consistency,
    'expiry-txn-snapshot': expiry_txn_snapshot,
    'expiry-changes': expiry_changes,
}


def run(name, binary):
    with tempfile.TemporaryDirectory(prefix='lamina-expiry-') as temp:
        temp = pathlib.Path(temp)
        s = Session(binary, temp / 'data').start()
        try:
            observations = WORKFLOWS[name](s)
            return {'workflow': name, 'status': 'observed', 'observations': observations}
        finally:
            s.stop()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--workflow', choices=['all', *WORKFLOWS], default='all')
    args = parser.parse_args()
    binary = pathlib.Path(__file__).resolve().parents[1] / 'bin/laminad'
    if not binary.exists():
        raise SystemExit('build first: go build -o bin/laminad ./cmd/laminad')
    names = list(WORKFLOWS) if args.workflow == 'all' else [args.workflow]
    for name in names:
        print(json.dumps(run(name, binary), ensure_ascii=False), flush=True)


if __name__ == '__main__':
    main()
