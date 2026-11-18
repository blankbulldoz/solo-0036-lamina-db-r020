package engine

import "sort"

type Stats struct {
	Sequence       uint64         `json:"sequence"`
	Horizon        uint64         `json:"horizon"`
	Active         []Transaction  `json:"active"`
	Keys           int            `json:"keys"`
	Versions       int            `json:"versions"`
	IndexEntries   map[string]int `json:"index_entries"`
	JournalRecords int            `json:"journal_records"`
	HistoryBase    uint64         `json:"history_base"`
	RecoveredTail  int64          `json:"recovered_tail_bytes"`
	Commits        uint64         `json:"commits_since_open"`
	Conflicts      uint64         `json:"conflicts_since_open"`
	Queries        uint64         `json:"queries_since_open"`
	// Expiry and physical reclamation are separate observables. ExpiredVersions
	// counts versions whose validity ended at the head; ExpiredKeys counts
	// tuples currently hidden only by expiry. ReclaimableVersions counts what a
	// vacuum to the current retention floor would physically remove.
	ExpiredVersions      int    `json:"expired_versions"`
	ExpiredKeys          int    `json:"expired_keys"`
	ReclaimableVersions  int    `json:"reclaimable_versions"`
	ReclaimableExpired   int    `json:"reclaimable_expired"`
	RetentionFloor       uint64 `json:"retention_floor"`
	OldestSnapshot       uint64 `json:"oldest_snapshot,omitempty"`
	VersionChainMaxDepth int    `json:"version_chain_max_depth"`
}

func (d *DB) Stats() (Stats, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if err := d.usable(); err != nil {
		return Stats{}, err
	}
	keys, versions := d.store.Count()
	s := Stats{Sequence: d.sequence, Horizon: d.horizon, Keys: keys, Versions: versions, Active: []Transaction{}, IndexEntries: map[string]int{}, JournalRecords: len(d.wal.Records), HistoryBase: d.wal.Base, RecoveredTail: d.wal.RecoveredTail, Commits: d.commits, Conflicts: d.conflicts, Queries: d.queries}
	for _, tx := range d.transactions {
		copy := *tx
		copy.writes = nil
		s.Active = append(s.Active, copy)
	}
	sort.Slice(s.Active, func(i, j int) bool { return s.Active[i].ID < s.Active[j].ID })
	for name, t := range d.indexes.Trees {
		s.IndexEntries[name] = t.Size()
	}
	head := d.sequence
	s.ExpiredVersions = d.store.ExpiredVersions(head)
	s.ExpiredKeys = d.store.ExpiredKeyCount(head)
	s.RetentionFloor = d.retentionFloorLocked()
	plan := d.store.Plan(s.RetentionFloor, head)
	s.ReclaimableVersions = plan.Removed
	s.ReclaimableExpired = plan.RemovedExpired
	if oldest, ok := d.oldestSnapshot(); ok {
		s.OldestSnapshot = oldest
	}
	for _, rows := range d.store.Tables {
		for _, chain := range rows {
			if len(chain) > s.VersionChainMaxDepth {
				s.VersionChainMaxDepth = len(chain)
			}
		}
	}
	return s, nil
}

// retentionFloorLocked mirrors Vacuum's effective floor for observation only;
// the caller already holds the database lock.
func (d *DB) retentionFloorLocked() uint64 {
	horizon := d.sequence
	if oldest, ok := d.oldestSnapshot(); ok && oldest < horizon {
		horizon = oldest
	}
	if horizon < d.horizon {
		horizon = d.horizon
	}
	return horizon
}
