package engine

import (
	"crypto/rand"
	"encoding/hex"
	"lamina/internal/datum"
	"lamina/internal/journal"
	"lamina/internal/versions"
	"sort"
	"time"
)

type staged struct {
	Mutation   versions.Mutation
	InsertOnly bool
	// TTL is unresolved until commit, when it becomes commit sequence + TTL.
	// Mutation.ExpiresAt carries an absolute expiry already supplied by the
	// caller. At most one of the two is set.
	TTL *uint64
}
type Transaction struct {
	ID       string `json:"id"`
	Snapshot uint64 `json:"snapshot"`
	ReadOnly bool   `json:"read_only"`
	Started  string `json:"started"`
	writes   map[string]map[string]staged
}
type Commit struct {
	Sequence uint64 `json:"sequence"`
	Writes   int    `json:"writes"`
}

// Expiry selects a row's validity window. ExpiresAt is an absolute commit
// sequence; TTL is a number of commit sequences counted from the committing
// record. Expiry is measured in commit sequences rather than wall-clock time so
// that the same historical query always yields the same answer, regardless of
// restart or when reclamation runs. Supplying neither makes the row permanent
// until deleted; supplying both is rejected.
type Expiry struct {
	ExpiresAt *uint64 `json:"expires_at,omitempty"`
	TTL       *uint64 `json:"ttl,omitempty"`
}

func (d *DB) Begin(readOnly bool) (Transaction, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.usable(); err != nil {
		return Transaction{}, err
	}
	if len(d.transactions) >= 128 {
		return Transaction{}, fault("capacity", "128 active transactions reached")
	}
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return Transaction{}, err
	}
	tx := &Transaction{ID: hex.EncodeToString(id[:]), Snapshot: d.sequence, ReadOnly: readOnly, Started: time.Now().UTC().Format(time.RFC3339Nano), writes: map[string]map[string]staged{}}
	d.transactions[tx.ID] = tx
	out := *tx
	out.writes = nil
	return out, nil
}
func (d *DB) tx(id string) (*Transaction, error) {
	tx, ok := d.transactions[id]
	if !ok {
		return nil, fault("not_found", "transaction not active")
	}
	return tx, nil
}
func (d *DB) Put(id, table string, row datum.Row, insertOnly bool, expiry Expiry) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.usable(); err != nil {
		return err
	}
	tx, err := d.tx(id)
	if err != nil {
		return err
	}
	if tx.ReadOnly {
		return fault("conflict", "read-only transaction")
	}
	r, err := d.catalog.Relation(table)
	if err != nil {
		return invalid(err)
	}
	if r.Created > tx.Snapshot {
		return fault("conflict", "relation outside transaction catalog snapshot")
	}
	normalized, err := r.Normalize(row)
	if err != nil {
		return invalid(err)
	}
	if err = expiry.validate(); err != nil {
		return err
	}
	key := r.Key(normalized)
	rows := d.store.Scan(table, tx.Snapshot)
	overlay(rows, tx.writes, table)
	if insertOnly {
		if _, ok := rows[key]; ok {
			return fault("conflict", "primary already exists")
		}
	}
	if tx.writes[table] == nil {
		tx.writes[table] = map[string]staged{}
	}
	if err = tx.reserve(table, key); err != nil {
		return err
	}
	s := staged{Mutation: versions.Mutation{Relation: table, Key: key, Row: normalized}, InsertOnly: insertOnly, TTL: expiry.TTL}
	if expiry.ExpiresAt != nil {
		s.Mutation.ExpiresAt = *expiry.ExpiresAt
	}
	tx.writes[table][key] = s
	return nil
}

// validate rejects ambiguous expiry inputs. TTL 0 would already be expired at
// its own commit sequence, so it is rejected; TTL 1 keeps the row readable for
// exactly the snapshot at its committing sequence.
func (e Expiry) validate() error {
	if e.ExpiresAt != nil && e.TTL != nil {
		return fault("invalid", "expires_at and ttl are mutually exclusive")
	}
	if e.ExpiresAt != nil && *e.ExpiresAt == 0 {
		return fault("invalid", "expires_at must be a positive commit sequence")
	}
	if e.TTL != nil && *e.TTL == 0 {
		return fault("invalid", "ttl must be at least 1 commit sequence")
	}
	return nil
}
func (tx *Transaction) reserve(table, key string) error {
	if _, exists := tx.writes[table][key]; exists {
		return nil
	}
	count := 0
	for _, m := range tx.writes {
		count += len(m)
	}
	if count >= 1000 {
		return fault("capacity", "transaction mutation limit 1000")
	}
	return nil
}
func (d *DB) Delete(id, table string, value datum.Value) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.usable(); err != nil {
		return err
	}
	tx, err := d.tx(id)
	if err != nil {
		return err
	}
	if tx.ReadOnly {
		return fault("conflict", "read-only transaction")
	}
	r, err := d.catalog.Relation(table)
	if err != nil {
		return invalid(err)
	}
	if r.Created > tx.Snapshot {
		return fault("conflict", "relation outside transaction catalog snapshot")
	}
	col, _ := r.Column(r.Primary)
	if value.Null() || value.Kind != col.Type {
		return fault("invalid", "invalid primary value")
	}
	key := value.Key()
	if tx.writes[table] == nil {
		tx.writes[table] = map[string]staged{}
	}
	if err = tx.reserve(table, key); err != nil {
		return err
	}
	tx.writes[table][key] = staged{Mutation: versions.Mutation{Relation: table, Key: key, Deleted: true}}
	return nil
}
func (d *DB) Rollback(id string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.usable(); err != nil {
		return err
	}
	if _, err := d.tx(id); err != nil {
		return err
	}
	delete(d.transactions, id)
	return nil
}
func (d *DB) Commit(id string) (Commit, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.usable(); err != nil {
		return Commit{}, err
	}
	tx, err := d.tx(id)
	if err != nil {
		return Commit{}, err
	}
	defer delete(d.transactions, id)
	// A staged write is published at the next commit sequence, so relative TTLs
	// and absolute expiries are resolved against exactly that sequence before
	// validation. A failed commit discards them together with the write set.
	sequence := d.sequence + 1
	writes := make([]versions.Mutation, 0)
	for table, mutations := range tx.writes {
		for key, m := range mutations {
			if d.store.Latest(table, key) > tx.Snapshot {
				d.conflicts++
				return Commit{}, fault("conflict", "write conflict on %s", table)
			}
			mutation := m.Mutation
			if m.TTL != nil {
				if *m.TTL > ^uint64(0)-sequence {
					return Commit{}, fault("invalid", "ttl overflows commit sequence")
				}
				mutation.ExpiresAt = sequence + *m.TTL
			}
			if mutation.ExpiresAt != 0 && mutation.ExpiresAt <= sequence {
				return Commit{}, fault("invalid", "expiry must be after the committing sequence %d", sequence)
			}
			writes = append(writes, mutation)
		}
		if err = d.unique(table, tx.writes); err != nil {
			d.conflicts++
			return Commit{}, err
		}
	}
	if len(writes) == 0 {
		return Commit{Sequence: tx.Snapshot}, nil
	}
	sort.Slice(writes, func(i, j int) bool {
		if writes[i].Relation == writes[j].Relation {
			return writes[i].Key < writes[j].Key
		}
		return writes[i].Relation < writes[j].Relation
	})
	if err = d.publish(journal.Record{Kind: "commit", Transaction: id, Writes: writes}); err != nil {
		return Commit{}, err
	}
	d.commits++
	return Commit{Sequence: d.sequence, Writes: len(writes)}, nil
}
