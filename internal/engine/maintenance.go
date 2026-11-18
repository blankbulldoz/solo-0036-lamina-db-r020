package engine

import (
	"fmt"
	"lamina/internal/index"
	"lamina/internal/journal"
	"lamina/internal/snapshot"
	"lamina/internal/versions"
	"os"
	"path/filepath"
)

func (d *DB) image() snapshot.Image {
	return snapshot.Image{Format: snapshot.FormatCurrent, Sequence: d.sequence, Horizon: d.horizon, Catalog: d.catalog.Clone(), Store: d.store.Clone()}
}
func (d *DB) Checkpoint() (uint64, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.usable(); err != nil {
		return 0, err
	}
	if err := snapshot.Write(filepath.Join(d.dir, "checkpoint.lmi"), d.image()); err != nil {
		return 0, fault("unavailable", "checkpoint: %s", err)
	}
	return d.sequence, nil
}
func (d *DB) Backup() ([]byte, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if err := d.usable(); err != nil {
		return nil, err
	}
	b, err := snapshot.Encode(d.image())
	if err != nil {
		return nil, err
	}
	if len(b) > snapshot.MaxImage {
		return nil, fault("capacity", "backup exceeds 128MiB")
	}
	return b, nil
}
func Restore(dir string, data []byte) error {
	image, err := snapshot.Decode(data)
	if err != nil {
		return err
	}
	if err = os.Mkdir(dir, 0700); err != nil {
		return fmt.Errorf("restore requires a new directory: %w", err)
	}
	installed := false
	defer func() {
		if !installed {
			os.Remove(filepath.Join(dir, "checkpoint.lmi"))
			os.Remove(filepath.Join(dir, "journal.lmw"))
			os.Remove(filepath.Join(dir, "lamina.lock"))
			os.Remove(dir)
		}
	}()
	w, err := journal.Open(dir, image.Sequence)
	if err != nil {
		return err
	}
	if err = snapshot.Write(filepath.Join(dir, "checkpoint.lmi"), image); err != nil {
		w.Close()
		return err
	}
	if err = w.Close(); err != nil {
		return err
	}
	installed = true
	return nil
}

// VacuumRequest describes one reclamation decision. Horizon is optional and
// defaults to the current head. Retain optionally keeps at least that many
// recent commit sequences readable, choosing reproducible history over prompt
// space release. DryRun reports the same decision without publishing it.
type VacuumRequest struct {
	Horizon *uint64 `json:"horizon,omitempty"`
	Retain  *uint64 `json:"retain,omitempty"`
	DryRun  bool    `json:"dry_run,omitempty"`
}

// Vacuum advances the physical retention floor. The effective floor is the
// smallest of the requested horizon, the oldest active transaction snapshot,
// and head-retain; it never moves backwards. Expiry itself is unaffected: it is
// decided per snapshot from version expiry metadata, so the answers for every
// snapshot at or above the new floor stay identical before and after restart.
func (d *DB) Vacuum(request VacuumRequest) (versions.VacuumResult, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.usable(); err != nil {
		return versions.VacuumResult{}, err
	}
	requested := d.sequence
	if request.Horizon != nil {
		requested = *request.Horizon
	}
	if requested > d.sequence || requested < d.horizon {
		return versions.VacuumResult{}, fault("invalid", "vacuum horizon outside retained interval %d..%d", d.horizon, d.sequence)
	}
	horizon := requested
	result := versions.VacuumResult{Requested: requested}
	if request.Retain != nil {
		// Keep at least this many recent commit sequences readable. The floor may
		// never advance past head-retain, so history stays reproducible at the
		// price of holding more versions in memory.
		limit := uint64(0)
		if *request.Retain < d.sequence {
			limit = d.sequence - *request.Retain
		}
		if limit < horizon {
			horizon = limit
			result.LimitedByPolicy = true
		}
	}
	if oldest, ok := d.oldestSnapshot(); ok && oldest < horizon {
		horizon = oldest
		result.PinnedByReader = true
		result.PinnedBy = oldest
	}
	if horizon < d.horizon {
		horizon = d.horizon
	}
	result.Pinned = horizon < requested
	result.Horizon = horizon
	result.DryRun = request.DryRun

	head := d.sequence
	plan := d.store.Plan(horizon, head)
	plan.Requested = result.Requested
	plan.Horizon = horizon
	plan.Pinned = result.Pinned
	plan.PinnedByReader = result.PinnedByReader
	plan.PinnedBy = result.PinnedBy
	plan.LimitedByPolicy = result.LimitedByPolicy
	plan.DryRun = request.DryRun
	if request.DryRun || horizon == d.horizon {
		return plan, nil
	}
	if err := d.publish(journal.Record{Kind: "vacuum", Horizon: horizon}); err != nil {
		return versions.VacuumResult{}, err
	}
	d.indexes = index.Build(d.catalog, d.store)
	return plan, nil
}

func (d *DB) oldestSnapshot() (uint64, bool) {
	var oldest uint64
	found := false
	for _, tx := range d.transactions {
		if !found || tx.Snapshot < oldest {
			oldest = tx.Snapshot
			found = true
		}
	}
	return oldest, found
}
