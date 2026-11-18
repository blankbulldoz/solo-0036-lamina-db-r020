package versions

// VacuumResult reports one reclamation decision. "Removed" and "Retained" are
// physical version counts; their "Expired" companions count those versions
// whose logical validity had already ended. Expiry and physical reclamation are
// therefore observable separately: a version can be expired long before it is
// removable, and a removable version need not have expired.
//
// All fields describe the state after the decision would be applied, so a dry
// run is an exact preview of applying the same vacuum.
type VacuumResult struct {
	Requested       uint64 `json:"requested"`
	Horizon         uint64 `json:"horizon"`
	Pinned          bool   `json:"pinned"`
	PinnedByReader  bool   `json:"pinned_by_reader"`
	PinnedBy        uint64 `json:"pinned_by_snapshot,omitempty"`
	LimitedByPolicy bool   `json:"limited_by_retention"`
	DryRun          bool   `json:"dry_run"`
	Removed         int    `json:"removed"`
	RemovedExpired  int    `json:"removed_expired"`
	Retained        int    `json:"retained"`
	RetainedExpired int    `json:"retained_expired"`
	ExpiredVersions int    `json:"expired_versions"`
	ExpiredKeys     int    `json:"expired_keys"`
}

func expired(v Version, at uint64) bool { return ExpiredAt(v, at) }

// removablePrefix returns how many leading versions of a chain may disappear
// without changing any answer at a snapshot >= horizon.
//
// Only the newest version at or below the horizon can still be read from that
// prefix; every older prefix version is masked by it. If that newest version
// is a tombstone or already expired at the horizon it stays invisible for all
// later snapshots too, so the whole prefix, including that version, may go.
// Versions after the horizon are always kept because snapshots between their
// sequence and their expiry still observe them.
func removablePrefix(chain []Version, horizon uint64) int {
	keep := -1
	for i, v := range chain {
		if v.Sequence > horizon {
			break
		}
		keep = i
	}
	if keep < 0 {
		return 0
	}
	if chain[keep].Deleted || expired(chain[keep], horizon) {
		return keep + 1
	}
	return keep
}

// expiredAtHead reports whether the tuple is currently absent specifically
// because its newest published version expired, rather than because it never
// existed or was explicitly deleted.
func expiredAtHead(chain []Version, head uint64) bool {
	v, ok := latestAt(chain, head)
	return ok && !v.Deleted && expired(v, head)
}

// Plan computes the reclamation decision at horizon against head without
// changing the store, so callers can preview space release. Every field
// describes the post-decision state, matching what Vacuum would leave behind.
func (s *Store) Plan(horizon, head uint64) VacuumResult {
	result := VacuumResult{Horizon: horizon, Requested: horizon}
	for _, rows := range s.Tables {
		for _, chain := range rows {
			remove := removablePrefix(chain, horizon)
			for _, v := range chain[:remove] {
				result.Removed++
				if expired(v, head) {
					result.RemovedExpired++
				}
			}
			// Report expiry over the versions that would remain, not the store
			// as it is now, so dry_run and applied vacuum agree exactly.
			for _, v := range chain[remove:] {
				result.Retained++
				if expired(v, head) {
					result.RetainedExpired++
					result.ExpiredVersions++
				}
			}
			remaining := chain[remove:]
			if latest, ok := latestAt(remaining, head); ok && !latest.Deleted && expired(latest, head) {
				result.ExpiredKeys++
			}
		}
	}
	return result
}

// Vacuum physically drops the removable prefix decided by Plan.
func (s *Store) Vacuum(horizon, head uint64) VacuumResult {
	result := s.Plan(horizon, head)
	for _, rows := range s.Tables {
		for key, chain := range rows {
			remove := removablePrefix(chain, horizon)
			if remove == 0 {
				continue
			}
			if remove == len(chain) {
				delete(rows, key)
				continue
			}
			rows[key] = append([]Version(nil), chain[remove:]...)
		}
	}
	return result
}

// ExpiredVersions counts versions whose validity window has ended by head.
func (s *Store) ExpiredVersions(head uint64) int {
	count := 0
	for _, rows := range s.Tables {
		for _, chain := range rows {
			for _, v := range chain {
				if expired(v, head) {
					count++
				}
			}
		}
	}
	return count
}

// ExpiredKeyCount counts tuples currently hidden only by expiry.
func (s *Store) ExpiredKeyCount(head uint64) int {
	count := 0
	for _, rows := range s.Tables {
		for _, chain := range rows {
			if expiredAtHead(chain, head) {
				count++
			}
		}
	}
	return count
}
