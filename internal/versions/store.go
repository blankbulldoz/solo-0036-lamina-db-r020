package versions

import "lamina/internal/datum"

type Mutation struct {
	Relation  string    `json:"relation"`
	Key       string    `json:"key"`
	Row       datum.Row `json:"row,omitempty"`
	Deleted   bool      `json:"deleted,omitempty"`
	ExpiresAt uint64    `json:"expires_at,omitempty"`
}
type Version struct {
	Sequence  uint64    `json:"sequence"`
	Row       datum.Row `json:"row,omitempty"`
	Deleted   bool      `json:"deleted,omitempty"`
	ExpiresAt uint64    `json:"expires_at,omitempty"`
}
type Store struct {
	Tables map[string]map[string][]Version `json:"tables"`
}

func New() *Store { return &Store{Tables: map[string]map[string][]Version{}} }
func (s *Store) Ensure(name string) {
	if s.Tables[name] == nil {
		s.Tables[name] = map[string][]Version{}
	}
}
func (s *Store) Apply(seq uint64, writes []Mutation) {
	for _, m := range writes {
		s.Ensure(m.Relation)
		s.Tables[m.Relation][m.Key] = append(s.Tables[m.Relation][m.Key], Version{Sequence: seq, Row: datum.Clone(m.Row), Deleted: m.Deleted, ExpiresAt: m.ExpiresAt})
	}
}

// ExpiredAt reports whether a version's validity window has ended at snapshot
// "at". Validity is measured in commit sequences, never wall-clock time, so an
// answer for a given sequence is reproducible after restart and after vacuum.
func ExpiredAt(v Version, at uint64) bool { return v.ExpiresAt > 0 && at >= v.ExpiresAt }

// latestAt locates the newest version with sequence <= at without judging
// whether that version is visible.
func latestAt(chain []Version, at uint64) (Version, bool) {
	lo, hi := 0, len(chain)
	for lo < hi {
		mid := (lo + hi) / 2
		if chain[mid].Sequence <= at {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	if lo == 0 {
		return Version{}, false
	}
	return chain[lo-1], true
}

// Visible returns the row visible at a snapshot. A tombstone or an expired
// newest version makes the tuple absent; older versions never reappear,
// because expiry is a logical end of validity, not a rollback.
func Visible(chain []Version, at uint64) (Version, bool) {
	v, ok := latestAt(chain, at)
	if !ok || v.Deleted || ExpiredAt(v, at) {
		return v, false
	}
	return v, true
}
func (s *Store) Read(table, key string, at uint64) (datum.Row, bool) {
	v, ok := Visible(s.Tables[table][key], at)
	return datum.Clone(v.Row), ok
}
func (s *Store) Latest(table, key string) uint64 {
	v := s.Tables[table][key]
	if len(v) == 0 {
		return 0
	}
	return v[len(v)-1].Sequence
}
func (s *Store) Scan(table string, at uint64) map[string]datum.Row {
	out := map[string]datum.Row{}
	for k, c := range s.Tables[table] {
		if v, ok := Visible(c, at); ok {
			out[k] = datum.Clone(v.Row)
		}
	}
	return out
}
func (s *Store) Clone() *Store {
	out := New()
	for name, rows := range s.Tables {
		out.Ensure(name)
		for key, chain := range rows {
			for _, v := range chain {
				v.Row = datum.Clone(v.Row)
				out.Tables[name][key] = append(out.Tables[name][key], v)
			}
		}
	}
	return out
}
func (s *Store) Count() (keys, versions int) {
	for _, rows := range s.Tables {
		keys += len(rows)
		for _, v := range rows {
			versions += len(v)
		}
	}
	return
}
