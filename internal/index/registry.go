package index

import (
	"lamina/internal/catalog"
	"lamina/internal/versions"
)

type Registry struct{ Trees map[string]*Tree }

// Build derives historical candidate sets from every version that is still
// physically retained. Expired versions are intentionally included while they
// exist: a snapshot before their expiry may still read them, and the access
// path rechecks the real version at the requested snapshot. Vacuum removes the
// versions first and rebuilds, so candidates and visibility agree.
func Build(c *catalog.Catalog, s *versions.Store) *Registry {
	r := &Registry{Trees: map[string]*Tree{}}
	for name, definition := range c.Indexes {
		tree := New()
		for key, chain := range s.Tables[definition.Relation] {
			for _, v := range chain {
				if !v.Deleted {
					tree.Add(v.Row[definition.Column], key)
				}
			}
		}
		r.Trees[name] = tree
	}
	return r
}
func (r *Registry) Apply(c *catalog.Catalog, writes []versions.Mutation) {
	for _, m := range writes {
		if m.Deleted {
			continue
		}
		for name, i := range c.Indexes {
			if i.Relation == m.Relation {
				r.Trees[name].Add(m.Row[i.Column], m.Key)
			}
		}
	}
}
