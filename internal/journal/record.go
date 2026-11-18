package journal

import (
	"lamina/internal/catalog"
	"lamina/internal/versions"
)

// Record is one durable commit. Format 1 is the original layout; format 2 adds
// per-version expiry. Format 2 records are self-describing: ExpiresAt travels
// with each mutation, so replay reconstructs expiry state without consulting a
// clock or process-local counters. Readers accept both formats, and format 1
// records simply carry no expiry.
type Record struct {
	Format      int                 `json:"format"`
	Sequence    uint64              `json:"sequence"`
	Kind        string              `json:"kind"`
	Transaction string              `json:"transaction,omitempty"`
	Relation    *catalog.Relation   `json:"relation,omitempty"`
	Index       *catalog.Index      `json:"index,omitempty"`
	Writes      []versions.Mutation `json:"writes,omitempty"`
	Horizon     uint64              `json:"horizon,omitempty"`
}

// SupportedFormat reports whether a recovered record may be applied.
func SupportedFormat(format int) bool { return format == 1 || format == 2 }

// CurrentFormat is the format written by this build. It adds expiry metadata
// to commit writes; format 1 remains readable for previously written journals.
const CurrentFormat = 2
