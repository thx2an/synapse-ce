package notification

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/KKloudTarus/synapse-ce/internal/domain/shared"
)

// SourceRecord is one row of the notification inbox (notification_source_records). A producer appends
// it through ports.NotificationOutbox inside the transaction that changes the business state (EPIC #1327
// D2); the notification source later projects it into an Event. The (TenantID, SourceKind, SourceID)
// triple is the idempotency key: appending the same source twice records it once.
type SourceRecord struct {
	TenantID      shared.ID
	SourceKind    string
	SourceID      string
	EventType     EventType
	EngagementID  shared.ID
	Severity      shared.Severity
	SchemaVersion int
	// SubjectKind and SubjectID name the entity the event is about, which can differ from its source.
	SubjectKind string
	SubjectID   string
	OccurredAt  time.Time
	Data        json.RawMessage
	// Context is the template context snapshot. Empty means the empty object.
	Context json.RawMessage
}

// Bounds shared with the notification_source_records CHECK constraints (migration 0190), so an adapter
// without a database refuses what PostgreSQL refuses. The context bound is measured on the bytes given;
// PostgreSQL measures its own jsonb rendering, so near the limit its CHECK stays the backstop.
const (
	maxSourceSubjectIDLength = 512
	maxSourceContextBytes    = 1 << 20
)

var sourceSubjectKindShape = regexp.MustCompile(`^([a-z_]+)?$`)

// Validate refuses a record the projection could never turn into an event, and a record the schema
// would reject. Data is deliberately not size-bounded here: it can carry user-sized content such as a
// finding title, and an oversized payload must not fail the producer's business write. The projection
// bounds it when it builds the event.
func (r SourceRecord) Validate() error {
	if r.TenantID.IsZero() || strings.TrimSpace(r.SourceKind) == "" || strings.TrimSpace(r.SourceID) == "" || r.OccurredAt.IsZero() {
		return fmt.Errorf("%w: invalid notification source record", shared.ErrValidation)
	}
	spec, known := catalog[r.EventType]
	if !known {
		return fmt.Errorf("%w: notification source record has an unknown event type", shared.ErrValidation)
	}
	// Operator-only events are sent on demand to one channel; no rule matches them, so a recorded one
	// would never reach anyone.
	if spec.OperatorOnly {
		return fmt.Errorf("%w: %s events are not recorded through the outbox", shared.ErrValidation, r.EventType)
	}
	if r.SchemaVersion < 1 || r.SchemaVersion > spec.SchemaVersion {
		return fmt.Errorf("%w: notification source record schema version is not known for %s", shared.ErrValidation, r.EventType)
	}
	if r.Severity != "" && shared.SeverityRank(r.Severity) == 0 {
		return fmt.Errorf("%w: invalid notification source record severity", shared.ErrValidation)
	}
	if !sourceSubjectKindShape.MatchString(r.SubjectKind) {
		return fmt.Errorf("%w: invalid notification source record subject kind", shared.ErrValidation)
	}
	// PostgreSQL's length() counts characters, not bytes.
	if utf8.RuneCountInString(r.SubjectID) > maxSourceSubjectIDLength {
		return fmt.Errorf("%w: notification source record subject id is too long", shared.ErrValidation)
	}
	if !isJSONObject(r.Data) {
		return fmt.Errorf("%w: notification source record data must be an object", shared.ErrValidation)
	}
	if len(r.Context) > 0 && (len(r.Context) > maxSourceContextBytes || !isJSONObject(r.Context)) {
		return fmt.Errorf("%w: notification source record context must be an object of at most 1 MiB", shared.ErrValidation)
	}
	return nil
}

func isJSONObject(raw json.RawMessage) bool {
	var object map[string]any
	return len(raw) > 0 && json.Unmarshal(raw, &object) == nil && object != nil
}
