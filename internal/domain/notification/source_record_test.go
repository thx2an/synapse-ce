package notification

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/KKloudTarus/synapse-ce/internal/domain/shared"
)

func validSourceRecord() SourceRecord {
	return SourceRecord{
		TenantID: "tenant", SourceKind: "scan_job", SourceID: "scan-1", EventType: EventScanCompleted,
		EngagementID: "eng", SchemaVersion: 1, SubjectKind: "scan_job", SubjectID: "scan-1",
		OccurredAt: time.Unix(1_700_000_000, 0).UTC(), Data: json.RawMessage(`{"title":"Scan completed"}`),
	}
}

func TestSourceRecordValidateAcceptsAWellFormedRecord(t *testing.T) {
	record := validSourceRecord()
	if err := record.Validate(); err != nil {
		t.Fatalf("valid record refused: %v", err)
	}
	// The optional columns default in the schema, so leaving them out is well formed.
	record.EngagementID, record.SubjectKind, record.SubjectID, record.Context = "", "", "", nil
	if err := record.Validate(); err != nil {
		t.Fatalf("record without optional fields refused: %v", err)
	}
	record.Severity, record.Context = shared.SeverityHigh, json.RawMessage(`{"finding":{"title":"x"}}`)
	if err := record.Validate(); err != nil {
		t.Fatalf("record with severity and context refused: %v", err)
	}
}

// Each case breaks exactly one field of an otherwise valid record, so a guard that stopped firing
// would let its case through.
func TestSourceRecordValidateRefusesMalformedRecords(t *testing.T) {
	spec, _ := LookupEvent(EventScanCompleted)
	for name, mutate := range map[string]func(*SourceRecord){
		"no tenant":                    func(r *SourceRecord) { r.TenantID = "" },
		"blank source kind":            func(r *SourceRecord) { r.SourceKind = "  " },
		"blank source id":              func(r *SourceRecord) { r.SourceID = "" },
		"no occurrence time":           func(r *SourceRecord) { r.OccurredAt = time.Time{} },
		"unknown event type":           func(r *SourceRecord) { r.EventType = "finding.imagined" },
		"operator-only event type":     func(r *SourceRecord) { r.EventType = EventTest },
		"schema version zero":          func(r *SourceRecord) { r.SchemaVersion = 0 },
		"schema version above catalog": func(r *SourceRecord) { r.SchemaVersion = spec.SchemaVersion + 1 },
		"unknown severity":             func(r *SourceRecord) { r.Severity = "apocalyptic" },
		"subject kind with uppercase":  func(r *SourceRecord) { r.SubjectKind = "Scan_job" },
		"subject kind with a dot":      func(r *SourceRecord) { r.SubjectKind = "scan.job" },
		"subject id over 512 characters": func(r *SourceRecord) {
			r.SubjectID = strings.Repeat("é", maxSourceSubjectIDLength+1)
		},
		"no data":        func(r *SourceRecord) { r.Data = nil },
		"null data":      func(r *SourceRecord) { r.Data = json.RawMessage(`null`) },
		"array data":     func(r *SourceRecord) { r.Data = json.RawMessage(`[1]`) },
		"malformed data": func(r *SourceRecord) { r.Data = json.RawMessage(`{"title":`) },
		"array context":  func(r *SourceRecord) { r.Context = json.RawMessage(`[]`) },
		"null context":   func(r *SourceRecord) { r.Context = json.RawMessage(`null`) },
		"context over 1 MiB": func(r *SourceRecord) {
			r.Context = json.RawMessage(`{"blob":"` + strings.Repeat("a", maxSourceContextBytes) + `"}`)
		},
	} {
		t.Run(name, func(t *testing.T) {
			record := validSourceRecord()
			mutate(&record)
			if err := record.Validate(); !errors.Is(err, shared.ErrValidation) {
				t.Fatalf("want ErrValidation, got %v", err)
			}
		})
	}
}

// The subject id bound counts characters the way PostgreSQL's length() does, so a multi-byte id at the
// limit is accepted rather than refused on its byte length.
func TestSourceRecordSubjectIDBoundCountsCharacters(t *testing.T) {
	record := validSourceRecord()
	record.SubjectID = strings.Repeat("é", maxSourceSubjectIDLength)
	if err := record.Validate(); err != nil {
		t.Fatalf("512-character subject id refused: %v", err)
	}
}

// Data carries user-sized content, so it has no size bound here: an oversized payload must not fail the
// producer's business write.
func TestSourceRecordValidateDoesNotBoundData(t *testing.T) {
	record := validSourceRecord()
	record.Data = json.RawMessage(`{"title":"` + strings.Repeat("a", 64<<10) + `"}`)
	if err := record.Validate(); err != nil {
		t.Fatalf("large data refused: %v", err)
	}
}
