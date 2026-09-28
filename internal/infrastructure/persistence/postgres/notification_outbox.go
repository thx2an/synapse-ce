package postgres

import (
	"context"
	"fmt"

	"github.com/KKloudTarus/synapse-ce/internal/domain/notification"
	"github.com/KKloudTarus/synapse-ce/internal/domain/shared"
	"github.com/KKloudTarus/synapse-ce/internal/usecase/ports"
)

// NotificationOutbox appends source records inside the producer's own tenant transaction. It holds no
// pool on purpose: a record written in a transaction of its own could commit while the business write
// it describes rolls back, or be lost when the business write commits and the process dies before it.
type NotificationOutbox struct{}

func NewNotificationOutbox() *NotificationOutbox { return &NotificationOutbox{} }

var _ ports.NotificationOutbox = (*NotificationOutbox)(nil)

// Append joins the transaction TenantTransactionRunner.Run bound to ctx and refuses to run without one.
// A record that predates the tenant's notification activation is dropped, and a repeated source is a
// no-op; both follow notification_capture_source() (migration 0163), which writes this table for the
// trigger-captured producers.
func (o *NotificationOutbox) Append(ctx context.Context, record notification.SourceRecord) error {
	if err := record.Validate(); err != nil {
		return err
	}
	tx, bound, err := contextTenantTx(ctx, record.TenantID)
	if err != nil {
		return err
	}
	if !bound {
		return fmt.Errorf("%w: a notification source record must be appended inside the producer's tenant transaction", shared.ErrValidation)
	}
	snapshot := []byte(record.Context)
	if len(snapshot) == 0 {
		snapshot = []byte(`{}`)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO notification_source_records(tenant_id,source_kind,source_id,event_type,engagement_id,severity,occurred_at,data,schema_version,subject_kind,subject_id,context)
		SELECT $1,$2,$3,$4,$5,$6,$7::timestamptz,$8::jsonb,$9::int,$10,$11,$12::jsonb
		WHERE EXISTS(SELECT 1 FROM notification_source_state s WHERE s.tenant_id=$1 AND s.source_kind='framework' AND s.source_id='activation' AND s.observed_at<=$7::timestamptz)
		ON CONFLICT DO NOTHING`,
		record.TenantID, record.SourceKind, record.SourceID, record.EventType, record.EngagementID, record.Severity,
		record.OccurredAt, []byte(record.Data), record.SchemaVersion, record.SubjectKind, record.SubjectID, snapshot); err != nil {
		return fmt.Errorf("append notification source record: %w", err)
	}
	return nil
}
