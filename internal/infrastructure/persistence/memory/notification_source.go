package memory

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/KKloudTarus/synapse-ce/internal/domain/notification"
	"github.com/KKloudTarus/synapse-ce/internal/domain/shared"
	"github.com/KKloudTarus/synapse-ce/internal/usecase/ports"
)

// NotificationSource is the in-memory conformance adapter for ports.NotificationSource. It projects the
// source records NotificationStore.Append captured, the same way the PostgreSQL source projects the
// records its capture trigger writes. The PostgreSQL source also polls tables that have no memory
// counterpart (vulnerability actions, SLA deadlines, fleet heartbeats, ownership intents and the
// personal inbox); this one does not.
type NotificationSource struct {
	store           *NotificationStore
	tenants         ports.TenantLister
	incidentEnabled bool
}

var _ ports.NotificationSource = (*NotificationSource)(nil)

// NewNotificationSource returns a source over store. incidentEnabled mirrors the PostgreSQL source: when
// false, incident records are consumed without publishing, because legacy routing delivers them.
func NewNotificationSource(store *NotificationStore, tenants ports.TenantLister, incidentEnabled bool) *NotificationSource {
	return &NotificationSource{store: store, tenants: tenants, incidentEnabled: incidentEnabled}
}

// capturedSourceKinds are the record kinds the PostgreSQL source projects (notification_source.go), in
// its order.
var capturedSourceKinds = []string{"scan_job", "project_analysis_gate", "incident"}

// Poll projects each tenant in its own transaction, so a failure rolls back that tenant's whole tick and
// leaves the other tenants' projections committed.
func (s *NotificationSource) Poll(ctx context.Context, now time.Time, limit int) (int, error) {
	if limit <= 0 {
		limit = 100
	}
	if limit > 200 {
		limit = 200
	}
	tenants, err := s.tenants.ListTenantIDs(ctx)
	if err != nil {
		return 0, fmt.Errorf("list notification tenants: %w", err)
	}
	sort.Slice(tenants, func(i, j int) bool { return tenants[i] < tenants[j] })
	runner := NewTenantTransactionRunner()
	total := 0
	var failures []error
	for _, tenant := range tenants {
		if tenant.IsZero() {
			continue
		}
		if ctx.Err() != nil {
			return total, ctx.Err()
		}
		n := 0
		err := runner.Run(ctx, tenant, func(txCtx context.Context) error {
			var err error
			n, err = s.store.project(txCtx, tenant, now.UTC(), limit, s.incidentEnabled)
			return err
		})
		if err != nil {
			failures = append(failures, fmt.Errorf("poll notification sources for tenant %s: %w", tenant, err))
			continue
		}
		total += n
	}
	return total, errors.Join(failures...)
}

// project activates the tenant on its first poll, capturing nothing that predates it, and afterwards
// publishes up to limit pending records of each captured kind.
func (s *NotificationStore) project(ctx context.Context, tenant shared.ID, now time.Time, limit int, incidentEnabled bool) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.checkpoint(ctx)
	t := s.tenant(tenant)
	if !t.activated {
		t.activated, t.activation = true, now.Truncate(time.Microsecond)
		return 0, nil
	}
	count := 0
	for _, kind := range capturedSourceKinds {
		var pending []notificationRecordRow
		for key, row := range t.records {
			if key.kind == kind && !row.processed {
				pending = append(pending, row)
			}
		}
		sort.Slice(pending, func(i, j int) bool {
			a, b := pending[i].record, pending[j].record
			if !a.OccurredAt.Equal(b.OccurredAt) {
				return a.OccurredAt.Before(b.OccurredAt)
			}
			return a.SourceID < b.SourceID
		})
		if len(pending) > limit {
			pending = pending[:limit]
		}
		for _, row := range pending {
			r := row.record
			// The PostgreSQL projection reads only these columns and stamps schema version 1.
			e := notification.Event{
				TenantID: tenant, ID: notificationStableID(tenant.String(), kind, r.SourceID), Type: r.EventType,
				SourceKind: kind, SourceID: r.SourceID, EngagementID: r.EngagementID, Severity: r.Severity,
				SchemaVersion: 1, OccurredAt: r.OccurredAt, Data: r.Data,
			}
			if kind != "incident" || incidentEnabled {
				if _, err := s.publish(ctx, t, e, ""); err != nil {
					return 0, err
				}
			}
			row.processed = true
			t.records[notificationSourceKey{kind, r.SourceID}] = row
		}
		count += len(pending)
	}
	return count, nil
}
