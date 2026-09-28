package memory

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/KKloudTarus/synapse-ce/internal/domain/engagement"
	"github.com/KKloudTarus/synapse-ce/internal/domain/notification"
	"github.com/KKloudTarus/synapse-ce/internal/domain/shared"
	"github.com/KKloudTarus/synapse-ce/internal/testutil/notificationconformance"
	"github.com/KKloudTarus/synapse-ce/internal/usecase/ports"
)

// notificationTenants is the tenant table NotificationSource.Poll walks.
type notificationTenants struct {
	mu  sync.Mutex
	ids []shared.ID
}

func (l *notificationTenants) add(id shared.ID) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.ids = append(l.ids, id)
}

func (l *notificationTenants) ListTenantIDs(context.Context) ([]shared.ID, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]shared.ID(nil), l.ids...), nil
}

func TestNotificationStoreConformance(t *testing.T) {
	notificationconformance.Run(t, func(t *testing.T) notificationconformance.Backend {
		engagements := NewEngagementRepository()
		tenants := &notificationTenants{}
		store := NewNotificationStore(engagements)
		return notificationconformance.Backend{
			Repository: store,
			Outbox:     store,
			Source:     NewNotificationSource(store, tenants, true),
			Runner:     NewTenantTransactionRunner(),
			AddTenant:  func(_ *testing.T, tenant shared.ID) { tenants.add(tenant) },
			AddEngagement: func(t *testing.T, tenant, id shared.ID) {
				if err := engagements.Create(context.Background(), &engagement.Engagement{ID: id, TenantID: tenant, Name: "engagement"}); err != nil {
					t.Fatal(err)
				}
			},
		}
	})
}

// The attempt loop is fenced by the durable job queue and re-checks scan, SLA and fleet state; the memory
// store refuses it outright rather than imitating it.
func TestNotificationStoreRefusesTheAttemptLoop(t *testing.T) {
	store := NewNotificationStore(nil)
	ctx := context.Background()
	_, relevant := store.DeliveryStillRelevant(ctx, ports.NotificationWork{})
	_, begin := store.BeginAttempt(ctx, "tenant", "delivery", "job", 1, "attempt", time.Now())
	finish := store.FinishAttempt(ctx, "tenant", "delivery", "job", 1, "attempt", time.Now(), "succeeded", 200, "", nil)
	cancel := store.CancelDelivery(ctx, "tenant", "delivery", "job", 1, "irrelevant")
	deadLettered, deadLetter := store.DeadLetterDelivery(ctx, "tenant", "delivery", "exhausted")
	for name, err := range map[string]error{
		"DeliveryStillRelevant": relevant, "BeginAttempt": begin, "FinishAttempt": finish,
		"CancelDelivery": cancel, "DeadLetterDelivery": deadLetter,
	} {
		if !errors.Is(err, errors.ErrUnsupported) {
			t.Errorf("%s = %v, want errors.ErrUnsupported", name, err)
		}
	}
	if deadLettered {
		t.Error("DeadLetterDelivery reported a transition it did not make")
	}
}

type notificationHarness struct {
	store   *NotificationStore
	tenants *notificationTenants
	runner  *TenantTransactionRunner
	base    time.Time
}

func newNotificationHarness(t *testing.T, tenants ...shared.ID) *notificationHarness {
	t.Helper()
	h := &notificationHarness{store: NewNotificationStore(nil), tenants: &notificationTenants{}, runner: NewTenantTransactionRunner(),
		base: time.Unix(1_700_000_000, 0).UTC()}
	for _, tenant := range tenants {
		h.tenants.add(tenant)
		c := notification.Channel{TenantID: tenant, ID: "hook", Name: "hook", Type: notification.ChannelWebhook, Enabled: true, Revision: 1, SecretVersion: 1, CreatedAt: h.base, UpdatedAt: h.base}
		if _, err := h.store.CreateChannel(context.Background(), c, "sealed"); err != nil {
			t.Fatal(err)
		}
		for _, eventType := range []notification.EventType{notification.EventScanCompleted, notification.EventIncidentCreated} {
			rule := notification.Rule{TenantID: tenant, ID: shared.ID(eventType), Name: string(eventType), Enabled: true, EventType: eventType,
				ChannelIDs: []shared.ID{"hook"}, Revision: 1, CreatedAt: h.base, UpdatedAt: h.base}
			if _, err := h.store.CreateRule(context.Background(), rule); err != nil {
				t.Fatal(err)
			}
		}
	}
	return h
}

func (h *notificationHarness) poll(t *testing.T, source *NotificationSource, offset time.Duration) (int, error) {
	t.Helper()
	return source.Poll(context.Background(), h.base.Add(offset), 100)
}

func (h *notificationHarness) append(t *testing.T, record notification.SourceRecord) {
	t.Helper()
	if err := h.runner.Run(context.Background(), record.TenantID, func(ctx context.Context) error { return h.store.Append(ctx, record) }); err != nil {
		t.Fatal(err)
	}
}

func (h *notificationHarness) deliveries(t *testing.T, tenant shared.ID) int {
	t.Helper()
	page, err := h.store.ListDeliveries(context.Background(), ports.NotificationDeliveryFilter{TenantID: tenant})
	if err != nil {
		t.Fatal(err)
	}
	return len(page.Items)
}

func harnessRecord(tenant shared.ID, kind, id string, eventType notification.EventType, at time.Time, data string) notification.SourceRecord {
	return notification.SourceRecord{TenantID: tenant, SourceKind: kind, SourceID: id, EventType: eventType, SchemaVersion: 1,
		OccurredAt: at, Data: json.RawMessage(data)}
}

// A record the projection cannot publish rolls back its whole tenant tick, as PostgreSQL's per-tenant
// transaction does, and leaves the other tenants' projections committed. #1343 replaces this with
// per-record isolation in both adapters.
func TestNotificationSourceRollsBackOnlyTheFailingTenant(t *testing.T) {
	h := newNotificationHarness(t, "tenant-a", "tenant-b")
	source := NewNotificationSource(h.store, h.tenants, true)
	if _, err := h.poll(t, source, 0); err != nil {
		t.Fatal(err)
	}
	at := h.base.Add(time.Second)
	h.append(t, harnessRecord("tenant-a", "scan_job", "a-valid", notification.EventScanCompleted, at, `{"title":"ok"}`))
	// Accepted by the outbox, which does not bound data, but over the 16 KiB an event may carry.
	h.append(t, harnessRecord("tenant-a", "scan_job", "a-oversized", notification.EventScanCompleted, at.Add(time.Second), `{"title":"`+strings.Repeat("x", 17<<10)+`"}`))
	h.append(t, harnessRecord("tenant-b", "scan_job", "b-valid", notification.EventScanCompleted, at, `{"title":"ok"}`))

	n, err := h.poll(t, source, 3*time.Second)
	if err == nil || !strings.Contains(err.Error(), "tenant-a") {
		t.Fatalf("poll error = %v, want tenant-a's failure", err)
	}
	if n != 1 || h.deliveries(t, "tenant-b") != 1 {
		t.Fatalf("projected %d records and %d tenant-b deliveries, want tenant-b's record committed", n, h.deliveries(t, "tenant-b"))
	}
	if got := h.deliveries(t, "tenant-a"); got != 0 {
		t.Fatalf("tenant-a has %d deliveries, want its whole tick rolled back", got)
	}
	// Rolled back means still pending: the next tick fails on the same record again.
	if _, err := h.poll(t, source, 4*time.Second); err == nil {
		t.Fatal("the failing record was marked processed despite the rollback")
	}
}

// The tick's checkpoint is taken before its first write, not at its first publication. A record whose event
// already exists is marked processed without publishing anything, and that mark must roll back with the
// rest of the tick, as it would in PostgreSQL's tenant transaction.
func TestNotificationSourceRollsBackRecordsMarkedBeforeTheFailure(t *testing.T) {
	h := newNotificationHarness(t, "tenant")
	source := NewNotificationSource(h.store, h.tenants, true)
	if _, err := h.poll(t, source, 0); err != nil {
		t.Fatal(err)
	}
	at := h.base.Add(time.Second)
	existing := notification.Event{TenantID: "tenant", ID: "existing", Type: notification.EventScanCompleted, SourceKind: "scan_job",
		SourceID: "already-published", SchemaVersion: 1, OccurredAt: at, Data: json.RawMessage(`{"title":"ok"}`)}
	if _, err := h.store.PublishToChannel(context.Background(), existing, "hook"); err != nil {
		t.Fatal(err)
	}
	h.append(t, harnessRecord("tenant", "scan_job", "already-published", notification.EventScanCompleted, at, `{"title":"ok"}`))
	h.append(t, harnessRecord("tenant", "scan_job", "oversized", notification.EventScanCompleted, at.Add(time.Second), `{"title":"`+strings.Repeat("x", 17<<10)+`"}`))

	if _, err := h.poll(t, source, 3*time.Second); err == nil {
		t.Fatal("poll succeeded despite an unpublishable record")
	}
	h.store.mu.Lock()
	defer h.store.mu.Unlock()
	if h.store.tenants["tenant"].records[notificationSourceKey{"scan_job", "already-published"}].processed {
		t.Fatal("a record marked processed in the failed tick survived its rollback")
	}
}

// With incident routing disabled the source consumes incident records without publishing them, because
// legacy routing delivers incidents; the PostgreSQL source does the same.
func TestNotificationSourceConsumesIncidentsWithoutPublishingWhenDisabled(t *testing.T) {
	for _, enabled := range []bool{true, false} {
		h := newNotificationHarness(t, "tenant")
		source := NewNotificationSource(h.store, h.tenants, enabled)
		if _, err := h.poll(t, source, 0); err != nil {
			t.Fatal(err)
		}
		h.append(t, harnessRecord("tenant", "incident", "inc-1", notification.EventIncidentCreated, h.base.Add(time.Second), `{"title":"incident"}`))
		n, err := h.poll(t, source, 2*time.Second)
		if err != nil || n != 1 {
			t.Fatalf("enabled=%v: poll = %d, %v, want the record consumed", enabled, n, err)
		}
		want := 0
		if enabled {
			want = 1
		}
		if got := h.deliveries(t, "tenant"); got != want {
			t.Fatalf("enabled=%v: deliveries = %d, want %d", enabled, got, want)
		}
		if n, err := h.poll(t, source, 3*time.Second); err != nil || n != 0 {
			t.Fatalf("enabled=%v: second poll = %d, %v, want nothing left", enabled, n, err)
		}
	}
}

// Only the kinds the PostgreSQL source projects are projected; a new kind waits for its projection.
func TestNotificationSourceProjectsOnlyTheCapturedKinds(t *testing.T) {
	h := newNotificationHarness(t, "tenant")
	source := NewNotificationSource(h.store, h.tenants, true)
	if _, err := h.poll(t, source, 0); err != nil {
		t.Fatal(err)
	}
	h.append(t, harnessRecord("tenant", "finding", "finding-1", notification.EventScanCompleted, h.base.Add(time.Second), `{"title":"x"}`))
	if n, err := h.poll(t, source, 2*time.Second); err != nil || n != 0 || h.deliveries(t, "tenant") != 0 {
		t.Fatalf("poll = %d, %v, want an unprojected kind left pending", n, err)
	}
}
