package memory

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/KKloudTarus/synapse-ce/internal/domain/engagement"
	"github.com/KKloudTarus/synapse-ce/internal/domain/notification"
	"github.com/KKloudTarus/synapse-ce/internal/domain/shared"
	"github.com/KKloudTarus/synapse-ce/internal/usecase/ports"
)

// NotificationStore is the in-memory conformance adapter for notification administration, delivery
// history and the source-record outbox. It is intentionally not wired into production: notifications
// require PostgreSQL, and the worker's delivery-attempt loop runs only there.
//
// It follows the PostgreSQL repository for channels, rules, publication, delivery history and the
// outbox, with these differences:
//   - The attempt loop (DeliveryStillRelevant, BeginAttempt, FinishAttempt, CancelDelivery,
//     DeadLetterDelivery) returns errors.ErrUnsupported. It is fenced by the durable job queue and
//     re-checks scan, SLA and fleet state, and a copy that only looked faithful would let tests pass
//     against behavior production does not have.
//   - Publication creates pending deliveries but enqueues no notification.deliver job, since no
//     worker can attempt them here.
//   - There is no personal inbox and no destination-change notice.
//   - There are no ownership teams, so a rule scoped to a team id reads that team as not found.
type NotificationStore struct {
	mu          sync.Mutex
	engagements notificationEngagements
	tenants     map[shared.ID]*notificationTenant
	now         func() time.Time
}

// notificationEngagements resolves the engagements a rule may be scoped to.
type notificationEngagements interface {
	GetByIDInTenant(ctx context.Context, tenantID, id shared.ID) (*engagement.Engagement, error)
}

type notificationTenant struct {
	activated  bool
	activation time.Time
	channels   map[shared.ID]notificationChannelRow
	rules      map[shared.ID]notification.Rule
	events     map[notificationSourceKey]notification.Event
	deliveries map[shared.ID]notificationDeliveryRow
	records    map[notificationSourceKey]notificationRecordRow
}

type notificationChannelRow struct {
	channel notification.Channel
	sealed  map[int]string // sealed configuration by secret version
}

type notificationDeliveryRow struct {
	delivery       notification.Delivery
	channelVersion int
}

type notificationSourceKey struct{ kind, id string }

type notificationRecordRow struct {
	record    notification.SourceRecord
	processed bool
}

var (
	_ ports.NotificationRepository = (*NotificationStore)(nil)
	_ ports.NotificationOutbox     = (*NotificationStore)(nil)
)

// errNotificationAttemptLoop is returned by the delivery-attempt methods, which run only against PostgreSQL.
var errNotificationAttemptLoop = fmt.Errorf("%w: notification delivery attempts run only against PostgreSQL", errors.ErrUnsupported)

// NewNotificationStore returns an empty store. engagements may be nil, in which case every engagement a
// rule names reads as not found.
func NewNotificationStore(engagements notificationEngagements) *NotificationStore {
	return &NotificationStore{engagements: engagements, tenants: map[shared.ID]*notificationTenant{}, now: time.Now}
}

func (s *NotificationStore) tenant(id shared.ID) *notificationTenant {
	t := s.tenants[id]
	if t == nil {
		t = &notificationTenant{
			channels:   map[shared.ID]notificationChannelRow{},
			rules:      map[shared.ID]notification.Rule{},
			events:     map[notificationSourceKey]notification.Event{},
			deliveries: map[shared.ID]notificationDeliveryRow{},
			records:    map[notificationSourceKey]notificationRecordRow{},
		}
		s.tenants[id] = t
	}
	return t
}

// checkpoint makes a mutation roll back with the enclosing TenantTransactionRunner.Run. Callers hold s.mu.
func (s *NotificationStore) checkpoint(ctx context.Context) {
	registerTenantCheckpoint(ctx, s, func(tenantID shared.ID) func() {
		previous := s.tenants[tenantID].clone()
		return func() {
			s.mu.Lock()
			defer s.mu.Unlock()
			if previous == nil {
				delete(s.tenants, tenantID)
				return
			}
			s.tenants[tenantID] = previous
		}
	})
}

// clone copies every map. Stored values are never mutated in place, so their slices can be shared.
func (t *notificationTenant) clone() *notificationTenant {
	if t == nil {
		return nil
	}
	out := *t
	out.channels = cloneMapValues(t.channels, func(row notificationChannelRow) notificationChannelRow {
		row.sealed = cloneMapValues(row.sealed, func(v string) string { return v })
		return row
	})
	out.rules = cloneMapValues(t.rules, func(r notification.Rule) notification.Rule { return r })
	out.events = cloneMapValues(t.events, func(e notification.Event) notification.Event { return e })
	out.deliveries = cloneMapValues(t.deliveries, func(d notificationDeliveryRow) notificationDeliveryRow { return d })
	out.records = cloneMapValues(t.records, func(r notificationRecordRow) notificationRecordRow { return r })
	return &out
}

// timestamp matches the microsecond precision PostgreSQL stores.
func (s *NotificationStore) timestamp() time.Time {
	return s.now().UTC().Truncate(time.Microsecond)
}

// Append records a source inside the producer's tenant transaction; see ports.NotificationOutbox.
func (s *NotificationStore) Append(ctx context.Context, record notification.SourceRecord) error {
	if err := record.Validate(); err != nil {
		return err
	}
	transaction, bound := ctx.Value(tenantTransactionKey{}).(*tenantTransaction)
	if !bound {
		return fmt.Errorf("%w: a notification source record must be appended inside the producer's tenant transaction", shared.ErrValidation)
	}
	if transaction.tenantID != record.TenantID {
		return fmt.Errorf("%w: nested tenant transaction mismatch", shared.ErrValidation)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	t := s.tenants[record.TenantID]
	// The capture trigger's rule: nothing is captured before the tenant's notification activation.
	if t == nil || !t.activated || t.activation.After(record.OccurredAt.Truncate(time.Microsecond)) {
		return nil
	}
	key := notificationSourceKey{record.SourceKind, record.SourceID}
	if _, exists := t.records[key]; exists {
		return nil
	}
	s.checkpoint(ctx)
	record.Data = append(json.RawMessage(nil), record.Data...)
	if len(record.Context) == 0 {
		record.Context = json.RawMessage(`{}`)
	} else {
		record.Context = append(json.RawMessage(nil), record.Context...)
	}
	t.records[key] = notificationRecordRow{record: record}
	return nil
}

func (s *NotificationStore) CreateChannel(ctx context.Context, c notification.Channel, sealed string) (notification.Channel, error) {
	if err := c.Validate(); err != nil {
		return notification.Channel{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	t := s.tenant(c.TenantID)
	if err := t.admit("channel"); err != nil {
		return notification.Channel{}, err
	}
	if _, exists := t.channels[c.ID]; exists {
		return notification.Channel{}, fmt.Errorf("insert notification channel %s: %w", c.ID, shared.ErrConflict)
	}
	s.checkpoint(ctx)
	c.Recipients = cloneRecipients(c.Recipients)
	c.DeletedAt = nil
	t.channels[c.ID] = notificationChannelRow{channel: c, sealed: map[int]string{c.SecretVersion: sealed}}
	return c, nil
}

func (s *NotificationStore) UpdateChannel(ctx context.Context, c notification.Channel, sealed string, replace bool) (notification.Channel, error) {
	if err := c.Validate(); err != nil {
		return notification.Channel{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	t := s.tenant(c.TenantID)
	row, exists := t.channels[c.ID]
	if !exists || row.channel.DeletedAt != nil || row.channel.Revision != c.Revision-1 {
		return notification.Channel{}, fmt.Errorf("notification channel revision is stale: %w", shared.ErrConflict)
	}
	s.checkpoint(ctx)
	row.sealed = cloneMapValues(row.sealed, func(v string) string { return v })
	if replace {
		c.SecretVersion = row.channel.SecretVersion + 1
		row.sealed[c.SecretVersion] = sealed
	} else {
		c.SecretVersion = row.channel.SecretVersion
	}
	c.Recipients = cloneRecipients(c.Recipients)
	stored := c
	stored.CreatedAt, stored.DeletedAt = row.channel.CreatedAt, nil
	row.channel = stored
	t.channels[c.ID] = row
	if !c.Enabled {
		t.cancelPending(c.ID, "channel_disabled", c.UpdatedAt)
	}
	return c, nil
}

func (s *NotificationStore) DeleteChannel(ctx context.Context, tenant, id shared.ID, revision int, at time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	t := s.tenant(tenant)
	row, exists := t.channels[id]
	if !exists || row.channel.DeletedAt != nil || row.channel.Revision != revision {
		return fmt.Errorf("notification channel revision is stale: %w", shared.ErrConflict)
	}
	s.checkpoint(ctx)
	deleted := at
	row.channel.Enabled, row.channel.DeletedAt, row.channel.UpdatedAt = false, &deleted, at
	row.channel.Revision++
	t.channels[id] = row
	t.cancelPending(id, "channel_deleted", at)
	return nil
}

// cancelPending cancels the channel's undelivered deliveries. No attempt ever starts here, so none is
// protected the way PostgreSQL protects a delivery whose attempt is in flight.
func (t *notificationTenant) cancelPending(channelID shared.ID, reason string, at time.Time) {
	for id, row := range t.deliveries {
		d := row.delivery
		if d.ChannelID != channelID || (d.State != notification.DeliveryPending && d.State != notification.DeliveryRetrying) {
			continue
		}
		d.State, d.LastError, d.NextAttemptAt, d.UpdatedAt = notification.DeliveryCancelled, reason, nil, at
		row.delivery = d
		t.deliveries[id] = row
	}
}

func (s *NotificationStore) GetChannel(_ context.Context, tenant, id shared.ID) (notification.Channel, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if t := s.tenants[tenant]; t != nil {
		if row, ok := t.channels[id]; ok && row.channel.DeletedAt == nil {
			return cloneChannel(row.channel), nil
		}
	}
	return notification.Channel{}, fmt.Errorf("notification channel %s: %w", id, shared.ErrNotFound)
}

func (s *NotificationStore) ListChannels(_ context.Context, tenant shared.ID) ([]notification.Channel, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t := s.tenants[tenant]
	if t == nil {
		return nil, nil
	}
	var out []notification.Channel
	for _, row := range t.channels {
		if row.channel.DeletedAt == nil {
			out = append(out, cloneChannel(row.channel))
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Name != out[j].Name {
			return out[i].Name < out[j].Name
		}
		return out[i].ID < out[j].ID
	})
	return out, nil
}

func (s *NotificationStore) CreateRule(ctx context.Context, rule notification.Rule) (notification.Rule, error) {
	return s.saveRule(ctx, rule, false)
}

func (s *NotificationStore) UpdateRule(ctx context.Context, rule notification.Rule) (notification.Rule, error) {
	return s.saveRule(ctx, rule, true)
}

func (s *NotificationStore) saveRule(ctx context.Context, rule notification.Rule, update bool) (notification.Rule, error) {
	if err := rule.Normalize(); err != nil {
		return notification.Rule{}, err
	}
	if len(rule.TeamIDs) > 0 {
		return notification.Rule{}, fmt.Errorf("notification team %s: %w", rule.TeamIDs[0], shared.ErrNotFound)
	}
	for _, id := range rule.EngagementIDs {
		if err := s.engagementExists(ctx, rule.TenantID, id); err != nil {
			return notification.Rule{}, err
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	t := s.tenant(rule.TenantID)
	existing, exists := t.rules[rule.ID]
	if update {
		if !exists || existing.Revision != rule.Revision-1 {
			return notification.Rule{}, fmt.Errorf("notification rule revision is stale: %w", shared.ErrConflict)
		}
	} else {
		if err := t.admit("rule"); err != nil {
			return notification.Rule{}, err
		}
		if exists {
			return notification.Rule{}, fmt.Errorf("insert notification rule %s: %w", rule.ID, shared.ErrConflict)
		}
	}
	for _, id := range rule.ChannelIDs {
		if row, ok := t.channels[id]; !ok || row.channel.DeletedAt != nil {
			return notification.Rule{}, fmt.Errorf("notification channel %s: %w", id, shared.ErrNotFound)
		}
	}
	s.checkpoint(ctx)
	stored := storedRule(rule)
	if update {
		stored.CreatedAt = existing.CreatedAt
	}
	t.rules[rule.ID] = stored
	return rule, nil
}

func (s *NotificationStore) engagementExists(ctx context.Context, tenant, id shared.ID) error {
	if s.engagements == nil {
		return fmt.Errorf("notification engagement %s: %w", id, shared.ErrNotFound)
	}
	if _, err := s.engagements.GetByIDInTenant(ctx, tenant, id); err != nil {
		if errors.Is(err, shared.ErrNotFound) {
			return fmt.Errorf("notification engagement %s: %w", id, shared.ErrNotFound)
		}
		return err
	}
	return nil
}

func (s *NotificationStore) DeleteRule(ctx context.Context, tenant, id shared.ID, revision int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	t := s.tenant(tenant)
	if rule, ok := t.rules[id]; !ok || rule.Revision != revision {
		return fmt.Errorf("notification rule revision is stale: %w", shared.ErrConflict)
	}
	s.checkpoint(ctx)
	delete(t.rules, id)
	return nil
}

func (s *NotificationStore) GetRule(_ context.Context, tenant, id shared.ID) (notification.Rule, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if t := s.tenants[tenant]; t != nil {
		if rule, ok := t.rules[id]; ok {
			return cloneRule(rule), nil
		}
	}
	return notification.Rule{}, fmt.Errorf("notification rule %s: %w", id, shared.ErrNotFound)
}

func (s *NotificationStore) ListRules(_ context.Context, tenant shared.ID) ([]notification.Rule, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t := s.tenants[tenant]
	if t == nil {
		return nil, nil
	}
	var out []notification.Rule
	for _, rule := range t.rules {
		out = append(out, cloneRule(rule))
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Name != out[j].Name {
			return out[i].Name < out[j].Name
		}
		return out[i].ID < out[j].ID
	})
	return out, nil
}

func (s *NotificationStore) PublishToChannel(ctx context.Context, e notification.Event, cid shared.ID) (shared.ID, error) {
	if err := e.Validate(); err != nil {
		return "", err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	t := s.tenant(e.TenantID)
	if e.Type == notification.EventTest {
		if row, ok := t.channels[cid]; !ok || !row.channel.Enabled || row.channel.DeletedAt != nil {
			return "", fmt.Errorf("notification channel %s: %w", cid, shared.ErrNotFound)
		}
		if t.recentTests(cid, s.now().Add(-time.Minute)) >= 10 {
			return "", fmt.Errorf("notification channel test rate limit exceeded: %w", shared.ErrSaturated)
		}
	}
	ids, err := s.publish(ctx, t, e, cid)
	if err != nil {
		return "", err
	}
	if len(ids) == 0 {
		return "", fmt.Errorf("notification channel %s: %w", cid, shared.ErrNotFound)
	}
	return ids[0], nil
}

// recentTests counts the distinct test events delivered to a channel since the cutoff.
func (t *notificationTenant) recentTests(channelID shared.ID, since time.Time) int {
	tests := map[shared.ID]bool{}
	for _, row := range t.deliveries {
		d := row.delivery
		if d.ChannelID == channelID && !d.CreatedAt.Before(since) && t.eventType(d.EventID) == notification.EventTest {
			tests[d.EventID] = true
		}
	}
	return len(tests)
}

func (t *notificationTenant) eventType(id shared.ID) notification.EventType {
	if e, ok := t.eventByID(id); ok {
		return e.Type
	}
	return ""
}

func (t *notificationTenant) eventByID(id shared.ID) (notification.Event, bool) {
	for _, e := range t.events {
		if e.ID == id {
			return e, true
		}
	}
	return notification.Event{}, false
}

// publish records the event and fans it out to matching rules' channels, or to only when it is set. An
// event whose source was already published returns that publication's deliveries. Every check runs
// before the first write, so a refusal leaves nothing behind even outside a transaction. Callers hold s.mu.
func (s *NotificationStore) publish(ctx context.Context, t *notificationTenant, e notification.Event, only shared.ID) ([]shared.ID, error) {
	if err := e.Validate(); err != nil {
		return nil, err
	}
	key := notificationSourceKey{e.SourceKind, e.SourceID}
	if existing, ok := t.events[key]; ok {
		var ids []shared.ID
		for id, row := range t.deliveries {
			if row.delivery.EventID == existing.ID {
				ids = append(ids, id)
			}
		}
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		return ids, nil
	}
	targets := map[shared.ID][]shared.ID{} // channel -> matched rules
	if !only.IsZero() {
		row, ok := t.channels[only]
		if !ok || !row.channel.Enabled || row.channel.DeletedAt != nil {
			return nil, fmt.Errorf("notification channel %s: %w", only, shared.ErrNotFound)
		}
		targets[only] = []shared.ID{}
	} else {
		ruleIDs := make([]shared.ID, 0, len(t.rules))
		for id := range t.rules {
			ruleIDs = append(ruleIDs, id)
		}
		sort.Slice(ruleIDs, func(i, j int) bool { return ruleIDs[i] < ruleIDs[j] })
		for _, id := range ruleIDs {
			rule := t.rules[id]
			if !rule.Matches(e) {
				continue
			}
			for _, cid := range rule.ChannelIDs {
				targets[cid] = append(targets[cid], rule.ID)
			}
		}
		for cid := range targets {
			if row, ok := t.channels[cid]; !ok || !row.channel.Enabled || row.channel.DeletedAt != nil {
				delete(targets, cid)
			}
		}
	}
	if len(targets) > 0 {
		if err := t.admit("delivery"); err != nil {
			return nil, err
		}
	}
	s.checkpoint(ctx)
	e.Data = append(json.RawMessage(nil), e.Data...)
	t.events[key] = e
	channelIDs := make([]shared.ID, 0, len(targets))
	for cid := range targets {
		channelIDs = append(channelIDs, cid)
	}
	sort.Slice(channelIDs, func(i, j int) bool { return channelIDs[i] < channelIDs[j] })
	ids := []shared.ID{}
	for _, cid := range channelIDs {
		channel := t.channels[cid].channel
		recipients := []string{""}
		if channel.Type == notification.ChannelEmail {
			recipients = channel.Recipients
		}
		for _, recipient := range recipients {
			if t.hasDelivery(e.ID, cid, recipient) {
				continue
			}
			did := notificationStableID(e.TenantID.String(), e.ID.String(), cid.String(), strings.ToLower(recipient))
			created := s.timestamp()
			t.deliveries[did] = notificationDeliveryRow{
				delivery: notification.Delivery{
					TenantID: e.TenantID, ID: did, EventID: e.ID, ChannelID: cid, ChannelType: channel.Type,
					Recipient: recipient, MatchedRuleIDs: append([]shared.ID{}, targets[cid]...),
					State: notification.DeliveryPending, CreatedAt: created, UpdatedAt: created,
				},
				channelVersion: channel.SecretVersion,
			}
			ids = append(ids, did)
		}
	}
	return ids, nil
}

func (t *notificationTenant) hasDelivery(eventID, channelID shared.ID, recipient string) bool {
	for _, row := range t.deliveries {
		d := row.delivery
		if d.EventID == eventID && d.ChannelID == channelID && d.Recipient == recipient {
			return true
		}
	}
	return false
}

// admit applies the per-tenant capacity PostgreSQL's notificationAdmission enforces.
func (t *notificationTenant) admit(kind string) error {
	count, maximum := 0, 0
	switch kind {
	case "channel":
		maximum = 50
		for _, row := range t.channels {
			if row.channel.DeletedAt == nil {
				count++
			}
		}
	case "rule":
		count, maximum = len(t.rules), 200
	case "delivery":
		maximum = 10000
		for _, row := range t.deliveries {
			if row.delivery.State == notification.DeliveryPending || row.delivery.State == notification.DeliveryRetrying {
				count++
			}
		}
	default:
		return shared.ErrValidation
	}
	if count >= maximum {
		return fmt.Errorf("%w: notification %s capacity reached", shared.ErrSaturated, kind)
	}
	return nil
}

func (s *NotificationStore) GetDelivery(_ context.Context, tenant, id shared.ID) (notification.Delivery, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if t := s.tenants[tenant]; t != nil {
		if row, ok := t.deliveries[id]; ok {
			return cloneDelivery(row.delivery), nil
		}
	}
	return notification.Delivery{}, fmt.Errorf("notification delivery %s: %w", id, shared.ErrNotFound)
}

func (s *NotificationStore) ListDeliveries(_ context.Context, f ports.NotificationDeliveryFilter) (notification.Page, error) {
	if f.Limit <= 0 {
		f.Limit = 50
	}
	if f.Limit > 200 {
		f.Limit = 200
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var out notification.Page
	t := s.tenants[f.TenantID]
	if t == nil {
		return out, nil
	}
	for _, row := range t.deliveries {
		d := row.delivery
		switch {
		case !f.From.IsZero() && d.CreatedAt.Before(f.From),
			!f.Until.IsZero() && d.CreatedAt.After(f.Until),
			!f.ChannelID.IsZero() && d.ChannelID != f.ChannelID,
			f.EventType != "" && t.eventType(d.EventID) != f.EventType,
			f.State != "" && d.State != f.State,
			!f.Before.IsZero() && !(d.CreatedAt.Before(f.Before) || (d.CreatedAt.Equal(f.Before) && d.ID < f.BeforeID)):
			continue
		}
		out.Items = append(out.Items, cloneDelivery(d))
	}
	sort.Slice(out.Items, func(i, j int) bool {
		if !out.Items[i].CreatedAt.Equal(out.Items[j].CreatedAt) {
			return out.Items[i].CreatedAt.After(out.Items[j].CreatedAt)
		}
		return out.Items[i].ID > out.Items[j].ID
	})
	if len(out.Items) > f.Limit {
		last := out.Items[f.Limit-1]
		out.Items = out.Items[:f.Limit]
		out.Next = last.CreatedAt.UTC().Format(time.RFC3339Nano) + "|" + last.ID.String()
	}
	return out, nil
}

// ListAttempts returns no attempts: none can start without the PostgreSQL attempt loop.
func (s *NotificationStore) ListAttempts(ctx context.Context, tenant, did shared.ID) ([]notification.Attempt, error) {
	if _, err := s.GetDelivery(ctx, tenant, did); err != nil {
		return nil, err
	}
	return nil, nil
}

func (s *NotificationStore) LoadWork(_ context.Context, tenant, did shared.ID) (ports.NotificationWork, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	notFound := fmt.Errorf("notification delivery %s: %w", did, shared.ErrNotFound)
	t := s.tenants[tenant]
	if t == nil {
		return ports.NotificationWork{}, notFound
	}
	row, ok := t.deliveries[did]
	if !ok {
		return ports.NotificationWork{}, notFound
	}
	event, eventOK := t.eventByID(row.delivery.EventID)
	channelRow, channelOK := t.channels[row.delivery.ChannelID]
	sealed, sealedOK := channelRow.sealed[row.channelVersion]
	if !eventOK || !channelOK || !sealedOK {
		return ports.NotificationWork{}, notFound
	}
	channel := cloneChannel(channelRow.channel)
	// The work carries the configuration version the delivery was created with, not the latest one.
	channel.SecretVersion, channel.Type, channel.DeletedAt = row.channelVersion, row.delivery.ChannelType, nil
	event.Data = append(json.RawMessage(nil), event.Data...)
	return ports.NotificationWork{Delivery: cloneDelivery(row.delivery), Event: event, Channel: channel, Sealed: sealed}, nil
}

func (s *NotificationStore) DeliveryStillRelevant(context.Context, ports.NotificationWork) (bool, error) {
	return false, errNotificationAttemptLoop
}

func (s *NotificationStore) BeginAttempt(context.Context, shared.ID, shared.ID, string, int64, shared.ID, time.Time) (notification.Attempt, error) {
	return notification.Attempt{}, errNotificationAttemptLoop
}

func (s *NotificationStore) FinishAttempt(context.Context, shared.ID, shared.ID, string, int64, shared.ID, time.Time, string, int, string, *time.Time) error {
	return errNotificationAttemptLoop
}

func (s *NotificationStore) CancelDelivery(context.Context, shared.ID, shared.ID, string, int64, string) error {
	return errNotificationAttemptLoop
}

func (s *NotificationStore) DeadLetterDelivery(context.Context, shared.ID, shared.ID, string) (bool, error) {
	return false, errNotificationAttemptLoop
}

// storedRule is the rule as PostgreSQL reads it back: channel ids in id order and empty lists, not nil.
func storedRule(r notification.Rule) notification.Rule {
	r = cloneRule(r)
	sort.Slice(r.ChannelIDs, func(i, j int) bool { return r.ChannelIDs[i] < r.ChannelIDs[j] })
	r.LeadTime = time.Duration(r.LeadTimeSecs) * time.Second
	return r
}

func cloneRule(r notification.Rule) notification.Rule {
	r.ActionTypes = append([]string{}, r.ActionTypes...)
	r.EngagementIDs = append([]shared.ID{}, r.EngagementIDs...)
	r.TeamIDs = append([]shared.ID{}, r.TeamIDs...)
	r.ChannelIDs = append([]shared.ID{}, r.ChannelIDs...)
	return r
}

func cloneChannel(c notification.Channel) notification.Channel {
	c.Recipients = cloneRecipients(c.Recipients)
	if c.DeletedAt != nil {
		deleted := *c.DeletedAt
		c.DeletedAt = &deleted
	}
	return c
}

func cloneRecipients(in []string) []string { return append([]string{}, in...) }

func cloneDelivery(d notification.Delivery) notification.Delivery {
	d.MatchedRuleIDs = append([]shared.ID{}, d.MatchedRuleIDs...)
	if d.NextAttemptAt != nil {
		next := *d.NextAttemptAt
		d.NextAttemptAt = &next
	}
	if d.DeliveredAt != nil {
		delivered := *d.DeliveredAt
		d.DeliveredAt = &delivered
	}
	return d
}

// notificationStableID matches the PostgreSQL adapter's stableID, so both name an event or a delivery
// identically.
func notificationStableID(parts ...string) shared.ID {
	h := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	return shared.ID(hex.EncodeToString(h[:16]))
}
