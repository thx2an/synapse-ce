package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/KKloudTarus/synapse-ce/internal/domain/integration"
	"github.com/KKloudTarus/synapse-ce/internal/domain/projectanalysis"
	"github.com/KKloudTarus/synapse-ce/internal/domain/shared"
	"github.com/KKloudTarus/synapse-ce/internal/usecase/ports"
)

// ProjectAnalysisStore persists immutable Project analysis snapshots.
type ProjectAnalysisStore struct{ pool *pgxpool.Pool }

func NewProjectAnalysisStore(pool *pgxpool.Pool) *ProjectAnalysisStore {
	return &ProjectAnalysisStore{pool: pool}
}

var _ ports.ProjectAnalysisStore = (*ProjectAnalysisStore)(nil)
var _ ports.IntegrationAnalysisMatcher = (*ProjectAnalysisStore)(nil)

func (r *ProjectAnalysisStore) MatchIntegrationAnalysis(ctx context.Context, projectID shared.ID, revision string) (analysisID shared.ID, state integration.CorrelationState, err error) {
	tenantID, ok := shared.TenantFrom(ctx)
	revision = strings.TrimSpace(revision)
	if !ok || projectID.IsZero() || revision == "" {
		return "", integration.CorrelationMissing, nil
	}
	var matches []shared.ID
	err = WithContextTenant(ctx, r.pool, func(tx pgx.Tx) error {
		rows, queryErr := tx.Query(ctx, `SELECT id FROM project_analyses WHERE tenant_id=$1 AND project_id=$2
			AND ((payload->>'source_commit')=$3 OR (payload#>>'{source_revision,head}')=$3)
			ORDER BY created_at DESC,id COLLATE "C" DESC LIMIT 2`, tenantID.String(), projectID.String(), revision)
		if queryErr != nil {
			return fmt.Errorf("match integration analysis: %w", queryErr)
		}
		defer rows.Close()
		for rows.Next() {
			var id shared.ID
			if scanErr := rows.Scan(&id); scanErr != nil {
				return scanErr
			}
			matches = append(matches, id)
		}
		return rows.Err()
	})
	if err != nil {
		return "", "", err
	}
	switch len(matches) {
	case 0:
		return "", integration.CorrelationMissing, nil
	case 1:
		return matches[0], integration.CorrelationLinked, nil
	default:
		return "", integration.CorrelationAmbiguous, nil
	}
}

func (r *ProjectAnalysisStore) Save(ctx context.Context, analysis projectanalysis.Analysis) error {
	return r.SaveWithResult(ctx, analysis, nil)
}

func (r *ProjectAnalysisStore) SaveWithResult(ctx context.Context, analysis projectanalysis.Analysis, result []byte) error {
	payload, err := json.Marshal(analysis)
	if err != nil {
		return fmt.Errorf("marshal project analysis: %w", err)
	}
	_, err = r.pool.Exec(ctx, `INSERT INTO project_analyses (id, tenant_id, project_id, created_at, payload, result)
		VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT (id) DO NOTHING`,
		analysis.ID, analysis.TenantID, analysis.ProjectID, analysis.CreatedAt, payload, result)
	if err != nil {
		return fmt.Errorf("insert project analysis: %w", err)
	}
	return nil
}

func (r *ProjectAnalysisStore) LatestForProjects(ctx context.Context, tenantID shared.ID, projectIDs []shared.ID) (map[shared.ID]projectanalysis.Analysis, error) {
	ids := make([]string, len(projectIDs))
	for i, id := range projectIDs {
		ids[i] = id.String()
	}
	if len(ids) == 0 {
		return map[shared.ID]projectanalysis.Analysis{}, nil
	}
	rows, err := r.pool.Query(ctx, `SELECT DISTINCT ON (project_id) project_id, payload FROM project_analyses WHERE tenant_id=$1 AND project_id = ANY($2) ORDER BY project_id, created_at DESC, id COLLATE "C" DESC`, tenantID.String(), ids)
	if err != nil {
		return nil, fmt.Errorf("list latest project analyses: %w", err)
	}
	defer rows.Close()
	out := map[shared.ID]projectanalysis.Analysis{}
	for rows.Next() {
		var id string
		var payload []byte
		if err := rows.Scan(&id, &payload); err != nil {
			return nil, fmt.Errorf("scan latest project analysis: %w", err)
		}
		var analysis projectanalysis.Analysis
		if err := json.Unmarshal(payload, &analysis); err != nil {
			return nil, fmt.Errorf("decode project analysis: %w", err)
		}
		out[shared.ID(id)] = analysis
	}
	return out, rows.Err()
}

func (r *ProjectAnalysisStore) LatestWithResult(ctx context.Context, tenantID, projectID shared.ID) (projectanalysis.Analysis, []byte, error) {
	var row pgx.Row
	if tenantID.IsZero() {
		row = r.pool.QueryRow(ctx, `SELECT payload, result FROM project_analyses WHERE project_id=$1 AND result IS NOT NULL ORDER BY created_at DESC, id COLLATE "C" DESC LIMIT 1`, projectID.String())
	} else {
		row = r.pool.QueryRow(ctx, `SELECT payload, result FROM project_analyses WHERE tenant_id=$1 AND project_id=$2 AND result IS NOT NULL ORDER BY created_at DESC, id COLLATE "C" DESC LIMIT 1`, tenantID.String(), projectID.String())
	}
	var payload, result []byte
	if err := row.Scan(&payload, &result); errors.Is(err, pgx.ErrNoRows) {
		return projectanalysis.Analysis{}, nil, shared.ErrNotFound
	} else if err != nil {
		return projectanalysis.Analysis{}, nil, fmt.Errorf("latest project analysis: %w", err)
	}
	var analysis projectanalysis.Analysis
	if err := json.Unmarshal(payload, &analysis); err != nil {
		return projectanalysis.Analysis{}, nil, fmt.Errorf("decode project analysis: %w", err)
	}
	return analysis, result, nil
}

func (r *ProjectAnalysisStore) List(ctx context.Context, tenantID, projectID shared.ID, limit int, beforeCreatedAt time.Time, beforeID shared.ID) ([]projectanalysis.Analysis, bool, error) {
	var (
		rows pgx.Rows
		err  error
	)
	cursor := beforeCreatedAt
	if beforeCreatedAt.IsZero() {
		cursor = time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC)
	}
	if tenantID.IsZero() {
		rows, err = r.pool.Query(ctx, `SELECT payload FROM project_analyses
			WHERE project_id=$1 AND (created_at < $2 OR (created_at = $2 AND id COLLATE "C" < $3))
			ORDER BY created_at DESC, id COLLATE "C" DESC LIMIT $4`, projectID.String(), cursor, beforeID.String(), limit+1)
	} else {
		rows, err = r.pool.Query(ctx, `SELECT payload FROM project_analyses
			WHERE tenant_id=$1 AND project_id=$2 AND (created_at < $3 OR (created_at = $3 AND id COLLATE "C" < $4))
			ORDER BY created_at DESC, id COLLATE "C" DESC LIMIT $5`, tenantID.String(), projectID.String(), cursor, beforeID.String(), limit+1)
	}
	if err != nil {
		return nil, false, fmt.Errorf("list project analyses: %w", err)
	}
	defer rows.Close()
	out := make([]projectanalysis.Analysis, 0, limit+1)
	for rows.Next() {
		analysis, err := scanProjectAnalysis(rows)
		if err != nil {
			return nil, false, err
		}
		out = append(out, analysis)
	}
	if err := rows.Err(); err != nil {
		return nil, false, err
	}
	hasMore := len(out) > limit
	if hasMore {
		out = out[:limit]
	}
	return out, hasMore, nil
}

func (r *ProjectAnalysisStore) Get(ctx context.Context, tenantID, projectID, analysisID shared.ID) (projectanalysis.Analysis, error) {
	var row pgx.Row
	if tenantID.IsZero() {
		row = r.pool.QueryRow(ctx, `SELECT payload FROM project_analyses WHERE project_id=$1 AND id=$2`, projectID.String(), analysisID.String())
	} else {
		row = r.pool.QueryRow(ctx, `SELECT payload FROM project_analyses WHERE tenant_id=$1 AND project_id=$2 AND id=$3`, tenantID.String(), projectID.String(), analysisID.String())
	}
	analysis, err := scanProjectAnalysis(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return projectanalysis.Analysis{}, shared.ErrNotFound
	}
	if err != nil {
		return projectanalysis.Analysis{}, fmt.Errorf("get project analysis: %w", err)
	}
	return analysis, nil
}

func scanProjectAnalysis(row rowScanner) (projectanalysis.Analysis, error) {
	var payload []byte
	if err := row.Scan(&payload); err != nil {
		return projectanalysis.Analysis{}, err
	}
	var analysis projectanalysis.Analysis
	if err := json.Unmarshal(payload, &analysis); err != nil {
		return projectanalysis.Analysis{}, fmt.Errorf("decode project analysis: %w", err)
	}
	return analysis, nil
}
