package configstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	domain "agent-compose/pkg/model"
	"agent-compose/pkg/projects"
)

func (s *projectStore) StageProjectRunCompletion(ctx context.Context, completion domain.ProjectRunCompletionRecord, events []domain.ProjectRunEventRecord) (domain.ProjectRunCompletionRecord, bool, error) {
	completion.RunID = strings.TrimSpace(completion.RunID)
	if completion.RunID == "" || strings.TrimSpace(completion.TargetStatus) == "" || strings.TrimSpace(completion.TransitionJSON) == "" {
		return domain.ProjectRunCompletionRecord{}, false, fmt.Errorf("run completion id, target status, and transition are required")
	}
	if err := validateRunEventBatch(completion.RunID, events); err != nil {
		return domain.ProjectRunCompletionRecord{}, false, err
	}
	now := time.Now().UTC()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return domain.ProjectRunCompletionRecord{}, false, fmt.Errorf("begin stage project run completion: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	result, err := tx.ExecContext(ctx, `INSERT INTO project_run_completion(
		run_id, target_status, transition_json, cleanup_action, attempt, last_error, next_attempt_at, created_at, updated_at
	) VALUES(?, ?, ?, ?, 0, '', 0, ?, ?) ON CONFLICT(run_id) DO NOTHING`,
		completion.RunID, completion.TargetStatus, completion.TransitionJSON, completion.CleanupAction, now.UnixMilli(), now.UnixMilli())
	if err != nil {
		return domain.ProjectRunCompletionRecord{}, false, fmt.Errorf("insert project run completion %s: %w", completion.RunID, err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return domain.ProjectRunCompletionRecord{}, false, fmt.Errorf("inspect project run completion insert %s: %w", completion.RunID, err)
	}
	created := rows > 0
	if created {
		if _, _, err := appendProjectRunEventsTx(ctx, tx, events); err != nil {
			return domain.ProjectRunCompletionRecord{}, false, fmt.Errorf("append staged project run completion events: %w", err)
		}
		// Keep queued timeouts and interruptions visibly unstarted through the
		// completion journal; other terminal work remains active while cleanup runs.
		if _, err := tx.ExecContext(ctx, `UPDATE project_run SET
			status = CASE WHEN status = 'pending' AND ? IN ('timed_out', 'interrupted') THEN status ELSE 'running' END,
			cleanup_error = '',
			started_at = CASE WHEN started_at = 0 AND NOT (status = 'pending' AND ? IN ('timed_out', 'interrupted')) THEN ? ELSE started_at END,
			updated_at = ?
			WHERE run_id = ? AND status IN ('pending', 'running')`, completion.TargetStatus, completion.TargetStatus, now.UnixMilli(), now.Unix(), completion.RunID); err != nil {
			return domain.ProjectRunCompletionRecord{}, false, fmt.Errorf("keep project run %s active during completion: %w", completion.RunID, err)
		}
	}
	stored, err := getProjectRunCompletionTx(ctx, tx, completion.RunID)
	if err != nil {
		return domain.ProjectRunCompletionRecord{}, false, err
	}
	if err := tx.Commit(); err != nil {
		return domain.ProjectRunCompletionRecord{}, false, fmt.Errorf("commit staged project run completion: %w", err)
	}
	return stored, created, nil
}

func (s *projectStore) GetProjectRunCompletion(ctx context.Context, runID string) (domain.ProjectRunCompletionRecord, error) {
	return scanProjectRunCompletion(s.db.QueryRowContext(ctx, projectRunCompletionSelect+` WHERE run_id = ?`, strings.TrimSpace(runID)).Scan)
}

func (s *projectStore) ListDueProjectRunCompletions(ctx context.Context, now time.Time, limit int) ([]domain.ProjectRunCompletionRecord, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.db.QueryContext(ctx, projectRunCompletionSelect+` WHERE next_attempt_at <= ? ORDER BY next_attempt_at, created_at LIMIT ?`, now.UTC().UnixMilli(), limit)
	if err != nil {
		return nil, fmt.Errorf("query due project run completions: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var items []domain.ProjectRunCompletionRecord
	for rows.Next() {
		item, scanErr := scanProjectRunCompletion(rows.Scan)
		if scanErr != nil {
			return nil, scanErr
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *projectStore) RecordProjectRunCompletionFailure(ctx context.Context, failure domain.ProjectRunCompletionFailure) error {
	runID, message, attempt, next := failure.RunID, failure.Message, failure.Attempt, failure.NextAttemptAt
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin record project run completion failure: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, `UPDATE project_run_completion SET attempt = ?, last_error = ?, next_attempt_at = ?, updated_at = ? WHERE run_id = ?`, attempt, message, next.UTC().UnixMilli(), time.Now().UTC().UnixMilli(), runID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE project_run SET cleanup_error = ?, updated_at = ? WHERE run_id = ?`, message, time.Now().UTC().Unix(), runID); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *projectStore) FinalizeProjectRunCompletion(ctx context.Context, run domain.ProjectRunRecord, statusEvent domain.ProjectRunEventRecord) (domain.ProjectRunRecord, error) {
	run, err := projects.NormalizeRunRecord(run)
	if err != nil {
		return domain.ProjectRunRecord{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return domain.ProjectRunRecord{}, fmt.Errorf("begin finalize project run completion: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := getProjectRunCompletionTx(ctx, tx, run.RunID); err != nil {
		return domain.ProjectRunRecord{}, err
	}
	run.CleanupError = ""
	result, err := tx.ExecContext(ctx, `UPDATE project_run SET
		project_id=?, project_name=?, project_revision=?, agent_name=?, agent_id=?, source=?, scheduler_id=?, scheduler_run_id=?, trigger_id=?, status=?,
		sandbox_id=?, exit_code=?, error=?, error_stack=?, prompt=?, output=?, result_json=?, logs_path=?, artifacts_dir=?, cleanup_error='', cleanup_policy=?, sandbox_created=?, driver=?, image_ref=?,
		started_at=?, completed_at=?, duration_ms=?, updated_at=? WHERE run_id=?`,
		run.ProjectID, run.ProjectName, run.ProjectRevision, run.AgentName, run.AgentID, run.Source, run.SchedulerID, nullableSchedulerRunID(run.SchedulerRunID), run.TriggerID, run.Status,
		run.SandboxID, run.ExitCode, run.Error, run.ErrorStack, run.Prompt, run.Output, run.ResultJSON, run.LogsPath, run.ArtifactsDir, run.CleanupPolicy, BoolToInt(run.SandboxCreated), run.Driver, run.ImageRef,
		domain.NonZeroTimeUnixMilli(run.StartedAt), domain.NonZeroTimeUnixMilli(run.CompletedAt), run.DurationMs, time.Now().UTC().Unix(), run.RunID)
	if err != nil {
		return domain.ProjectRunRecord{}, err
	}
	if rows, _ := result.RowsAffected(); rows == 0 {
		return domain.ProjectRunRecord{}, domain.ResourceError(domain.ErrNotFound, "project run", run.RunID, "project run not found", nil)
	}
	if _, _, err := appendProjectRunEventsTx(ctx, tx, []domain.ProjectRunEventRecord{statusEvent}); err != nil {
		return domain.ProjectRunRecord{}, fmt.Errorf("append terminal project run status event: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM project_run_completion WHERE run_id = ?`, run.RunID); err != nil {
		return domain.ProjectRunRecord{}, err
	}
	stored, err := getProjectRunTx(ctx, tx, run.RunID)
	if err != nil {
		return domain.ProjectRunRecord{}, err
	}
	if err := tx.Commit(); err != nil {
		return domain.ProjectRunRecord{}, fmt.Errorf("commit finalized project run completion: %w", err)
	}
	return stored, nil
}

func (s *projectStore) ProjectRunCompletionForSandbox(ctx context.Context, sandboxID string) (string, bool, error) {
	var runID string
	err := s.db.QueryRowContext(ctx, `SELECT c.run_id FROM project_run_completion c JOIN project_run r ON r.run_id=c.run_id WHERE r.sandbox_id=? LIMIT 1`, strings.TrimSpace(sandboxID)).Scan(&runID)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	return runID, err == nil, err
}

const projectRunCompletionSelect = `SELECT run_id, target_status, transition_json, cleanup_action, attempt, last_error, next_attempt_at, created_at, updated_at FROM project_run_completion`

func getProjectRunCompletionTx(ctx context.Context, tx *sql.Tx, runID string) (domain.ProjectRunCompletionRecord, error) {
	return scanProjectRunCompletion(tx.QueryRowContext(ctx, projectRunCompletionSelect+` WHERE run_id = ?`, runID).Scan)
}

func scanProjectRunCompletion(scan func(...any) error) (domain.ProjectRunCompletionRecord, error) {
	var item domain.ProjectRunCompletionRecord
	var next, created, updated int64
	if err := scan(&item.RunID, &item.TargetStatus, &item.TransitionJSON, &item.CleanupAction, &item.Attempt, &item.LastError, &next, &created, &updated); err != nil {
		return domain.ProjectRunCompletionRecord{}, err
	}
	if next > 0 {
		item.NextAttemptAt = time.UnixMilli(next).UTC()
	}
	item.CreatedAt = time.UnixMilli(created).UTC()
	item.UpdatedAt = time.UnixMilli(updated).UTC()
	return item, nil
}
