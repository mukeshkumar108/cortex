-- Consolidate tenant alias rows into canonical tenant ids.
-- Current mapping:
--   sophie-prod -> default
--
-- This migration is idempotent and safe to re-run.

DO $$
DECLARE
    v_alias TEXT := 'sophie-prod';
    v_canonical TEXT := 'default';
BEGIN
    IF v_alias = v_canonical THEN
        RETURN;
    END IF;

    -- Tables with tenant-only scope and no tenant-based PK conflicts.
    UPDATE loops SET tenant_id = v_canonical WHERE tenant_id = v_alias;
    UPDATE graphiti_outbox SET tenant_id = v_canonical WHERE tenant_id = v_alias;
    UPDATE startbrief_history SET tenant_id = v_canonical WHERE tenant_id = v_alias;
    UPDATE checkin_tactic_log SET tenant_id = v_canonical WHERE tenant_id = v_alias;

    -- user_identity: PK (tenant_id, user_id)
    INSERT INTO user_identity (tenant_id, user_id, data, created_at, updated_at)
    SELECT v_canonical, user_id, data, created_at, updated_at
    FROM user_identity
    WHERE tenant_id = v_alias
    ON CONFLICT (tenant_id, user_id) DO UPDATE
    SET
        data = CASE
            WHEN EXCLUDED.updated_at >= user_identity.updated_at THEN EXCLUDED.data
            ELSE user_identity.data
        END,
        created_at = LEAST(user_identity.created_at, EXCLUDED.created_at),
        updated_at = GREATEST(user_identity.updated_at, EXCLUDED.updated_at);
    DELETE FROM user_identity WHERE tenant_id = v_alias;

    -- identity_cache: PK (tenant_id, user_id)
    INSERT INTO identity_cache (tenant_id, user_id, preferred_name, timezone, facts, last_synced_at)
    SELECT v_canonical, user_id, preferred_name, timezone, facts, last_synced_at
    FROM identity_cache
    WHERE tenant_id = v_alias
    ON CONFLICT (tenant_id, user_id) DO UPDATE
    SET
        preferred_name = CASE
            WHEN EXCLUDED.last_synced_at >= identity_cache.last_synced_at THEN EXCLUDED.preferred_name
            ELSE identity_cache.preferred_name
        END,
        timezone = CASE
            WHEN EXCLUDED.last_synced_at >= identity_cache.last_synced_at THEN EXCLUDED.timezone
            ELSE identity_cache.timezone
        END,
        facts = CASE
            WHEN EXCLUDED.last_synced_at >= identity_cache.last_synced_at THEN EXCLUDED.facts
            ELSE identity_cache.facts
        END,
        last_synced_at = GREATEST(identity_cache.last_synced_at, EXCLUDED.last_synced_at);
    DELETE FROM identity_cache WHERE tenant_id = v_alias;

    -- session_buffer: PK (tenant_id, session_id)
    INSERT INTO session_buffer (
        tenant_id, session_id, user_id, messages, session_state, rolling_summary,
        closed_at, created_at, updated_at
    )
    SELECT
        v_canonical, session_id, user_id, messages, session_state, rolling_summary,
        closed_at, created_at, updated_at
    FROM session_buffer
    WHERE tenant_id = v_alias
    ON CONFLICT (tenant_id, session_id) DO UPDATE
    SET
        user_id = EXCLUDED.user_id,
        messages = CASE
            WHEN EXCLUDED.updated_at >= session_buffer.updated_at THEN EXCLUDED.messages
            ELSE session_buffer.messages
        END,
        session_state = CASE
            WHEN EXCLUDED.updated_at >= session_buffer.updated_at THEN EXCLUDED.session_state
            ELSE session_buffer.session_state
        END,
        rolling_summary = CASE
            WHEN EXCLUDED.updated_at >= session_buffer.updated_at THEN EXCLUDED.rolling_summary
            ELSE session_buffer.rolling_summary
        END,
        closed_at = COALESCE(EXCLUDED.closed_at, session_buffer.closed_at),
        created_at = LEAST(session_buffer.created_at, EXCLUDED.created_at),
        updated_at = GREATEST(session_buffer.updated_at, EXCLUDED.updated_at);
    DELETE FROM session_buffer WHERE tenant_id = v_alias;

    -- session_transcript: PK (tenant_id, session_id)
    INSERT INTO session_transcript (
        tenant_id, session_id, user_id, messages, created_at, updated_at
    )
    SELECT
        v_canonical, session_id, user_id, messages, created_at, updated_at
    FROM session_transcript
    WHERE tenant_id = v_alias
    ON CONFLICT (tenant_id, session_id) DO UPDATE
    SET
        user_id = EXCLUDED.user_id,
        messages = CASE
            WHEN EXCLUDED.updated_at >= session_transcript.updated_at THEN EXCLUDED.messages
            ELSE session_transcript.messages
        END,
        created_at = LEAST(session_transcript.created_at, EXCLUDED.created_at),
        updated_at = GREATEST(session_transcript.updated_at, EXCLUDED.updated_at);
    DELETE FROM session_transcript WHERE tenant_id = v_alias;

    -- user_model: PK (tenant_id, user_id)
    INSERT INTO user_model (
        tenant_id, user_id, model, version, last_source, created_at, updated_at,
        narrative_stable, narrative_current
    )
    SELECT
        v_canonical, user_id, model, version, last_source, created_at, updated_at,
        narrative_stable, narrative_current
    FROM user_model
    WHERE tenant_id = v_alias
    ON CONFLICT (tenant_id, user_id) DO UPDATE
    SET
        model = CASE
            WHEN EXCLUDED.updated_at >= user_model.updated_at THEN EXCLUDED.model
            ELSE user_model.model
        END,
        version = GREATEST(user_model.version, EXCLUDED.version),
        last_source = CASE
            WHEN EXCLUDED.updated_at >= user_model.updated_at THEN EXCLUDED.last_source
            ELSE user_model.last_source
        END,
        narrative_stable = COALESCE(
            CASE
                WHEN EXCLUDED.updated_at >= user_model.updated_at THEN EXCLUDED.narrative_stable
                ELSE user_model.narrative_stable
            END,
            user_model.narrative_stable,
            EXCLUDED.narrative_stable
        ),
        narrative_current = COALESCE(
            CASE
                WHEN EXCLUDED.updated_at >= user_model.updated_at THEN EXCLUDED.narrative_current
                ELSE user_model.narrative_current
            END,
            user_model.narrative_current,
            EXCLUDED.narrative_current
        ),
        created_at = LEAST(user_model.created_at, EXCLUDED.created_at),
        updated_at = GREATEST(user_model.updated_at, EXCLUDED.updated_at);
    DELETE FROM user_model WHERE tenant_id = v_alias;

    -- daily_analysis: PK (tenant_id, user_id, analysis_date)
    INSERT INTO daily_analysis (
        tenant_id, user_id, analysis_date, themes, scores, steering_note, confidence,
        source, metadata, created_at, updated_at
    )
    SELECT
        v_canonical, user_id, analysis_date, themes, scores, steering_note, confidence,
        source, metadata, created_at, updated_at
    FROM daily_analysis
    WHERE tenant_id = v_alias
    ON CONFLICT (tenant_id, user_id, analysis_date) DO UPDATE
    SET
        themes = CASE
            WHEN EXCLUDED.updated_at >= daily_analysis.updated_at THEN EXCLUDED.themes
            ELSE daily_analysis.themes
        END,
        scores = CASE
            WHEN EXCLUDED.updated_at >= daily_analysis.updated_at THEN EXCLUDED.scores
            ELSE daily_analysis.scores
        END,
        steering_note = CASE
            WHEN EXCLUDED.updated_at >= daily_analysis.updated_at THEN EXCLUDED.steering_note
            ELSE daily_analysis.steering_note
        END,
        confidence = CASE
            WHEN EXCLUDED.updated_at >= daily_analysis.updated_at THEN EXCLUDED.confidence
            ELSE daily_analysis.confidence
        END,
        source = CASE
            WHEN EXCLUDED.updated_at >= daily_analysis.updated_at THEN EXCLUDED.source
            ELSE daily_analysis.source
        END,
        metadata = CASE
            WHEN EXCLUDED.updated_at >= daily_analysis.updated_at THEN EXCLUDED.metadata
            ELSE daily_analysis.metadata
        END,
        created_at = LEAST(daily_analysis.created_at, EXCLUDED.created_at),
        updated_at = GREATEST(daily_analysis.updated_at, EXCLUDED.updated_at);
    DELETE FROM daily_analysis WHERE tenant_id = v_alias;

    -- user_model_enrichment_state: PK (tenant_id, user_id)
    INSERT INTO user_model_enrichment_state (
        tenant_id, user_id, last_enriched_at, last_daily_enriched_at, last_weekly_enriched_at,
        next_retry_at, retry_attempts, last_error, last_mode, updated_at,
        last_hygiene_at, next_hygiene_at
    )
    SELECT
        v_canonical, user_id, last_enriched_at, last_daily_enriched_at, last_weekly_enriched_at,
        next_retry_at, retry_attempts, last_error, last_mode, updated_at,
        last_hygiene_at, next_hygiene_at
    FROM user_model_enrichment_state
    WHERE tenant_id = v_alias
    ON CONFLICT (tenant_id, user_id) DO UPDATE
    SET
        last_enriched_at = CASE
            WHEN EXCLUDED.updated_at >= user_model_enrichment_state.updated_at THEN EXCLUDED.last_enriched_at
            ELSE user_model_enrichment_state.last_enriched_at
        END,
        last_daily_enriched_at = CASE
            WHEN EXCLUDED.updated_at >= user_model_enrichment_state.updated_at THEN EXCLUDED.last_daily_enriched_at
            ELSE user_model_enrichment_state.last_daily_enriched_at
        END,
        last_weekly_enriched_at = CASE
            WHEN EXCLUDED.updated_at >= user_model_enrichment_state.updated_at THEN EXCLUDED.last_weekly_enriched_at
            ELSE user_model_enrichment_state.last_weekly_enriched_at
        END,
        next_retry_at = CASE
            WHEN EXCLUDED.updated_at >= user_model_enrichment_state.updated_at THEN EXCLUDED.next_retry_at
            ELSE user_model_enrichment_state.next_retry_at
        END,
        retry_attempts = CASE
            WHEN EXCLUDED.updated_at >= user_model_enrichment_state.updated_at THEN EXCLUDED.retry_attempts
            ELSE user_model_enrichment_state.retry_attempts
        END,
        last_error = CASE
            WHEN EXCLUDED.updated_at >= user_model_enrichment_state.updated_at THEN EXCLUDED.last_error
            ELSE user_model_enrichment_state.last_error
        END,
        last_mode = CASE
            WHEN EXCLUDED.updated_at >= user_model_enrichment_state.updated_at THEN EXCLUDED.last_mode
            ELSE user_model_enrichment_state.last_mode
        END,
        last_hygiene_at = CASE
            WHEN EXCLUDED.updated_at >= user_model_enrichment_state.updated_at THEN EXCLUDED.last_hygiene_at
            ELSE user_model_enrichment_state.last_hygiene_at
        END,
        next_hygiene_at = CASE
            WHEN EXCLUDED.updated_at >= user_model_enrichment_state.updated_at THEN EXCLUDED.next_hygiene_at
            ELSE user_model_enrichment_state.next_hygiene_at
        END,
        updated_at = GREATEST(user_model_enrichment_state.updated_at, EXCLUDED.updated_at);
    DELETE FROM user_model_enrichment_state WHERE tenant_id = v_alias;

    -- user_model_write_claims: PK (tenant_id, user_id)
    INSERT INTO user_model_write_claims (
        tenant_id, user_id, claim_owner, claimed_at, expires_at
    )
    SELECT
        v_canonical, user_id, claim_owner, claimed_at, expires_at
    FROM user_model_write_claims
    WHERE tenant_id = v_alias
    ON CONFLICT (tenant_id, user_id) DO UPDATE
    SET
        claim_owner = EXCLUDED.claim_owner,
        claimed_at = GREATEST(user_model_write_claims.claimed_at, EXCLUDED.claimed_at),
        expires_at = GREATEST(user_model_write_claims.expires_at, EXCLUDED.expires_at);
    DELETE FROM user_model_write_claims WHERE tenant_id = v_alias;

    -- habit_dedupe_state: PK (tenant_id, user_id)
    INSERT INTO habit_dedupe_state (
        tenant_id, user_id, last_run_date, updated_at
    )
    SELECT
        v_canonical, user_id, last_run_date, updated_at
    FROM habit_dedupe_state
    WHERE tenant_id = v_alias
    ON CONFLICT (tenant_id, user_id) DO UPDATE
    SET
        last_run_date = GREATEST(habit_dedupe_state.last_run_date, EXCLUDED.last_run_date),
        updated_at = GREATEST(habit_dedupe_state.updated_at, EXCLUDED.updated_at);
    DELETE FROM habit_dedupe_state WHERE tenant_id = v_alias;
END $$;
