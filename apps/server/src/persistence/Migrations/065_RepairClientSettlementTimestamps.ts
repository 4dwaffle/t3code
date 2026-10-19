import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

// Follow-up to 046 for the pre-reactor client-attributed auto-settle sweeps
// reported in #10937. Those events carry actor_kind = 'client' with a plain
// command_id, not the server:auto-settle: shape 046 matches, so 046 silently
// skipped them. A one-off manual settle has the same event shape, so this
// only treats an event as automatic when it is also part of a dense burst:
// >= 10 distinct threads settled by one of the known buggy desktop builds
// (0.0.35-0.0.38, before the server-side reactor shipped in #8600) within a
// two-minute span. Membership is gap-based (consecutive qualifying events no
// more than 30 seconds apart stay in the same cluster) rather than
// "anything within two minutes of a qualifying event," so a later unrelated
// manual settle just outside the sweep cannot get chained into it through an
// overlapping window.
//
// Three repair sites, because the V1 -> V2 import can land in between:
// - projection_threads.settled_at is V1's own projection. It is still the
//   only copy of the truth for a thread that has not been imported into V2
//   yet, and importedThread() in LegacyV1ThreadImporter reads this column
//   verbatim, so fixing it here also fixes every thread imported from this
//   point forward.
// - orchestration_v2_projection_threads.payload_json is V2's projection. For
//   a thread already imported before this migration ran, the import already
//   copied the bad settled_at into this JSON blob, and nothing re-reads
//   projection_threads afterwards. The second statement below patches that
//   copy directly, using the same detection and the same last-activity
//   computation, sourced from V1 user messages (kept for the transcript
//   import) and V1 turn events, so it does not depend on import status.
// - V2 thread events. The import and every later thread snapshot event carry
//   the whole thread, bad settledAt included, and a V2 projection rebuild
//   replays them. They are patched too, as 064 does, so a rebuild keeps the
//   fix. V1 events stay as recorded: nothing replays them anymore.
export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  // Shared burst-detection logic, repeated in each statement below because a
  // WITH clause does not carry over between statements.
  const detectBurst = sql`
    legacy_settlements AS (
      SELECT
        stream_id AS thread_id,
        occurred_at,
        unixepoch(occurred_at, 'subsec') AS occurred_seconds
      FROM orchestration_events
      WHERE aggregate_kind = 'thread'
        AND event_type = 'thread.settled'
        AND actor_kind = 'client'
        AND application_event_version = 1
        AND json_type(payload_json, '$.settledAt') = 'text'
        AND json_extract(payload_json, '$.settledAt') = occurred_at
        AND json_extract(metadata_json, '$.origin.appVersion') IN ('0.0.35', '0.0.36', '0.0.37', '0.0.38')
        AND julianday(occurred_at) < julianday('2026-09-05T00:00:00.000Z')
    ),
    -- Gap in seconds to the chronologically previous qualifying event, across
    -- all threads. The sweep fired roughly once a second; a real person
    -- resettling a thread shortly after a sweep lands well outside the gap
    -- below, so it starts a cluster of its own instead of joining the sweep.
    ordered AS (
      SELECT
        thread_id,
        occurred_at,
        occurred_seconds,
        occurred_seconds - LAG(occurred_seconds) OVER (ORDER BY occurred_seconds, thread_id)
          AS gap_seconds
      FROM legacy_settlements
    ),
    -- A new cluster starts whenever the gap exceeds the threshold (or at the
    -- very first row); the running SUM then labels every row with the
    -- cluster it belongs to.
    clustered AS (
      SELECT
        thread_id,
        occurred_at,
        occurred_seconds,
        SUM(CASE WHEN gap_seconds IS NULL OR gap_seconds > 30 THEN 1 ELSE 0 END)
          OVER (ORDER BY occurred_seconds, thread_id) AS cluster_id
      FROM ordered
    ),
    burst_clusters AS (
      SELECT
        cluster_id,
        COUNT(DISTINCT thread_id) AS distinct_thread_count,
        MAX(occurred_seconds) - MIN(occurred_seconds) AS span_seconds
      FROM clustered
      GROUP BY cluster_id
    ),
    automatic_settlements AS (
      SELECT clustered.thread_id, clustered.occurred_at
      FROM clustered
      JOIN burst_clusters ON burst_clusters.cluster_id = clustered.cluster_id
      WHERE burst_clusters.distinct_thread_count >= 10
        AND burst_clusters.span_seconds <= 120
    ),
    swept_threads AS (
      SELECT DISTINCT thread_id FROM automatic_settlements
    ),
    -- Only the swept threads' history matters, and stream_id is indexed.
    turn_events AS (
      SELECT stream_id AS thread_id, sequence, event_type, occurred_at, payload_json
      FROM orchestration_events
      WHERE aggregate_kind = 'thread'
        AND application_event_version = 1
        AND stream_id IN (SELECT thread_id FROM swept_threads)
        AND event_type IN (
          'thread.turn-start-requested',
          'thread.turn-diff-completed',
          'thread.turn-interrupt-requested',
          'thread.message-sent',
          'thread.session-set'
        )
    ),
    turn_openings AS (
      SELECT thread_id, sequence
      FROM turn_events
      WHERE (
          event_type = 'thread.session-set'
          AND json_extract(payload_json, '$.session.status') = 'running'
          AND json_type(payload_json, '$.session.activeTurnId') = 'text'
        )
        OR (
          event_type = 'thread.message-sent'
          AND json_extract(payload_json, '$.role') = 'assistant'
          AND json_type(payload_json, '$.turnId') = 'text'
          AND json_type(payload_json, '$.streaming') = 'true'
        )
    ),
    session_ends AS (
      SELECT
        thread_id,
        sequence,
        occurred_at,
        payload_json,
        LAG(sequence, 1, 0) OVER (PARTITION BY thread_id ORDER BY sequence)
          AS previous_end_sequence
      FROM turn_events
      WHERE event_type = 'thread.session-set'
        AND json_extract(payload_json, '$.session.status')
          IN ('idle', 'ready', 'error', 'interrupted', 'stopped')
    ),
    -- Last activity is what the old projection_turns table recorded: turn
    -- requests, starts, assistant messages, interrupts, checkpoints, and a
    -- session reaching a final state while a turn was open, which is how the
    -- projector marked a turn finished. A turn opens with a running session
    -- or a streamed reply. 063 dropped that table, so read the V1 events it
    -- was projected from instead. A final state with no turn open since the
    -- last one, such as a restart stopping an idle session, is not activity.
    activity_timestamps AS (
      SELECT thread_id, created_at AS activity_at
      FROM projection_thread_messages
      WHERE role = 'user'
        AND thread_id IN (SELECT thread_id FROM swept_threads)
      UNION ALL
      SELECT
        thread_id,
        CASE
          WHEN event_type = 'thread.session-set' THEN occurred_at
          WHEN event_type = 'thread.turn-diff-completed'
            THEN json_extract(payload_json, '$.completedAt')
          WHEN event_type = 'thread.message-sent' AND json_type(payload_json, '$.streaming') = 'false'
            THEN json_extract(payload_json, '$.updatedAt')
          ELSE json_extract(payload_json, '$.createdAt')
        END
      FROM turn_events
      WHERE event_type IN ('thread.turn-start-requested', 'thread.turn-diff-completed')
        OR (
          event_type = 'thread.turn-interrupt-requested'
          AND json_type(payload_json, '$.turnId') = 'text'
        )
        OR (
          event_type = 'thread.message-sent'
          AND json_extract(payload_json, '$.role') = 'assistant'
          AND json_type(payload_json, '$.turnId') = 'text'
        )
        OR (
          event_type = 'thread.session-set'
          AND json_extract(payload_json, '$.session.status') = 'running'
          AND json_type(payload_json, '$.session.activeTurnId') = 'text'
        )
      UNION ALL
      SELECT
        session_end.thread_id,
        COALESCE(json_extract(session_end.payload_json, '$.session.updatedAt'), session_end.occurred_at)
      FROM session_ends AS session_end
      WHERE EXISTS (
        SELECT 1
        FROM turn_openings AS opening
        WHERE opening.thread_id = session_end.thread_id
          AND opening.sequence > session_end.previous_end_sequence
          AND opening.sequence < session_end.sequence
      )
    )
  `;

  yield* sql`
    WITH ${detectBurst}
    UPDATE projection_threads AS thread
    SET settled_at = (
      SELECT COALESCE(
        (
          SELECT activity.activity_at
          FROM activity_timestamps AS activity
          WHERE activity.thread_id = thread.thread_id
            AND julianday(activity.activity_at) IS NOT NULL
            AND julianday(activity.activity_at) <= julianday(automatic.occurred_at)
          ORDER BY julianday(activity.activity_at) DESC
          LIMIT 1
        ),
        thread.created_at
      )
      FROM automatic_settlements AS automatic
      WHERE automatic.thread_id = thread.thread_id
        AND automatic.occurred_at = thread.settled_at
      LIMIT 1
    )
    WHERE thread.settled_override = 'settled'
      AND EXISTS (
        SELECT 1
        FROM automatic_settlements AS automatic
        WHERE automatic.thread_id = thread.thread_id
          AND automatic.occurred_at = thread.settled_at
      )
  `;

  yield* sql`
    WITH ${detectBurst}
    UPDATE orchestration_v2_projection_threads AS thread
    SET payload_json = json_set(
      thread.payload_json,
      '$.settledAt',
      (
        SELECT COALESCE(
          (
            SELECT activity.activity_at
            FROM activity_timestamps AS activity
            WHERE activity.thread_id = thread.thread_id
              AND julianday(activity.activity_at) IS NOT NULL
              AND julianday(activity.activity_at) <= julianday(automatic.occurred_at)
            ORDER BY julianday(activity.activity_at) DESC
            LIMIT 1
          ),
          json_extract(thread.payload_json, '$.createdAt')
        )
        FROM automatic_settlements AS automatic
        WHERE automatic.thread_id = thread.thread_id
          AND automatic.occurred_at = json_extract(thread.payload_json, '$.settledAt')
        LIMIT 1
      )
    )
    WHERE json_extract(thread.payload_json, '$.settledOverride') = 'settled'
      AND EXISTS (
        SELECT 1
        FROM automatic_settlements AS automatic
        WHERE automatic.thread_id = thread.thread_id
          AND automatic.occurred_at = json_extract(thread.payload_json, '$.settledAt')
      )
  `;

  yield* sql`
    WITH ${detectBurst}
    UPDATE orchestration_events AS event
    SET payload_json = json_set(
      event.payload_json,
      '$.settledAt',
      (
        SELECT COALESCE(
          (
            SELECT activity.activity_at
            FROM activity_timestamps AS activity
            WHERE activity.thread_id = event.stream_id
              AND julianday(activity.activity_at) IS NOT NULL
              AND julianday(activity.activity_at) <= julianday(automatic.occurred_at)
            ORDER BY julianday(activity.activity_at) DESC
            LIMIT 1
          ),
          json_extract(event.payload_json, '$.createdAt'),
          json_extract(event.payload_json, '$.settledAt')
        )
        FROM automatic_settlements AS automatic
        WHERE automatic.thread_id = event.stream_id
          AND automatic.occurred_at = json_extract(event.payload_json, '$.settledAt')
        LIMIT 1
      )
    )
    WHERE event.aggregate_kind = 'thread'
      AND event.application_event_version = 2
      AND event.stream_id IN (SELECT thread_id FROM automatic_settlements)
      AND EXISTS (
        SELECT 1
        FROM automatic_settlements AS automatic
        WHERE automatic.thread_id = event.stream_id
          AND automatic.occurred_at = json_extract(event.payload_json, '$.settledAt')
      )
  `;
});
