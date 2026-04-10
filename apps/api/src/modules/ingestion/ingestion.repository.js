// src/modules/ingestion/ingestion.repository.js
import { pool } from '../../config/database.js';

export async function findPipelineHistory() {
  const result = await pool.query(`
    SELECT
      DATE(created_at)                                   AS run_date,
      MIN(created_at)                                    AS started_at,
      MAX(completed_at)                                  AS finished_at,
      COUNT(*)::integer                                  AS total_processes,
      COUNT(*) FILTER (WHERE status = 'COMPLETED')::integer AS completed,
      COUNT(*) FILTER (WHERE status = 'FAILED')::integer    AS failed,
      SUM(records_processed)::integer                    AS total_records,
      ROUND(SUM(execution_time_seconds)::numeric, 1)    AS total_seconds
    FROM biq_config.etl_process_windows
    WHERE created_at >= NOW() - INTERVAL '7 days'
    GROUP BY DATE(created_at)
    ORDER BY DATE(created_at) DESC
    LIMIT 14
  `);
  return result.rows;
}
