@outcome_time_window_table_create

IF OBJECT_ID('@cohort_database_schema.@cohort_outcome_table', 'U') IS NOT NULL
	DROP TABLE @cohort_database_schema.@cohort_outcome_table;

CREATE TABLE @cohort_database_schema.@cohort_outcome_table (
  cohort_definition_id BIGINT, 
  outcome_cohort_definition_id BIGINT,
  window_id INT, 
	outcome_count INT
);

-- Get the censored cohorts that remain from the
-- main cohort table
SELECT DISTINCT cohort_definition_id
INTO #cohort_ref
FROM @cohort_database_schema.@cohort_table
;


/*
 * For {T, TwS, TwoS}, restrict the cohort start/end based on the
 * outcome windows specified. At this time only consider
 * the start date of the target cohort as the anchor point for evaluating
 * the outcome
 */
select 
	tsfeas.window_id, 
	ts.cohort_definition_id, 
	ts.subject_id, 
  DATEADD(dd, tsfeas.window_start, ts.cohort_start_date) cohort_outcome_window_start,
  CASE 
  	WHEN DATEADD(dd, tsfeas.window_end, ts.cohort_start_date) <= ts.cohort_end_date THEN DATEADD(dd, tsfeas.window_end, ts.cohort_start_date)
  	ELSE ts.cohort_end_date
  END cohort_outcome_window_end
INTO #ts_windowed
from (
	SELECT *
	FROM #cohort_ref, #outcome_windows
) tsfeas
INNER JOIN @cohort_database_schema.@cohort_table ts ON tsfeas.cohort_definition_id = ts.cohort_definition_id
;

/*
* Evaluate the O's intersecting with T, TwS, TwoS. 
*/

-- Get the sumamry of {T} with {O} in {windows}
INSERT INTO @cohort_database_schema.@cohort_outcome_table (
  cohort_definition_id, 
  outcome_cohort_definition_id,
  window_id, 
	outcome_count
)
SELECT 
	ts.cohort_definition_id, 
	o.cohort_definition_id outcome_cohort_definition_id, 
	w.window_id, 
	SUM(CASE WHEN ts.cohort_outcome_window_start <= o.cohort_start_date AND ts.cohort_outcome_window_end >= o.cohort_start_date THEN 1 ELSE 0 END) outcome_count
from #ts_windowed ts
inner join (
	SELECT *
	FROM @cohort_database_schema.@cohort_staging_table c
	WHERE cohort_definition_id IN (@outcome_ids)
) o ON o.subject_id = ts.subject_id
inner join #outcome_windows w ON w.window_id = ts.window_id
GROUP BY
	w.window_id, 
	w.window_start,
	w.window_end,
	ts.cohort_definition_id, 
	o.cohort_definition_id
;

TRUNCATE TABLE #cohort_ref;
DROP TABLE #cohort_ref;

TRUNCATE TABLE #ts_windowed;
DROP TABLE #ts_windowed;


@outcome_time_window_table_drop