@outcome_time_window_table_create

IF OBJECT_ID('@cohort_database_schema.@cohort_outcome_table', 'U') IS NOT NULL
	DROP TABLE @cohort_database_schema.@cohort_outcome_table;

CREATE TABLE @cohort_database_schema.@cohort_outcome_table (
  cohort_definition_id BIGINT, 
  outcome_cohort_definition_id BIGINT,
  window_id INT, 
	outcome_count INT
);

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
  a.cohort_definition_id,
  a.outcome_cohort_definition_id,
  a.window_id,
  COUNT(DISTINCT a.subject_id) outcome_count
FROM (
  SELECT DISTINCT
  	ts.cohort_definition_id, 
  	ts.subject_id,
  	o.cohort_definition_id outcome_cohort_definition_id, 
  	w.window_id
  from @cohort_database_schema.@cohort_table ts
  inner join (
  	SELECT *
  	FROM @cohort_database_schema.@cohort_staging_table c, #outcome_windows
  	WHERE c.cohort_definition_id IN (@outcome_ids)
  ) o ON o.subject_id = ts.subject_id
  AND DATEADD(dd, o.window_start, ts.cohort_start_date) <= o.cohort_start_date 
  AND DATEADD(dd, o.window_end, ts.cohort_start_date) >= o.cohort_start_date 
) a
;

@outcome_time_window_table_drop