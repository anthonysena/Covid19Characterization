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
	ts.cohort_definition_id, 
	o.cohort_definition_id outcome_cohort_definition_id, 
	w.window_id, 
	SUM(
	  CASE WHEN 
	    DATEADD(dd, o.window_start, ts.cohort_start_date) <= o.cohort_start_date 
	    AND DATEADD(dd, o.window_end, ts.cohort_start_date) >= o.cohort_start_date 
	    THEN 1 
	  ELSE 0 END
	) outcome_count
from @cohort_database_schema.@cohort_table ts
inner join (
	SELECT *
	FROM @cohort_database_schema.@cohort_staging_table c, #outcome_windows
	WHERE c.cohort_definition_id IN (@outcome_ids)
) o ON o.subject_id = ts.subject_id
GROUP BY
	ts.cohort_definition_id, 
	o.cohort_definition_id,
	o.window_id, 
	o.window_start,
	o.window_end
;

@outcome_time_window_table_drop