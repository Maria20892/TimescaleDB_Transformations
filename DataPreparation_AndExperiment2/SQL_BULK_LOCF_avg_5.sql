DO $$
DECLARE
    end_time TIMESTAMP;
	exec_time INTERVAL;
	
	timeseries_name TEXT;  
	copied_ts_id_list TEXT[];
	
    start_time TIMESTAMP := clock_timestamp();
    start_timestamp TIMESTAMP := '2024-11-10 04:00:00+01'; --first timestamp in the timeseries that has to be transformed 
    end_timestamp TIMESTAMP := '2024-11-10 04:00:00.109+01'; --last timestamp in the timeseries that has to be transformed 
    transformation_type VARCHAR(30) := 'gap_filling'; -- Set the transformation type 
    aggregation_function VARCHAR(5) := 'avg'; -- Set the aggregation function 
    time_interval INTERVAL := interval '5 millisecond'; -- Set the time interval 

BEGIN

	SELECT ARRAY(SELECT id FROM Timeseries) INTO copied_ts_id_list;
	SELECT ARRAY(SELECT id FROM Timeseries WHERE id NOT LIKE '%_gap_fill_%') INTO copied_ts_id_list;

	FOREACH timeseries_name IN ARRAY copied_ts_id_list
	LOOP
		-- Inserting new timeseries with additional properties
		WITH new_timeseries AS (
			INSERT INTO Timeseries (id, original_timeseries_id, type, measure_id, source, additional_properties)
			SELECT 
			id || 'LOCF_gap_fill_5mlsec', -- Concatenate to the ID
				id,
				type,              
				measure_id,                    
				source,      
			--add info what was used - LOCF, NO, interpolation
				COALESCE(additional_properties, '{}'::jsonb) || format('{"transformation_type": "%s", "aggregation_function": "%s", "time-interval": "%s"}', transformation_type, aggregation_function, time_interval)::jsonb
			FROM 
				Timeseries
			WHERE 
				id = timeseries_name 
			RETURNING id
		),
		aggregated_data AS (
			SELECT 
				timeseries_id AS original_timeseries_id,
				time_bucket_gapfill(time_interval, timestamp, start_timestamp, end_timestamp) AS bucket,  
				LOCF(avg(value)) AS value
				
			FROM 
				Signal_Values_timestamp
			WHERE 
				timeseries_id = timeseries_name
			GROUP BY 
				original_timeseries_id, bucket
			ORDER BY 
				bucket ASC
		)
		INSERT INTO Signal_Values_timestamp (timeseries_id, timestamp, value)
		SELECT 
			nt.id,
			ad.bucket,
			ad.value
		FROM 
			new_timeseries nt, aggregated_data ad;	
		
	END LOOP;
	
	end_time := clock_timestamp(); -- Capture end time
	exec_time := end_time - start_time;
	RAISE NOTICE 'Execution time: %', exec_time;
	 -- Insert execution time into log table
	INSERT INTO query_execution_log (query_name, execution_time)
	VALUES ('BULK_LOCF_5', exec_time);
END $$;

SELECT * FROM query_execution_log ORDER BY executed_at DESC;
--DELETE FROM query_execution_log where id BETWEEN 324 AND 328;