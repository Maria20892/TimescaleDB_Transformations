DO $$
DECLARE
    end_time TIMESTAMP;
	exec_time INTERVAL;

    start_time TIMESTAMP := clock_timestamp();
    start_timestamp TIMESTAMP := '2024-11-10 04:00:00+01'; --first timestamp in the timeseries that has to be transformed 
    end_timestamp TIMESTAMP := '2024-11-10 04:01:49.999+01'; --last timestamp in the timeseries that has to be transformed 
    original_id VARCHAR := 'Exp3P1V5happiness_x1000_with_gaps'; -- Set the original timeseries_id 
    transformation_type VARCHAR(30) := 'gap_filling'; -- Set the transformation type 
    aggregation_function VARCHAR(5) := 'avg'; -- Set the aggregation function 
    time_interval INTERVAL := interval '2 millisecond'; -- Set the time interval 
    interpolation BOOLEAN := TRUE; --can be true of false
    LOCF BOOLEAN := FALSE; --can be true of false
BEGIN
    -- Inserting new timeseries with additional properties
    WITH new_timeseries AS (
        INSERT INTO Timeseries (id, original_timeseries_id, type, measure_id, source, additional_properties)
        SELECT 
	    id || '__ITPL_gap_fill_2mlsec__', -- Concatenate to the ID
            id,
            type,              
            measure_id,                    
            source,      
		--add info what was used - LOCF, NO, interpolation
            COALESCE(additional_properties, '{}'::jsonb) || format('{"transformation_type": "%s", "aggregation_function": "%s", "time-interval": "%s", "LOCF": "%s", "Interpolation":"%s"}', transformation_type, aggregation_function, time_interval, LOCF, interpolation)::jsonb
		FROM 
            Timeseries
        WHERE 
            id = original_id
        RETURNING id
    ),
    aggregated_data AS (
        SELECT 
            timeseries_id AS original_timeseries_id,
            time_bucket_gapfill(time_interval, timestamp, start_timestamp, end_timestamp) AS bucket,  
        CASE
            WHEN interpolation THEN
                CASE
                    WHEN aggregation_function = 'max' THEN interpolate(max(value))
                    WHEN aggregation_function = 'min' THEN interpolate(min(value))
                    WHEN aggregation_function = 'avg' THEN interpolate(avg(value))
                    ELSE interpolate(avg(value)) -- Default to avg(value) if invalid value provided
                END
            WHEN LOCF THEN
                CASE
                    WHEN aggregation_function = 'max' THEN LOCF(max(value))
                    WHEN aggregation_function = 'min' THEN LOCF(min(value))
                    WHEN aggregation_function = 'avg' THEN LOCF(avg(value))
                    ELSE LOCF(avg(value)) -- Default to avg(value) if invalid value provided
                END
            ELSE
                CASE
                    WHEN aggregation_function = 'max' THEN max(value)
                    WHEN aggregation_function = 'min' THEN min(value)
                    WHEN aggregation_function = 'avg' THEN avg(value)
                    ELSE avg(value) -- Default to avg(value) if invalid value provided
                END
        END AS value
        FROM 
            Signal_Values_timestamp
        WHERE 
            timeseries_id = original_id
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

    end_time := clock_timestamp(); -- Capture end time
	exec_time := end_time - start_time;
    RAISE NOTICE 'Execution time: %', exec_time;
	 -- Insert execution time into log table
    INSERT INTO query_execution_log (query_name, execution_time)
    VALUES ('x1000_INTERPOLATION_2', exec_time);
END $$;
--select * from Signal_Values_timestamp 
--select * from Signal_Values_timestamp order by timestamp desc 
--select * from Timeseries;

-- DELETE FROM Signal_Values_timestamp;
-- DELETE from Timeseries;


SELECT * FROM query_execution_log ORDER BY id DESC;
-- DELETE FROM query_execution_log where query_name = 'x1000_INTERPOLATION_2';