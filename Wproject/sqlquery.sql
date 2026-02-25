select * from hospital_final
--Average Treatment Delay per Hospital
WITH delay_cte AS (
    SELECT 
        hospital_id,
        DATEDIFF(MINUTE, arrival_time, treatment_start_time) AS delay_minutes
    FROM hospital_final
)
SELECT 
    hospital_id,
    AVG(delay_minutes) AS avg_delay_minutes
FROM delay_cte
GROUP BY hospital_id;

--Daily Emergency Count
WITH daily_cases AS (
    SELECT 
        CAST(arrival_time AS DATE) AS visit_date,
        COUNT(*) AS total_cases
    FROM hospital_final
    GROUP BY CAST(arrival_time AS DATE)
)
SELECT *
FROM daily_cases
ORDER BY visit_date;

--Rank Hospitals by Emergency Load
SELECT 
    hospital_id,
    COUNT(*) AS total_cases,
    RANK() OVER (ORDER BY COUNT(*) DESC) AS hospital_rank
FROM hospital_final
GROUP BY hospital_id;
--Running Total of Emergencies by Date
SELECT 
    CAST(arrival_time AS DATE) AS visit_date,
    COUNT(*) AS daily_cases,
    SUM(COUNT(*)) OVER (ORDER BY CAST(arrival_time AS DATE)) AS running_total
FROM hospital_final
GROUP BY CAST(arrival_time AS DATE)
ORDER BY visit_date;

--Compare Patient Delay with Hospital Average
SELECT 
    patient_id,
    hospital_id,
    DATEDIFF(MINUTE, arrival_time, treatment_start_time) AS delay_minutes,
    AVG(DATEDIFF(MINUTE, arrival_time, treatment_start_time)) 
        OVER (PARTITION BY hospital_id) AS hospital_avg_delay
FROM hospital_final;

--Top 2 Longest Waiting Patients per Hospital
WITH ranked_patients AS (
    SELECT 
        patient_id,
        hospital_id,
        DATEDIFF(MINUTE, arrival_time, treatment_start_time) AS delay_minutes,
        DENSE_RANK() OVER (
            PARTITION BY hospital_id 
            ORDER BY DATEDIFF(MINUTE, arrival_time, treatment_start_time) DESC
        ) AS rnk
    FROM hospital_final
)
SELECT *
FROM ranked_patients
WHERE rnk <= 2;
