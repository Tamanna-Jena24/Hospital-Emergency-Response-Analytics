import pandas as pd
# 1. Read raw CSV
df = pd.read_csv(r"C:\Users\taman\Desktop\Wproject\hospita_data.csv")
# Convert time columns to datetime
df['arrival_time'] = pd.to_datetime(df['arrival_time'])
df['treatment_start_time'] = pd.to_datetime(df['treatment_start_time'])
df['discharge_time'] = pd.to_datetime(df['discharge_time'])
# 2. Create Dimension Tables
# dim_patient
dim_patient = df[['patient_id']].drop_duplicates().reset_index(drop=True)
# dim_hospital
dim_hospital = df[['hospital_id']].drop_duplicates().reset_index(drop=True)
# dim_triage
dim_triage = df[['triage_level']].drop_duplicates().reset_index(drop=True)
dim_triage['triage_id'] = dim_triage.index + 1
# dim_condition
dim_condition = df[['condition']].drop_duplicates().reset_index(drop=True)
dim_condition['condition_id'] = dim_condition.index + 1
# dim_outcome
dim_outcome = df[['outcome']].drop_duplicates().reset_index(drop=True)
dim_outcome['outcome_id'] = dim_outcome.index + 1
# dim_time
dim_time = df[['arrival_time']].drop_duplicates().reset_index(drop=True)
dim_time['time_id'] = dim_time.index + 1
dim_time['date'] = dim_time['arrival_time'].dt.date
dim_time['day'] = dim_time['arrival_time'].dt.day_name()
dim_time['month'] = dim_time['arrival_time'].dt.month_name()
dim_time['year'] = dim_time['arrival_time'].dt.year
dim_time['hour'] = dim_time['arrival_time'].dt.hour
dim_time = dim_time[['time_id', 'date', 'day', 'month', 'year', 'hour', 'arrival_time']]
# 3. Create Fact Table
# Calculate metrics
df['wait_time_minutes'] = (df['treatment_start_time'] - df['arrival_time']).dt.total_seconds() / 60
df['treatment_time_minutes'] = (df['discharge_time'] - df['treatment_start_time']).dt.total_seconds() / 60
# Merge to replace text with IDs
fact = df.merge(dim_triage, on='triage_level') \
.merge(dim_condition, on='condition') \
.merge(dim_outcome, on='outcome') \
.merge(dim_time, on='arrival_time')
fact_table = fact[['patient_id', 'hospital_id', 'time_id',
'triage_id', 'condition_id', 'outcome_id',
'wait_time_minutes', 'treatment_time_minutes']]
# 4. Create ONE FINAL TABLE (join fact + dimensions)
final_star = fact_table \
.merge(dim_hospital, on='hospital_id') \
.merge(dim_patient, on='patient_id') \
.merge(dim_triage[['triage_id', 'triage_level']], on='triage_id') \
.merge(dim_condition[['condition_id', 'condition']], on='condition_id') \
.merge(dim_outcome[['outcome_id', 'outcome']], on='outcome_id') \
.merge(dim_time[['time_id', 'date', 'day', 'month', 'year', 'hour']], on='time_id')
# 5. Save ONE final CSV
final_star.to_csv("Hospital_d.csv", index=False)