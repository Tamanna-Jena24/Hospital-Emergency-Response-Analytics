
import snowflake.connector
import pandas as pd
import json


# Connect to Snowflake

conn = snowflake.connector.connect(
    user='YOUR_USERNAME',
    password='YOUR_PASSWORD',
    account='YOUR_ACCOUNT',          # e.g., abc123.us-east-1
    warehouse='HOSP_WAREHOUSE',
    database='HOSP_DB',
    schema='HOSP_SCHEMA'
)

cs = conn.cursor()
print("Connected to Snowflake ")


#  Read CSV Locally

df = pd.read_csv("hospital_final.csv")

# Optional: Convert some columns to JSON (semi-structured)
df['condition_json'] = df.apply(
    lambda row: json.dumps({"condition": row['condition'], "triage": row['triage_level']}),
    axis=1
)

print("CSV Loaded with JSON Column")


#  Create Table in Snowflake


cs.execute("""
CREATE OR REPLACE TABLE HOSP_EMERGENCY (
    patient_id STRING,
    hospital_id STRING,
    arrival_time TIMESTAMP,
    triage_level STRING,
    condition STRING,
    condition_json VARIANT
)
CLUSTER BY (arrival_time, triage_level)
""")
print("Table Created with Clustering")


# Insert Data into Snowflake


for index, row in df.iterrows():
    cs.execute(
        "INSERT INTO HOSP_EMERGENCY (patient_id,hospital_id,arrival_time,triage_level,condition,condition_json) VALUES (%s,%s,%s,%s,%s,PARSE_JSON(%s))",
        (row['patient_id'], row['hospital_id'], row['arrival_time'], row['triage_level'], row['condition'], row['condition_json'])
    )

print("Data Inserted into Snowflake")


#  Demonstrate Time Travel


# Select last 5 rows (current state)
cs.execute("SELECT * FROM HOSP_EMERGENCY ORDER BY arrival_time DESC LIMIT 5")
print("Current Data:")
for r in cs.fetchall():
    print(r)

# Suppose we delete a row
cs.execute("DELETE FROM HOSP_EMERGENCY WHERE patient_id='P005'")
print("Deleted P005")

# Query using Time Travel (5 minutes ago)
cs.execute("SELECT * FROM HOSP_EMERGENCY AT (OFFSET => -5) WHERE patient_id='P005'")
print("Time Travel Query Result for P005:")
for r in cs.fetchall():
    print(r)


# Close Connection

cs.close()
conn.close()
print("Snowflake Connection Closed")
