import pandas as pd
import os

# ==========================================
# CONFIGURATION
# ==========================================

INPUT_FILE = "hospital_data.csv"
OUTPUT_FOLDER = "output"
OUTPUT_FILE = "hospital_final.csv"


# ==========================================
# EXTRACT FUNCTION
# ==========================================

def extract(file_path):
    print(" Extract Phase Started...")
    df = pd.read_csv(r"C:\Users\taman\Desktop\Wproject\hospita_data.csv")
    print("Data Extracted Successfully!")
    print("Columns Found:", df.columns.tolist())
    
    return df


# ==========================================
# TRANSFORM FUNCTION
# ==========================================

def transform(df):
    print("\n🔹 Transform Phase Started...")

    # Clean duplicates
    df = df.drop_duplicates()

    # Convert arrival_time to datetime
    if "arrival_time" in df.columns:
        df["arrival_time"] = pd.to_datetime(df["arrival_time"])

    # --------------------------------------
    # Create Dimension Tables
    # --------------------------------------

    # Patient Dimension
    dim_patient = df[["patient_id"]].drop_duplicates().reset_index(drop=True)
    dim_patient["patient_key"] = dim_patient.index + 1

    # Hospital Dimension
    dim_hospital = df[["hospital_id"]].drop_duplicates().reset_index(drop=True)
    dim_hospital["hospital_key"] = dim_hospital.index + 1

    # Triage Dimension
    dim_triage = df[["triage_level"]].drop_duplicates().reset_index(drop=True)
    dim_triage["triage_key"] = dim_triage.index + 1

    # Time Dimension
    dim_time = df[["arrival_time"]].drop_duplicates().reset_index(drop=True)
    dim_time["time_key"] = dim_time.index + 1
    dim_time["date"] = dim_time["arrival_time"].dt.date
    dim_time["day"] = dim_time["arrival_time"].dt.day
    dim_time["month"] = dim_time["arrival_time"].dt.month
    dim_time["year"] = dim_time["arrival_time"].dt.year
    dim_time["hour"] = dim_time["arrival_time"].dt.hour

    # --------------------------------------
    # Merge (Flatten Star Schema)
    # --------------------------------------

    final_df = df.merge(dim_patient, on="patient_id")
    final_df = final_df.merge(dim_hospital, on="hospital_id")
    final_df = final_df.merge(dim_triage, on="triage_level")
    final_df = final_df.merge(dim_time, on="arrival_time")

    # Add Emergency ID
    final_df["emergency_id"] = final_df.index + 1001

    print("Transform Completed Successfully!")

    return final_df


# ==========================================
# LOAD FUNCTION
# ==========================================

def load(df, output_folder, output_file):
    print("\n Load Phase Started...")

    # Create output folder if not exists
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    output_path = os.path.join(output_folder, output_file)

    df.to_csv(output_path, index=False)

    print("Data Loaded Successfully!")
    print("Saved at:", output_path)


# ==========================================
# MAIN PIPELINE CONTROLLER
# ==========================================

def run_pipeline():
    print(" ETL Pipeline Started...\n")

    # Extract
    data = extract(INPUT_FILE)

    # Transform
    transformed_data = transform(data)

    # Load
    load(transformed_data, OUTPUT_FOLDER, OUTPUT_FILE)

    print("\n ETL Pipeline Completed Successfully!")


# ==========================================
# RUN PIPELINE
# ==========================================

if __name__ == "__main__":
    run_pipeline()