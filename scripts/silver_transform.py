import json
import pandas as pd
from pathlib import Path

def run_silver_transform(**context):
    execution_date = context["ds_nodash"]

    bronze_file = context["ti"].xcom_pull(
        key="bronze_file",
        task_ids="bronze_ingest"
    )

    if not bronze_file:
        raise ValueError("No bronze file found in XCom")

    silver_path = Path("/opt/airflow/data/silver")
    silver_path.mkdir(parents=True, exist_ok=True)

    with open(bronze_file) as f:
        raw = json.load(f)

    df_raw = pd.DataFrame(raw["states"])

    df_raw.columns = [
        "icao24", "callsign", "origin_country", "time_position", "last_contact", "longitude",
        "latitude", "baro_altitude", "on_ground", 
        "velocity", "true_track", "vertical_rate",
        "sensors", "geo_altitude", "squawk",
        "spi", "position_source"
    ]

    df = df_raw[
        [
            "icao24",
            "origin_country",
            "velocity",
            "on_ground",
        ]
    ].copy()

    df = df.dropna(subset=["origin_country"])

    df["origin_country"] = df["origin_country"].astype(str).str.strip()
    df = df[df["origin_country"] != ""]

    df = df[df["origin_country"].str.match(r"^[A-Za-z][A-Za-z\s\-\'\.\,\(\)]+$", na=False)]

    df["velocity"] = pd.to_numeric(df["velocity"], errors="coerce")
    df = df.dropna(subset=["velocity"])

    df["on_ground"] = df["on_ground"].apply(
        lambda v: 1 if v is True or v == 1 else 0
    )

    output_file = silver_path / f"flights_silver_{execution_date}.csv"
    df.to_csv(output_file, index=False)

    context["ti"].xcom_push(
        key="silver_file",
        value=str(output_file)
    )