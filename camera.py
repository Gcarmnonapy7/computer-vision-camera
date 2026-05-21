import os
import time
import requests
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime

# =========================================================
# LOAD ENV VARIABLES
# =========================================================

load_dotenv()

API_KEY = os.getenv("MERAKI_API_KEY")
NETWORK_ID = os.getenv("NETWORK_ID")
CAMERA_SERIAL = os.getenv("CAMERA_SERIAL")

# =========================================================
# HEADERS
# =========================================================

HEADERS = {
    "X-Cisco-Meraki-API-Key": API_KEY,
    "Content-Type": "application/json"
}

# =========================================================
# GET LIVE CAMERA ANALYTICS
# =========================================================

def get_camera_analytics(serial):
    """
    Get live analytics from Meraki MV camera
    """

    url = f"https://api.meraki.com/api/v1/devices/{serial}/camera/analytics/live"

    response = requests.get(url, headers=HEADERS)

    if response.status_code != 200:
        print("Error:", response.status_code)
        print(response.text)
        return None

    return response.json()

# =========================================================
# EXTRACT PEOPLE DETECTION
# =========================================================

def extract_people_data(data):
    """
    Extract people count from analytics JSON
    """

    records = []

    timestamp = datetime.utcnow()

    zones = data.get("zones", {})

    for zone_name, zone_data in zones.items():

        people_count = zone_data.get("person", 0)

        record = {
            "timestamp": timestamp,
            "zone": zone_name,
            "people_count": people_count
        }

        records.append(record)

    return pd.DataFrame(records)

# =========================================================
# EXPORT CSV
# =========================================================

def export_csv(df):
    """
    Export data to CSV
    """

    filename = f"meraki_people_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

    df.to_csv(filename, index=False)

    print(f"CSV exported: {filename}")

# =========================================================
# MAIN
# =========================================================

if __name__ == "__main__":

    analytics = get_camera_analytics(CAMERA_SERIAL)

    if analytics:

        print("Raw Analytics:")
        print(analytics)

        df = extract_people_data(analytics)

        print("\nProcessed Data:")
        print(df)

        export_csv(df)
