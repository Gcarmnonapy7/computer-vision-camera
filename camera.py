# pip install requests python-dotenv azure-identity notebookutils
# pip install pandas pyarrow  # For efficient data handling

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dotenv import load_dotenv
import os
from typing import Optional, Dict, List
import logging
import pandas as pd
from datetime import datetime, timedelta
import json

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

class MerakiCameraClient:
    """Robust Meraki Camera API client with retry logic and error handling."""
    
    BASE_URL = "https://api.meraki.com/api/v1"
    
    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.getenv("MERAKI_API_KEY")
        if not self.api_key:
            raise ValueError("API key required: set MERAKI_API_KEY or pass to constructor")
        
        self.session = self._create_session()
    
    def _create_session(self) -> requests.Session:
        """Create session with automatic retries on network errors."""
        session = requests.Session()
        
        # Retry on connection errors, timeouts, and 5xx errors
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,  # Wait 1s, 2s, 4s between retries
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"]
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        
        session.headers.update({
            "X-Cisco-Meraki-API-Key": self.api_key,
            "Content-Type": "application/json"
        })
        
        return session
    
    def _make_request(self, method: str, endpoint: str, **kwargs) -> Dict:
        """Make API request with error handling."""
        url = f"{self.BASE_URL}/{endpoint.lstrip('/')}"
        
        try:
            response = self.session.request(method, url, **kwargs)
            response.raise_for_status()
            return response.json()
        
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error: {e.response.status_code} - {e.response.text}")
            raise
        except requests.exceptions.ConnectionError:
            logger.error(f"Connection error for {url}")
            raise
        except requests.exceptions.Timeout:
            logger.error(f"Request timeout for {url}")
            raise
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {str(e)}")
            raise
    
    def list_cameras(self, network_id: str) -> List[Dict]:
        """Get all MV cameras in a network."""
        devices = self._make_request("GET", f"networks/{network_id}/devices")
        
        cameras = [
            device for device in devices 
            if device.get("model", "").startswith("MV")
        ]
        
        logger.info(f"Found {len(cameras)} cameras in network {network_id}")
        return cameras
    
    def get_snapshot(self, serial: str, timestamp: Optional[str] = None) -> str:
        """
        Generate camera snapshot.
        
        Args:
            serial: Camera serial number
            timestamp: Optional ISO 8601 timestamp for historical snapshot
        
        Returns:
            URL to snapshot image
        """
        endpoint = f"devices/{serial}/camera/generateSnapshot"
        
        payload = {}
        if timestamp:
            payload["timestamp"] = timestamp
        
        result = self._make_request("POST", endpoint, json=payload)
        
        snapshot_url = result.get("url")
        if not snapshot_url:
            raise ValueError("No snapshot URL in response")
        
        logger.info(f"Generated snapshot for {serial}: {snapshot_url}")
        return snapshot_url
    
    def get_analytics(self, serial: str) -> Dict:
        """Get live camera analytics (people/vehicle counts)."""
        endpoint = f"devices/{serial}/camera/analytics/live"
        analytics = self._make_request("GET", endpoint)
        
        logger.info(f"Analytics for {serial}: {analytics}")
        return analytics
    
    def get_recent_motion_events(
        self, 
        serial: str, 
        t0: Optional[str] = None,
        t1: Optional[str] = None
    ) -> List[Dict]:
        """
        Get recent motion events.
        
        Args:
            serial: Camera serial number
            t0: Start time (ISO 8601 or seconds since epoch)
            t1: End time
        """
        endpoint = f"devices/{serial}/camera/analytics/recent"
        
        params = {}
        if t0:
            params["t0"] = t0
        if t1:
            params["t1"] = t1
        
        events = self._make_request("GET", endpoint, params=params)
        return events
    
    def get_camera_analytics_dataframe(
        self, 
        network_id: str,
        hours_back: int = 24
    ) -> pd.DataFrame:
        """
        Get analytics from all cameras as a pandas DataFrame ready for Fabric.
        
        Args:
            network_id: Meraki network ID
            hours_back: How many hours of historical data to fetch
        
        Returns:
            DataFrame with camera analytics
        """
        cameras = self.list_cameras(network_id)
        
        all_data = []
        
        for camera in cameras:
            serial = camera["serial"]
            camera_name = camera.get("name", "Unknown")
            model = camera.get("model", "Unknown")
            
            try:
                # Get live analytics
                analytics = self.get_analytics(serial)
                
                # Flatten the analytics data
                record = {
                    "timestamp": datetime.utcnow().isoformat(),
                    "camera_serial": serial,
                    "camera_name": camera_name,
                    "camera_model": model,
                    "network_id": network_id
                }
                
                # Extract zone data
                if "zones" in analytics:
                    for zone_name, zone_data in analytics["zones"].items():
                        record[f"zone_{zone_name}_person_count"] = zone_data.get("person", 0)
                        record[f"zone_{zone_name}_vehicle_count"] = zone_data.get("vehicle", 0)
                
                all_data.append(record)
                
            except Exception as e:
                logger.error(f"Failed to get analytics for {serial}: {e}")
        
        df = pd.DataFrame(all_data)
        logger.info(f"Created DataFrame with {len(df)} records")
        
        return df


class FabricDataExporter:
    """Export Meraki camera data to Microsoft Fabric Lakehouse."""
    
    def __init__(
        self,
        lakehouse_name: str,
        workspace_id: Optional[str] = None
    ):
        """
        Initialize Fabric exporter.
        
        Args:
            lakehouse_name: Name of your Fabric Lakehouse
            workspace_id: Optional workspace ID (defaults to current)
        """
        self.lakehouse_name = lakehouse_name
        self.workspace_id = workspace_id
        
        # Import Fabric utilities (only available in Fabric environment)
        try:
            from notebookutils import mssparkutils
            self.mssparkutils = mssparkutils
        except ImportError:
            logger.warning("notebookutils not available - running outside Fabric")
            self.mssparkutils = None
    
    def write_to_lakehouse_table(
        self,
        df: pd.DataFrame,
        table_name: str,
        mode: str = "append"
    ):
        """
        Write DataFrame to Fabric Lakehouse as Delta table.
        
        Args:
            df: Pandas DataFrame to write
            table_name: Name of the table in Lakehouse
            mode: Write mode - "append", "overwrite", or "merge"
        """
        if self.mssparkutils is None:
            logger.error("Cannot write to Lakehouse - not in Fabric environment")
            # Fallback: save locally
            df.to_parquet(f"{table_name}.parquet")
            logger.info(f"Saved to local file: {table_name}.parquet")
            return
        
        try:
            # Convert pandas to Spark DataFrame
            from pyspark.sql import SparkSession
            spark = SparkSession.builder.getOrCreate()
            
            spark_df = spark.createDataFrame(df)
            
            # Write to Delta table
            table_path = f"Tables/{table_name}"
            
            spark_df.write \
                .format("delta") \
                .mode(mode) \
                .option("mergeSchema", "true") \
                .save(table_path)
            
            logger.info(f"Written {len(df)} rows to {table_path}")
            
        except Exception as e:
            logger.error(f"Failed to write to Lakehouse: {e}")
            raise
    
    def write_to_lakehouse_files(
        self,
        df: pd.DataFrame,
        file_path: str,
        format: str = "parquet"
    ):
        """
        Write DataFrame to Lakehouse Files section.
        
        Args:
            df: Pandas DataFrame to write
            file_path: Path in Files section (e.g., "camera_data/2024/01/data.parquet")
            format: File format - "parquet", "csv", "json"
        """
        if self.mssparkutils is None:
            logger.error("Cannot write to Lakehouse - not in Fabric environment")
            return
        
        try:
            full_path = f"Files/{file_path}"
            
            if format == "parquet":
                df.to_parquet(full_path, index=False)
            elif format == "csv":
                df.to_csv(full_path, index=False)
            elif format == "json":
                df.to_json(full_path, orient="records", lines=True)
            else:
                raise ValueError(f"Unsupported format: {format}")
            
            logger.info(f"Written to {full_path}")
            
        except Exception as e:
            logger.error(f"Failed to write to Files: {e}")
            raise
    
    def append_streaming_data(
        self,
        df: pd.DataFrame,
        table_name: str = "camera_analytics_stream"
    ):
        """
        Append real-time camera data with partitioning.
        
        Creates partitions by date for efficient querying.
        """
        # Add partition columns
        df["ingestion_date"] = pd.to_datetime(df["timestamp"]).dt.date
        df["ingestion_hour"] = pd.to_datetime(df["timestamp"]).dt.hour
        
        self.write_to_lakehouse_table(df, table_name, mode="append")


# Example usage for Fabric Notebook
def main_fabric_pipeline():
    """
    Main pipeline to run in Microsoft Fabric Notebook.
    
    Schedule this to run every 15 minutes for real-time monitoring.
    """
    
    # Initialize clients
    meraki_client = MerakiCameraClient()
    fabric_exporter = FabricDataExporter(lakehouse_name="CameraAnalytics")
    
    # Your Meraki network ID
    network_id = os.getenv("MERAKI_NETWORK_ID")
    
    # Get camera analytics
    logger.info("Fetching camera analytics...")
    df = meraki_client.get_camera_analytics_dataframe(network_id)
    
    # Export to Fabric Lakehouse
    logger.info("Exporting to Fabric Lakehouse...")
    fabric_exporter.append_streaming_data(df, table_name="camera_analytics")
    
    logger.info("Pipeline completed successfully!")
    
    return df


if __name__ == "__main__":
    df = main_fabric_pipeline()
    print(df.head())


# # Cell 1: Install dependencies
# %pip install requests python-dotenv urllib3

# # Cell 2: Set environment variables (or use Key Vault)
# import os
# os.environ["MERAKI_API_KEY"] = "your-api-key-here"
# os.environ["MERAKI_NETWORK_ID"] = "your-network-id-here"

# # Cell 3: Run the pipeline
# from your_module import main_fabric_pipeline

# df = main_fabric_pipeline()
# display(df)

# import requests
# from azure.identity import DefaultAzureCredential
# import json

# class FabricRESTExporter:
#     """Export data using Fabric REST API."""
    
#     def __init__(self, workspace_id: str, lakehouse_id: str):
#         self.workspace_id = workspace_id
#         self.lakehouse_id = lakehouse_id
#         self.base_url = "https://api.fabric.microsoft.com/v1"
        
#         # Get Azure token
#         credential = DefaultAzureCredential()
#         token = credential.get_token("https://analysis.windows.net/powerbi/api/.default")
#         self.token = token.token
    
#     def upload_file(self, df: pd.DataFrame, file_path: str):
#         """Upload DataFrame as file to Lakehouse."""
        
#         # Convert to bytes
#         buffer = df.to_parquet(index=False)
        
#         url = f"{self.base_url}/workspaces/{self.workspace_id}/lakehouses/{self.lakehouse_id}/files/{file_path}"
        
#         headers = {
#             "Authorization": f"Bearer {self.token}",
#             "Content-Type": "application/octet-stream"
#         }
        
#         response = requests.put(url, headers=headers, data=buffer)
#         response.raise_for_status()
        
#         logger.info(f"Uploaded {file_path} to Fabric")
