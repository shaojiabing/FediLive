# fetcher/masto_list_fetcher.py
import argparse
from .database_manager import DatabaseManager
from .instance_manager import InstanceManager
import requests
from .config import Config
import logging

logger = logging.getLogger(__name__)
config = Config()

def fetch_instances(db_manager):
        """Fetch instance data from Mastodon API."""
        query = {
            "count": 0
        }
        token = config.api.get('instance_token')
        if not token:
            logger.error("Instance API token is not set in the configuration.")
            raise ValueError("Instance API token is not set in the configuration.")
        headers = {'Authorization': f'Bearer {token}'}
        logger.info("Sending request to fetch instances list...")
        response = requests.get("https://instances.social/api/1.0/instances/list", headers=headers, params=query)
        if response.status_code == 200:
            return response.json()
        else:
            db_manager.insert_error_log("fetch_instances", "API", "Failed to fetch instances", response_code=response.status_code, error_message=response.text)
            logger.error(f"Failed to fetch instances: {response.status_code}")
            raise Exception(f"Error fetching data: {response.status_code} - {response.text}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch Mastodon instances and livefeeds.")
    args = parser.parse_args()

    db_manager = DatabaseManager()
    db_manager.create_tables()

    instance_manager = InstanceManager(db_manager.connection)

    try:
        data = fetch_instances(db_manager)
        instances = data.get("instances", [])

        instance_manager.insert_instances(instances)
        logger.info(f"Inserted {len(instances)} instances into the database...")

        paths = config.get_paths()
        instances_list_path = paths.get('instances_list', 'instances_list.txt')
        instance_manager.save_names_to_file(instances_list_path)
        logger.info(f"All instance names have been saved to {instances_list_path}")

    except Exception as e:
        logger.exception(f"An error occurred while fetching instances: {e}")
    finally:
        db_manager.close()