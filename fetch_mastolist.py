import argparse
from database_manager import DatabaseManager
from instance_manager import InstanceManager
import requests

def fetch_instances(token, api_url="https://instances.social/api/1.0/instances/list", params={"count": 0}):
        """Fetch instance data from Mastodon API."""
        headers = {'Authorization': f'Bearer {token}'}
        response = requests.get(api_url, headers=headers, params=params)
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Error fetching data: {response.status_code} - {response.text}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch Mastodon instances and livefeeds.")
    parser.add_argument("--token", type=str, required=True, help="Your Mastodon API token")
    args = parser.parse_args()

    db_manager = DatabaseManager()
    db_manager.create_tables()

    instance_manager = InstanceManager(db_manager.connection)

    try:
        print("Fetching instance data...")
        data = fetch_instances(args.token)
        instances = data.get("instances", [])

        print(f"Inserting {len(instances)} instances into the database...")
        instance_manager.insert_instances(instances)

        print("Saving instance names to file...")
        instance_manager.save_names_to_file()

        print("All tasks completed successfully.")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        db_manager.close()