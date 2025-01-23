# fetcher/masto_list_fetcher.py
import requests
from pymongo import MongoClient, errors
from datetime import datetime
import logging
from utils import create_unique_index, save_error_log
from config import Config

logger = logging.getLogger(__name__)

def fetch_instances():
    """
    Fetches all Mastodon instances and stores their information in MongoDB.
    Also saves the list of instance names to a file.
    """
    config = Config()
    mongodb_uri = config.get_central_mongodb_uri()
    
    query = {
        "count": 0
    }
    
    token = config.api.get('central_token')
    if not token:
        logger.error("Central API token is not set in the configuration.")
        raise ValueError("Central API token is not set in the configuration.")
    
    headers = {'Authorization': f'Bearer {token}'}
    logger.info("Sending request to fetch instances list...")
    try:
        response = requests.get("https://instances.social/api/1.0/instances/list", headers=headers, params=query)
        if response.status_code != 200:
            save_error_log(None, "fetch_instances", "API", "Failed to fetch instances", res_code=response.status_code, error_message=response.text)
            logger.error(f"Failed to fetch instances: {response.status_code}")
            raise ConnectionError(f"Failed to fetch instances: {response.status_code}")
        
        data = response.json()
        current_time = datetime.now()
        
        client = MongoClient(mongodb_uri)
        db = client['mastodon']
        instances_collection = db["instances"]
        
        # Create unique index on 'name' field
        create_unique_index(instances_collection, 'name')
        
        # Prepare documents for insertion
        instances = data.get("instances", [])
        
        insert_num = 0
        for item in instances:
            item['loadtime'] = current_time
            item['processable'] = True
            item['round'] = -1
            item['statuses'] = int(item.get('statuses', 0))
            try:
                instances_collection.insert_one(item)
                insert_num += 1
            except errors.DuplicateKeyError:
                logger.warning(f"Duplicate instance found: {item['name']}, skipping.")
        
        logger.info(f"Inserted {insert_num} instances into MongoDB.")
        client.close()
        
        # Extract all names and save to file
        names = [item['name'] for item in instances]
        paths = config.get_paths()
        instances_list_path = paths.get('instances_list', 'instances_list.txt')
        with open(instances_list_path, 'w', encoding='utf-8') as name_file:
            for name in names:
                name_file.write(name + '\n')
        
        logger.info(f"All instance names have been saved to {instances_list_path}")
    except Exception as e:
        logger.exception(f"An error occurred while fetching instances: {e}")
        raise

if __name__ == "__main__":
    fetch_instances()
