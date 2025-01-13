# fetcher/livefeeds_worker.py
import requests
import time
import argparse
from datetime import datetime, timedelta
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from multiprocessing import Process
import random
import re
import logging
from .utils import (
    create_unique_index, judge_sleep, save_error_log,
    transform_ISO2datetime, transform_str2datetime, compute_round_time
)
from .config import Config

logger = logging.getLogger(__name__)

def compute_current_duration(current_round, global_duration, max_round):
    """
    Computes the current duration for the given round.
    
    Args:
        current_round (int): The current round number.
        global_duration (dict): Dictionary containing 'start_time' and 'end_time'.
        max_round (int): The maximum number of rounds.
    
    Returns:
        dict: Dictionary with 'start_time' and 'end_time' for the current round.
    """
    if current_round < max_round:
        new_start_time = global_duration['end_time'] - timedelta(hours=current_round)
    else:
        new_start_time = global_duration['start_time']
    new_end_time = global_duration['end_time'] - timedelta(hours=current_round - 1)
    return {'start_time': new_start_time, 'end_time': new_end_time}

def fetch_instance(round_num, instances_collection, max_round):
    """
    Fetches an instance from the MongoDB collection based on the round number.
    
    Args:
        round_num (int): The current round number.
        instances_collection (pymongo.collection.Collection): The instances collection.
        max_round (int): The maximum number of rounds.
    
    Returns:
        dict or None: The instance information or None if not found.
    """
    if round_num < 0:
        query = {
            "round": round_num,
            "processable": True
        }
    else:
        query = {
            "round": round_num,
            "processable": True,
            f"round{round_num}_id_range": {"$exists": True}
        }
    return instances_collection.find_one_and_update(
        query,
        {"$set": {"round": round_num + 1}},
        return_document=True,
        sort=[("statuses", -1)]
    )

def fetch_livefeeds(instance_info, config, local_collections, tokens, worker_id, global_duration, max_round):
    """
    Fetches livefeeds (tweets) from a specific Mastodon instance.
    
    Args:
        instance_info (dict): Information about the instance.
        config (Config): Configuration object.
        local_collections (dict): Local MongoDB collections.
        tokens (list): List of API tokens.
        worker_id (int): ID of the worker.
        global_duration (dict): Dictionary containing 'start_time' and 'end_time'.
        max_round (int): The maximum number of rounds.
    """
    instance_name = instance_info['name']
    current_round = instance_info['round']
    logger.info(f"Starting to fetch tweets from {instance_name}")
    livefeeds_url = f"https://{instance_name}/api/v1/timelines/public"
    last_page_flag = -1
    retry_time = 0
    id_range = {}
    if current_round != 0:
        id_range = instance_info.get(f'round{current_round-1}_id_range', {})
    r_in_nowround = -1
    token = tokens[worker_id % len(tokens)]
    headers = {'Authorization': f'Bearer {token}', 'Email': config.api.get('email', '')}
    
    while True:
        r_in_nowround += 1
        params = {
            "local": True,
            "limit": 40
        }
        if last_page_flag != -1:
            params['max_id'] = last_page_flag
        elif current_round != 0:
            params['max_id'] = id_range.get('min')
        
        try:
            logger.debug(f"Request parameters: {params}")
            response = requests.get(livefeeds_url, headers=headers, params=params, timeout=5)
            if response.status_code == 200:
                res_headers = {k.lower(): v for k, v in response.headers.items()}
                judge_sleep(res_headers, instance_name)
                data = response.json()
                logger.info(f"Successfully fetched {len(data)} tweets.")
                
                if current_round == 0:
                    for item in data:
                        created_at = transform_ISO2datetime(item['created_at'])
                        if global_duration['start_time'] <= created_at <= global_duration['end_time']:
                            id_range['max'] = item['id']
                            id_range['min'] = item['id']
                            item['instance_name'] = instance_name
                            item['sid'] = f"{instance_name}#{item['id']}"
                            item['loadtime'] = datetime.now()
                            item['processable'] = True
                            try:
                                local_collections['livefeeds'].insert_one(item)
                                logger.info(f"Saved a tweet from {instance_name}.")
                            except DuplicateKeyError:
                                logger.warning("Duplicate tweet found, skipping.")
                            except Exception as e:
                                logger.error(f"Error saving tweet: {e}")
                        elif created_at < global_duration['start_time']:
                            logger.info(f"{instance_name} has no tweets in the specified duration.")
                            local_collections['instances'].update_one(
                                {"name": instance_name},
                                {"$set": {"round": max_round, "processable": False}}
                            )
                            return
                else:
                    current_duration = compute_current_duration(current_round, global_duration, max_round)
                    for item in data:
                        created_at = transform_ISO2datetime(item['created_at'])
                        if current_duration['start_time'] <= created_at <= current_duration['end_time']:
                            item['instance_name'] = instance_name
                            item['sid'] = f"{instance_name}#{item['id']}"
                            item['loadtime'] = datetime.now()
                            item['processable'] = True
                            try:
                                local_collections['livefeeds'].insert_one(item)
                                logger.info(f"Saved a tweet from {instance_name}.")
                            except DuplicateKeyError:
                                logger.warning("Duplicate tweet found, skipping.")
                            except Exception as e:
                                logger.error(f"Error saving tweet: {e}")
                        else:
                            local_collections['instances'].update_one(
                                {"name": instance_name},
                                {"$set": {"round": max_round, "processable": False}}
                            )
                            return
                
                if 'link' not in res_headers or len(data) < 40:
                    return
                match = re.search(r'max_id=(\d+)', res_headers.get('link', ''))
                if match:
                    last_page_flag = match.group(1)
            elif response.status_code in [503, 429]:
                retry_time += 1
                time.sleep(random.random())
                logger.warning("Encountered 429 or 503 error, retrying...")
                if retry_time > 4:
                    local_collections['instances'].update_one(
                        {"name": instance_name},
                        {"$set": {"processable": False, "round": max_round}}
                    )
                    return
            else:
                logger.error(f"Error fetching tweets from {instance_name}: {response.status_code}")
                local_collections['instances'].update_one(
                    {"name": instance_name},
                    {"$set": {"round": max_round, "processable": False}}
                )
                return
        except requests.exceptions.Timeout:
            retry_time += 1
            time.sleep(0.1)
            logger.warning("Request timed out, retrying...")
            if retry_time > 4:
                local_collections['instances'].update_one(
                    {"name": instance_name},
                    {"$set": {"processable": False, "round": max_round}}
                )
                return
        except Exception as e:
            logger.exception(f"Exception while connecting to {instance_name}: {e}")
            local_collections['instances'].update_one(
                {"name": instance_name},
                {"$set": {"round": max_round, "processable": False}}
            )
            return

def process_task(worker_id, config, local_collections, tokens, global_duration, max_round):
    """
    Processes tasks by fetching instances and their tweets.
    
    Args:
        worker_id (int): The ID of the worker.
        config (Config): Configuration object.
        local_collections (dict): Local MongoDB collections.
        tokens (list): List of API tokens.
        global_duration (dict): Dictionary containing 'start_time' and 'end_time'.
        max_round (int): The maximum number of rounds.
    """
    for round_num in range(max_round + 1):
        while True:
            instance_info = fetch_instance(round_num - 1, local_collections['instances'], max_round)
            if instance_info:
                logger.info(f"Found instance: {instance_info['name']}, starting processing.")
                fetch_livefeeds(instance_info, config, local_collections, tokens, worker_id, global_duration, max_round)
            else:
                logger.info(f"No more instances to process for round {round_num}.")
                break

def main():
    """
    Main function to parse arguments and start worker processes.
    """
    parser = argparse.ArgumentParser(description='Mastodon Livefeeds Worker')
    parser.add_argument('--id', type=int, required=True, help='Worker ID')
    parser.add_argument('--processnum', type=int, default=1, help='Number of parallel processes')
    parser.add_argument('--start', type=str, required=True, help='Start time (YYYY-MM-DD HH:MM:SS)')
    parser.add_argument('--end', type=str, required=True, help='End time (YYYY-MM-DD HH:MM:SS)')
    args = parser.parse_args()
    
    config = Config()
    central_mongodb_uri = config.get_central_mongodb_uri()
    client = MongoClient(central_mongodb_uri)
    db = client['mastodon']
    instances_collection = db['instances']
    
    local_mongodb_uri = config.get_local_mongodb_uri()
    local_client = MongoClient(local_mongodb_uri)
    local_db = local_client['mastodon']
    local_livefeeds_collection = local_db['livefeeds']
    local_error_collection = local_db['error_log']
    
    create_unique_index(local_livefeeds_collection, 'sid')
    
    with open(config.paths.get('token_list', 'tokens/token_list.txt'), 'r', encoding='utf-8') as f:
        tokens = f.read().splitlines()
    
    global_duration = {
        'start_time': transform_str2datetime(args.start),
        'end_time': transform_str2datetime(args.end)
    }
    
    max_round = compute_round_time(global_duration)
    logger.info(f"Maximum rounds: {max_round}")
    
    local_collections = {
        'livefeeds': local_livefeeds_collection,
        'error_log': local_error_collection,
        'instances': instances_collection
    }
    
    process_list = []
    for i in range(args.processnum):
        p = Process(target=process_task, args=(args.id + i, config, local_collections, tokens, global_duration, max_round))
        p.start()
        process_list.append(p)
    
    for p in process_list:
        p.join()
    
    client.close()
    local_client.close()
    logger.info("Livefeeds Worker task completed.")

if __name__ == "__main__":
    main()
