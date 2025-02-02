import argparse
from datetime import datetime, timezone, timedelta
from multiprocessing import Process
from instance_manager import InstanceManager
from livefeeds_manager import LivefeedsManager
from utils import compute_round_time, judge_isin_duration, judge_sleep, transform_ISO2datetime
from database_manager import DatabaseManager
import json
import sqlite3
import requests
import re
import time
import random
import logging
from config import Config

logger = logging.getLogger(__name__)
config = Config()

def compute_current_duration(global_duration, current_round, max_round):
    """
    Computes the current duration for the given round.
    
    Args:
        current_round (int): The current round number.
        global_duration (dict): Dictionary containing 'start_time' and 'end_time'.
        max_round (int): The maximum number of rounds.
    
    Returns:
        dict: Dictionary with 'start_time' and 'end_time' for the current round.
    """
    current_duration = {}
    if current_round < max_round:
        new_start_time = global_duration['end_time'] - timedelta(hours=current_round)
    else:
        new_start_time = global_duration['start_time']
    new_end_time = global_duration['end_time'] - timedelta(hours=current_round-1)
    current_duration['start_time'] = new_start_time
    current_duration['end_time'] = new_end_time
    return current_duration

def fetch_livefeeds(instance_name, instance_info, global_duration, token, db_manager, livefeeds_manager, instance_manager, retry_thresh=4):
    """
    Fetches livefeeds data for a specified instance and saves it to the database.

    Args:
        instance_name (str): The name of the instance to fetch livefeeds from.
        instance_info (dict): Information about the instance, including its current round and ID range.
        global_duration (dict): The global time duration to filter livefeeds. Should include:
            - "start_time" (datetime): Start of the duration.
            - "end_time" (datetime): End of the duration.
        token (str): Authorization token used for API authentication.
        livefeeds_manager: Manager object responsible for handling and saving livefeeds data.
        instance_manager: Manager object responsible for updating instance-related metadata and state.
        retry_thresh (int, optional): Maximum number of retries allowed for failed requests (default is 4).

    Returns:
        bool: True if livefeeds data is successfully fetched and processed; False otherwise.
    """
    current_round = instance_info["round"]
    id_range = instance_info.get(f"round{current_round - 1}_id_range", {})
    logger.info(f"Starting to fetch toots from {instance_name}")
    livefeeds_url = f"https://{instance_name}/api/v1/timelines/public"
    headers = {"Authorization": f"Bearer {token}", 'Email': config.api.get('email', '')}
    retry_time = 0
    last_page_flag = -1
    r_in_nowround = -1

    while True:
        r_in_nowround += 1
        params = {"local": True, "limit": 40}
        if last_page_flag != -1:
            params["max_id"] = last_page_flag
        elif current_round != 0:
            params["max_id"] = id_range["min"]

        try:
            logger.debug(f"Request parameters: {params}")
            response = requests.get(livefeeds_url, headers=headers, params=params, timeout=5)
            if response.status_code == 200:
                res_headers = {k.lower(): v for k, v in response.headers.items()}
                judge_sleep(res_headers, instance_name)
                data = response.json()
                logger.info(f"Successfully fetched {len(data)} toots.")

                if current_round == 0:
                    for item in data:
                        created_at = transform_ISO2datetime(item["created_at"])
                        if judge_isin_duration(global_duration, created_at):
                            id_range["max"] = item["id"]
                            id_range["min"] = item["id"]
                            livefeeds_manager.save_ugc(item, instance_name)
                            instance_manager.update_round_id_range(instance_name, current_round, id_range)
                            return True
                        if created_at < global_duration["start_time"]:
                            logger.info(f"{instance_name} has no toots in the specified duration.")
                            instance_manager.update_instance(instance_name, processable=False, round=max_round)
                            return True
                else:
                    current_duration = compute_current_duration(global_duration, current_round, max_round)
                    for item in data:
                        id_range = instance_info[f'round{current_round-1}_id_range']
                        created_at = transform_ISO2datetime(item["created_at"])
                        if judge_isin_duration(current_duration, created_at):
                            livefeeds_manager.save_ugc(item, instance_name)
                            if r_in_nowround == 0:
                                id_range["max"] = item["id"]
                            id_range["min"] = item["id"]
                        else:
                            instance_manager.update_round_id_range(instance_name, current_round, id_range)
                            return True

                if "link" not in res_headers or len(data) < 40:
                    return True

                match = re.search(r"max_id=(\d+)", res_headers.get("link", ""))
                if match:
                    last_page_flag = match.group(1)
            elif response.status_code in [429, 503]:
                retry_time += 1
                time.sleep(random.random())
                logger.warning("Encountered 429 or 503 error, retrying...")
                if retry_time > retry_thresh:
                    db_manager.insert_error_log("livefeeds", instance_name, "429or503", response_code=response.status_code)
                    instance_manager.update_instance(instance_name, processable=False, round=max_round)
                    return False
            else:
                logger.error(f"Error fetching tweets from {instance_name}: {response.status_code}")
                db_manager.insert_error_log("livefeeds", instance_name, "Error", response_code=response.status_code)
                instance_manager.update_instance(instance_name, processable=False, round=max_round)
                return False
        except requests.exceptions.RequestException as e:
            retry_time += 1
            time.sleep(0.1)
            logger.warning("Request timed out, retrying...")
            if retry_time > retry_thresh:
                db_manager.insert_error_log("livefeeds", instance_name, "Timeout")
                instance_manager.update_instance(instance_name, processable=False, round=max_round)
                return False
        except Exception as e:
            db_manager.insert_error_log("livefeeds", instance_name, "Unexpected Error", error_message=str(e))
            logger.exception(f"Exception while connecting to {instance_name}")
            instance_manager.update_instance(instance_name, processable=False, round=max_round)
            return False

def fetch_instance(db_manager, round_num):
    """
    Fetches an instance from the database based on the specified round number
    and updates its round value for further processing. If the round number 
    is negative, the round_id_range condition is ignored.

    Args:
        db_manager: An object that manages database connections and queries. 
                    It provides access to the `connection` attribute for executing SQL queries.
        round_num (int): The round number to filter instances. If `round_num` is less than 0,
                         the query ignores the `round_id_range` condition.

    Returns:
        dict or None: A dictionary containing instance details if an instance is found, otherwise `None`.
    """
    try:
        with db_manager.connection:
            if round_num < 0:
                # If round < 0, ignore round_id_range
                query = """
                    UPDATE instances
                    SET round = ? 
                    WHERE name = (
                        SELECT name 
                        FROM instances 
                        WHERE round = ? AND processable = 1 
                        ORDER BY statuses DESC 
                        LIMIT 1
                    )
                    RETURNING name, statuses, processable, round
                """
                cursor = db_manager.connection.execute(query, (round_num + 1, round_num))
            else:
                # If round >= 0, check round_id_range
                column_name = f"round{round_num}_id_range"
                query = f"""
                    UPDATE instances
                    SET round = ? 
                    WHERE name = (
                        SELECT name 
                        FROM instances 
                        WHERE round = ? AND processable = 1 
                        AND {column_name} IS NOT NULL
                        ORDER BY statuses DESC 
                        LIMIT 1
                    )
                    RETURNING name, statuses, processable, round, {column_name}
                """
                cursor = db_manager.connection.execute(query, (round_num + 1, round_num))

            # Fetch the result
            row = cursor.fetchone()
            if row:
                # Parse round_id_range if it exists
                round_id_range = json.loads(row[4]) if len(row) == 5 else {}
                return {
                    "name": row[0],
                    "statuses": row[1],
                    "processable": bool(row[2]),
                    "round": row[3],
                    f"round{round_num}_id_range": round_id_range,
                }
    except Exception as e:
        logger.exception(f"An error occurred while fetching instances: {e}")
    return None

def process_task(db_manager, livefeeds_manager, instance_manager, max_round, global_duration, token):
    """
    Processes tasks by fetching instances and their toots.

    Args:
        db_manager: Database manager object used to interact with the database.
        livefeeds_manager: Manager object responsible for handling livefeeds data.
        instance_manager: Manager object responsible for managing instances.
        max_round (int): The maximum number of rounds to iterate through.
        global_duration (int): Duration parameter for processing livefeeds.
        token (str): Authorization token used for API requests or authentication.
    """
    for i in range(max_round + 1):
        while True:
            instance_info = fetch_instance(db_manager, i - 1)
            if instance_info:
                logger.info(f"Found instance: {instance_info['name']}, starting processing.")
                fetch_livefeeds(instance_info['name'], instance_info, global_duration, token, db_manager, livefeeds_manager, instance_manager)
            else:
                logger.info(f"No more instances to process for round {i}.")
                break
            
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch livefeeds and save to JSON.")
    parser.add_argument('--processnum', type=int, default=1, help='processing num')
    parser.add_argument("--start", type=str, required=True, help='Start time in "YYYY-MM-DD HH:MM:SS" format.')
    parser.add_argument("--end", type=str, default="now", help='End time in "YYYY-MM-DD HH:MM:SS" format (default: now).')
    args = parser.parse_args()
    
    token = config.api.get('livefeeds_token')
    # Parse global duration
    global_duration = {
        "start_time": datetime.strptime(args.start, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc),
        "end_time": datetime.now(timezone.utc) if args.end == "now" else datetime.strptime(args.end, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc),
    }

    # Compute maximum rounds
    max_round = compute_round_time(global_duration)

    # Initialize database and managers
    db_manager = DatabaseManager()
    db_manager.create_tables()
    livefeeds_manager = LivefeedsManager(db_manager.connection)
    instance_manager = InstanceManager(db_manager.connection)

    # Start processes
    process_list = []
    for i in range(args.processnum):
        p = Process(target=process_task, args=(db_manager, livefeeds_manager, instance_manager, max_round, global_duration, token))
        p.start()
        process_list.append(p)

    # Wait for all processes to complete
    for p in process_list:
        p.join()

    # Export livefeeds to JSON
    paths = config.get_paths()
    output_file_path = paths.get('output_file', 'output.json')
    livefeeds_manager.export_to_json(output_file_path)
    db_manager.close()

    logger.info("All tasks completed successfully.")
