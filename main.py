import argparse
from datetime import datetime, timezone
from multiprocessing import Process
from instance_manager import InstanceManager
from livefeeds_manager import LivefeedsManager
from utils import compute_round_time, judge_isin_duration, judge_sleep, transform_ISO2datetime, compute_current_duration
from database_manager import DatabaseManager
import json
import sqlite3
import requests
import re
import time
import random

def fetch_livefeeds(instance_name, instance_info, global_duration, token, livefeeds_manager, instance_manager, retry_thresh=4):
    """
    Fetch livefeeds data for a specific instance and save it into the database.
    """
    current_round = instance_info["round"]
    id_range = instance_info.get(f"round{current_round - 1}_id_range", {})
    livefeeds_url = f"https://{instance_name}/api/v1/timelines/public"
    headers = {"Authorization": f"Bearer {token}"}
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
            response = requests.get(livefeeds_url, headers=headers, params=params, timeout=5)
            if response.status_code == 200:
                res_headers = {k.lower(): v for k, v in response.headers.items()}
                judge_sleep(res_headers)

                data = response.json()
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
                            print(f"No status in this duration for {instance_name}")
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
                print("429or503 error")
                if retry_time > retry_thresh:
                    livefeeds_manager.insert_error_log("livefeeds", instance_name, "429or503", response_code=response.status_code)
                    instance_manager.update_instance(instance_name, processable=False, round=max_round)
                    return False
            else:
                livefeeds_manager.insert_error_log("livefeeds", instance_name, "Error", response_code=response.status_code)
                instance_manager.update_instance(instance_name, processable=False, round=max_round)
                return False
        except requests.exceptions.RequestException as e:
            retry_time += 1
            time.sleep(0.1)
            print("Timeout, retrying...")
            if retry_time > retry_thresh:
                livefeeds_manager.insert_error_log("livefeeds", instance_name, "Timeout")
                instance_manager.update_instance(instance_name, processable=False, round=max_round)
                return False
        except Exception as e:
            livefeeds_manager.insert_error_log("livefeeds", instance_name, "Unexpected Error", error_message=str(e))
            print(f"Exception fetching data for {instance_name}: {e}")
            instance_manager.update_instance(instance_name, processable=False, round=max_round)
            return False

def fetch_instance(db_manager, round_value):
    """
    Fetch an instance from the database based on the given round.
    If round < 0, it ignores round_id_range.
    """
    try:
        with db_manager.connection:
            if round_value < 0:
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
                cursor = db_manager.connection.execute(query, (round_value + 1, round_value))
            else:
                # If round >= 0, check round_id_range
                column_name = f"round{round_value}_id_range"
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
                cursor = db_manager.connection.execute(query, (round_value + 1, round_value))

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
                    f"round{round_value}_id_range": round_id_range,
                }
    except Exception as e:
        print(f"Unexpected error occurred: {e}")

    return None

def process_task(db_manager, livefeeds_manager, instance_manager, max_round, global_duration, token):
    """
    Task to fetch and process livefeeds.
    """
    for round_number in range(max_round + 1):
        for i in range(max_round + 1):
            while True:
                instance_info = fetch_instance(db_manager, i - 1)
                if instance_info:
                    print(f"Success find instance {instance_info['name']}")
                    fetch_livefeeds(instance_info['name'], instance_info, global_duration, token, livefeeds_manager, instance_manager)
                else:
                    print(f"No instance at round {i}")
                    break
            
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch livefeeds and save to JSON.")
    parser.add_argument('--processnum', type=int, default=1, help='processing num')
    parser.add_argument("--token", type=str, required=True, help="Your Mastodon API token")
    parser.add_argument("--start", type=str, required=True, help='Start time in "YYYY-MM-DD HH:MM:SS" format.')
    parser.add_argument("--end", type=str, default="now", help='End time in "YYYY-MM-DD HH:MM:SS" format (default: now).')
    parser.add_argument("--output", type=str, default="livefeeds.json", help="Output file for livefeeds data.")
    args = parser.parse_args()

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
        p = Process(target=process_task, args=(db_manager, livefeeds_manager, instance_manager, max_round, global_duration, args.token))
        p.start()
        process_list.append(p)

    # Wait for all processes to complete
    for p in process_list:
        p.join()

    # Export livefeeds to JSON
    livefeeds_manager.export_livefeeds_to_json(args.output)
    db_manager.close()

    print("All tasks completed successfully.")
