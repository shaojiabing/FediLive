import random
import requests
import time
import re
import argparse
from multiprocessing import Process
from utils import judge_api_islimit, judge_sleep_limit
from database_manager import DatabaseManager
from livefeeds_manager import LivefeedsManager

limit_dict = {}#[{} for i in token_num]
limit_set = set()#[set() for i in token_num]

def get_favourite_boost(livefeeds_manager, token, instance, status_id, retry_thresh=4):
    reblog_url = f"https://{instance}/api/v1/statuses/{status_id}/reblogged_by"
    favourite_url = f"https://{instance}/api/v1/statuses/{status_id}/favourited_by"
    HEADERS = {'Authorization': f'Bearer {token}'}
    
    reblogs = []
    last_page_flag = -1
    retry_time = 0

    while True:
        params = {
            'limit': 40, 
        }
        if last_page_flag != -1:
            params['max_id'] = last_page_flag
        try:
            response = requests.get(reblog_url, headers=HEADERS, params=params, timeout=5)
            if response.status_code == 200:
                judge_sleep_limit(response.headers,instance,limit_dict,limit_set)
                res_headers = {k.lower(): v for k, v in response.headers.items()}
                judge_sleep_limit(res_headers,instance,limit_dict,limit_set)
                reblogs.extend(response.json())
                if 'Link' not in response.headers or len(response.json()) < 40:
                    break
                match = re.search(r'max_id=(\d+)', response.headers['Link'])
                if match:
                    last_page_flag = match.group(1)
            elif response.status_code == 503 or response.status_code == 429:
                retry_time +=1
                time.sleep(random.random())
                print("429or503")
                if retry_time <= retry_thresh:
                    continue 
                else:
                    retry_time = 0
                    livefeeds_manager.insert_error_log("booster_favouriter",f"{instance}#{status_id}","429or503")
                    return False
            else:
                livefeeds_manager.insert_error_log("booster_favouriter",f"{instance}#{status_id}","Error",response_code=response.status_code)
                print(f"{instance}#{status_id}","Error fetching status boosters",response.status_code)
                return False
        except requests.exceptions.Timeout:
            retry_time +=1
            time.sleep(0.1)
            print("time out, retry...")
            if retry_time <= retry_thresh:
                continue  
            else:
                retry_time = 0
                livefeeds_manager.insert_error_log("booster_favouriter",f"{instance}#{status_id}","TimeOut")
                return False
        except Exception as e:
            livefeeds_manager.insert_error_log("booster_favouriter",f"{instance}#{status_id}","Error",error_message=e)
            print(f"{instance}#{status_id}",f"Failed to connect to {instance}#{status_id}, skipping this status", e)
            return False
    
    favourites = []
    last_page_flag = -1
    retry_time = 0
    while True:
        params = {
            'limit': 40,    
        }
        if last_page_flag != -1:
            params['max_id'] = last_page_flag
        try:
            response = requests.get(favourite_url, headers=HEADERS, params=params, timeout=5)
            if response.status_code == 200:
                judge_sleep_limit(response.headers,instance,limit_dict,limit_set)
                res_headers = {k.lower(): v for k, v in response.headers.items()}
                judge_sleep_limit(res_headers,instance,limit_dict,limit_set)
                favourites.extend(response.json())
                if 'Link' not in response.headers or len(response.json()) < 40:
                    livefeeds_manager.save_reblogfavourite(instance, status_id, reblogs, favourites)
                    return True
                match = re.search(r'max_id=(\d+)', response.headers['Link'])
                if match:
                    last_page_flag = match.group(1)
            elif response.status_code == 503 or response.status_code == 429:
                retry_time +=1
                time.sleep(random.random())
                print("429or503")
                if retry_time <= retry_thresh:
                    continue  # 继续重试
                else:
                    retry_time = 0
                    livefeeds_manager.insert_error_log("booster_favouriter",f"{instance}#{status_id}","429or503")
                    return False
            else:
                livefeeds_manager.insert_error_log("booster_favouriter",f"{instance}#{status_id}","Error",response_code=response.status_code)
                print(f"{instance}#{status_id}","Error fetching status boosters",response.status_code)
                return False
        except requests.exceptions.Timeout:
            retry_time +=1
            time.sleep(0.1)
            print("time out, retry...")
            if retry_time <= retry_thresh:
                continue  # 继续重试
            else:
                retry_time = 0
                livefeeds_manager.insert_error_log("booster_favouriter",f"{instance}#{status_id}","TimeOut")
                return False
        except Exception as e:
            livefeeds_manager.insert_error_log("booster_favouriter",f"{instance}#{status_id}","Error",error_message=e)
            print(f"{instance}#{status_id}",f"Failed to connect to {instance}#{status_id}, skipping this status", e)
            return False
    return True

def fetch_status_id(db_manager, retry_thresh=10):
    retry_time = 0

    while True:
        instance_query = ', '.join(f"'{item}'" for item in limit_set) if limit_set else None
        if instance_query:
            query = f"""
                SELECT sid, status, instance_name, id
                FROM livefeeds
                WHERE status = 'pending'
                AND (instance_name NOT IN ({instance_query}))
                LIMIT 5;
            """
        else:
            query = """
                SELECT sid, status, instance_name, id
                FROM livefeeds
                WHERE status = 'pending'
                LIMIT 5;
            """
        cursor = db_manager.connection.execute(query)
        candidates = cursor.fetchall()

        if not candidates and not limit_set:
            print("no candidates and no item in limit_set")
            return None, True

        for candidate in candidates:
            sid = candidate[0]
            update_query = """
                UPDATE livefeeds
                SET status = 'read'
                WHERE sid = ? AND status = 'pending'
                RETURNING sid
            """
            cursor = db_manager.connection.execute(update_query, (sid,))

            row = cursor.fetchone()
            if row:
                return candidate, False

        print(f"no match {retry_time}")
        time.sleep(2)
        retry_time += 1

        if retry_time >= retry_thresh:
            judge_api_islimit(limit_dict, limit_set)
            retry_time = 0

def process_task(token):
    db_manager = DatabaseManager()
    livefeeds_manager = LivefeedsManager(db_manager.connection)
    current_processing_status = None
    terminate_process = False
    while not terminate_process:
        try:
            judge_api_islimit(limit_dict, limit_set)
            info, terminate_process = fetch_status_id(db_manager)
            if info:
                current_processing_status = info
                get_favourite_boost(livefeeds_manager, token, current_processing_status[2], current_processing_status[3])
                print(f"successfully fetch{current_processing_status[0]}")
                current_processing_status = None
            else:
                if terminate_process:
                    return
                time.sleep(60)
        except Exception as e:
            print(f"error occur: {e}")
            if current_processing_status:
                update_query = """
                UPDATE livefeeds
                    SET status = 'pending'
                    WHERE sid = ?
                """
                db_manager.connection.execute(update_query, (current_processing_status[0],))
                current_processing_status = None
            time.sleep(5)
    db_manager.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch boosters and favouriters and save to JSON.")
    parser.add_argument('--processnum', type=int, default=1, help='processing num')
    parser.add_argument("--token", type=str, required=True, help="Your Mastodon API token")
    parser.add_argument("--output", type=str, default="reblog_favourite.json", help="Output file for livefeeds data.")
    args = parser.parse_args()
    db_manager = DatabaseManager()
    db_manager.create_tables()
    db_manager.check_status()
    db_manager.close()
    
    process_list = []
    for i in range(args.processnum):
        p = Process(target=process_task, args=(args.token,))
        p.start()
        process_list.append(p)
    for item in process_list:
        item.join()
    db_manager = DatabaseManager()
    livefeeds_manager = LivefeedsManager(db_manager.connection)
    livefeeds_manager.export_to_json(args.output, table="booster_favouriter")
    db_manager.close()

    print("All tasks completed successfully.")