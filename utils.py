from datetime import datetime, timezone, timedelta
import math
import time

def transform_ISO2datetime(time_str):
    """
    Convert ISO 8601 string to datetime with UTC timezone.
    """
    return datetime.strptime(time_str, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)

def compute_round_time(duration):
    """
    Compute the number of rounds based on the duration in hours.
    """
    time_diff = duration['end_time'] - duration['start_time']
    return math.ceil(time_diff.total_seconds() / 3600)

def judge_isin_duration(duration, current_time):
    """
    Check if the current_time is within the given duration.
    """
    return duration['start_time'] <= current_time <= duration['end_time']

def judge_sleep(res_headers):
    """
    Handle rate-limiting and sleep until the reset time.
    """
    res_headers = {k.lower(): v for k, v in res_headers.items()}
    if int(res_headers.get('x-ratelimit-remaining', 2)) <= 0:
        reset_time = res_headers.get('x-ratelimit-reset')
        if reset_time:
            if reset_time.endswith('Z'):
                reset_time = reset_time[:-1] + '+00:00'
            try:
                target_time = datetime.fromisoformat(reset_time.replace('T', ' ')).replace(tzinfo=timezone.utc)
                current_time = datetime.now(timezone.utc)
                sleep_time = (target_time - current_time).total_seconds()
                if sleep_time > 0:
                    print(f"Sleeping until {target_time} (for {sleep_time} seconds)...")
                    time.sleep(sleep_time)
                    return False
            except ValueError as e:
                print(f"Error parsing datetime string: {reset_time}. Error: {e}")
    return True

def compute_current_duration(global_duration, current_round, max_round):
    current_duration = {}
    if current_round < max_round:
        new_start_time = global_duration['end_time'] - timedelta(hours=current_round)
    else:
        new_start_time = global_duration['start_time']
    new_end_time = global_duration['end_time'] - timedelta(hours=current_round-1)
    current_duration['start_time'] = new_start_time
    current_duration['end_time'] = new_end_time
    return current_duration