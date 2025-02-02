# fetcher/utils.py
from datetime import datetime, timezone, timedelta
import math
import time
import logging

logger = logging.getLogger(__name__)

def transform_ISO2datetime(time_str):
    """
    Converts an ISO 8601 formatted string to a datetime object.
    
    Args:
        time_str (str): ISO 8601 formatted time string.
    
    Returns:
        datetime: Corresponding datetime object.
    """
    return datetime.strptime(time_str, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)

def compute_round_time(duration):
    """
    Computes the number of rounds based on the duration.
    
    Args:
        global_duration (dict): Dictionary containing 'start_time' and 'end_time'.
    
    Returns:
        int: Number of rounds.
    """
    time_diff = duration['end_time'] - duration['start_time']
    return math.ceil(time_diff.total_seconds() / 3600)

def judge_isin_duration(duration, current_time):
    """
    Determines whether the given `current_time` falls within the specified `duration`.

    Args:
        duration (dict): A dictionary specifying the time range with the following keys:
        current_time (datetime): The time to be checked, represented as a `datetime` object.

    Returns:
        bool: True if `current_time` is within the duration (inclusive of start and end times). False otherwise.
    """
    return duration['start_time'] <= current_time <= duration['end_time']

def judge_sleep(res_headers, instance_name):
    """
    Handles rate limiting by checking response headers and sleeping if necessary.
    
    Args:
        res_headers (dict): Response headers from the API.
        instance_name (str): Name of the Mastodon instance.
    
    Returns:
        bool: False if slept, True otherwise.
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
                    logger.info(f"[{instance_name}] Rate limit reached. Sleeping until {target_time.isoformat()}")
                    time.sleep(sleep_time)
                    return False
            except ValueError as e:
                logger.error(f"Error parsing datetime string: {reset_time}. Error: {e}")
    return True

def judge_api_islimit(limit_dict,limit_set):
    current_time = datetime.now(timezone.utc)
    keys_deleted = []
    for key,value in limit_dict.items():
        target_time = datetime.fromisoformat(value.replace('T', ' ')).replace(tzinfo=timezone.utc)
        if target_time <= current_time:
            limit_set.discard(key)
            keys_deleted.append(key)
    for item in keys_deleted:
        del limit_dict[item]
        
def judge_sleep_limit_table(res_headers,instance_name,limit_dict,limit_set):
    res_headers = {k.lower(): v for k, v in res_headers.items()}
    if int(res_headers.get('x-ratelimit-remaining', 2)) <= 0:
        target_time_str = res_headers.get('x-ratelimit-reset')
        if target_time_str is not None:
            if target_time_str.endswith('Z'):
                target_time_str = target_time_str[:-1] + '+00:00'
            try:
                target_time = datetime.fromisoformat(target_time_str.replace('T', ' ')).replace(tzinfo=timezone.utc)
                current_time = datetime.now(timezone.utc)  # usd UTC timezone
                sleep_time = (target_time - current_time).total_seconds()
                if sleep_time > 0:
                    print(current_time,'sleep till',target_time)
                    time.sleep(sleep_time)
                    return False
            except ValueError as e:
                logger.error(f"Error parsing datetime string: {target_time_str}. Error: {e}")

    if int(res_headers.get('x-ratelimit-remaining', 2)) <= 100:
        target_time_str = res_headers.get('x-ratelimit-reset')
        if target_time_str is not None:
            if target_time_str.endswith('Z'):
                target_time_str = target_time_str[:-1] + '+00:00'
            try:
                target_time = datetime.fromisoformat(target_time_str.replace('T', ' ')).replace(tzinfo=timezone.utc)
                current_time = datetime.now(timezone.utc) 
                if target_time>current_time:
                    limit_dict[instance_name] = target_time.isoformat()
                    limit_set.add(instance_name)
                    logger.info(f"take {instance_name} into limit dict")
                    return True
            except ValueError as e:
                logger.error(f"Error parsing datetime string: {target_time_str}. Error: {e}")