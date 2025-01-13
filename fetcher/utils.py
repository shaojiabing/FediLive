# fetcher/utils.py
import logging
from pymongo.errors import DuplicateKeyError
from datetime import datetime, timezone
import time
import math

logger = logging.getLogger(__name__)

def create_unique_index(collection, index_name):
    """
    Creates a unique index on a specified field in a MongoDB collection.
    
    Args:
        collection (pymongo.collection.Collection): The MongoDB collection.
        index_name (str): The field name to create a unique index on.
    """
    existing_indexes = collection.index_information()
    if index_name not in existing_indexes:
        try:
            collection.create_index([(index_name, 1)], unique=True)
            logger.info(f"Unique index on '{index_name}' created for collection '{collection.name}'.")
        except DuplicateKeyError:
            logger.error(f"Duplicate key error: An existing document violates the unique constraint on '{index_name}'.")
        except Exception as e:
            logger.error(f"Error creating index '{index_name}': {e}")
    else:
        logger.info(f"Unique index on '{index_name}' already exists for collection '{collection.name}'.")

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
        target_time_str = res_headers.get('x-ratelimit-reset')
        if target_time_str:
            if target_time_str.endswith('Z'):
                target_time_str = target_time_str[:-1] + '+00:00'
            try:
                target_time = datetime.fromisoformat(target_time_str.replace('T', ' ')).replace(tzinfo=timezone.utc)
                current_time = datetime.now(timezone.utc)
                sleep_time = (target_time - current_time).total_seconds()
                if sleep_time > 0:
                    logger.info(f"[{instance_name}] Rate limit reached. Sleeping until {target_time.isoformat()}")
                    time.sleep(sleep_time)
                    return False
            except ValueError as e:
                logger.error(f"Error parsing datetime string: {target_time_str}. Error: {e}")
    return True

def rename_key(d, old_key, new_key):
    """
    Renames a key in a dictionary if it exists.
    
    Args:
        d (dict): The dictionary.
        old_key (str): The old key name.
        new_key (str): The new key name.
    
    Returns:
        dict: The modified dictionary.
    """
    if old_key in d:
        d[new_key] = d.pop(old_key)
    return d

def save_error_log(collection, data_name, object_name, content, res_code='None', error_message='None'):
    """
    Saves an error log entry to the specified MongoDB collection.
    
    Args:
        collection (pymongo.collection.Collection): The MongoDB collection for error logs.
        data_name (str): Name of the data source.
        object_name (str): Name of the object involved.
        content (str): Content related to the error.
        res_code (str, optional): Response code. Defaults to 'None'.
        error_message (str, optional): Error message. Defaults to 'None'.
    """
    current_time = datetime.now()
    log_entry = {
        'loadtime': current_time,
        "data_name": data_name,
        "object": object_name,
        "content": content,
        "response_code": res_code,
        "error_message": error_message
    }
    try:
        collection.insert_one(log_entry)
        logger.info(f"Saved error log: {log_entry}")
    except Exception as e:
        logger.error(f"Failed to save error log: {e}")

def transform_ISO2datetime(time_str):
    """
    Converts an ISO 8601 formatted string to a datetime object.
    
    Args:
        time_str (str): ISO 8601 formatted time string.
    
    Returns:
        datetime: Corresponding datetime object.
    """
    return datetime.strptime(time_str, "%Y-%m-%dT%H:%M:%S.%fZ")

def transform_str2datetime(time_str):
    """
    Converts a formatted string to a datetime object.
    
    Args:
        time_str (str): Time string in the format 'YYYY-MM-DD HH:MM:SS.microseconds'.
    
    Returns:
        datetime: Corresponding datetime object.
    """
    return datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S.%f")

def compute_round_time(global_duration):
    """
    Computes the number of rounds based on the duration.
    
    Args:
        global_duration (dict): Dictionary containing 'start_time' and 'end_time'.
    
    Returns:
        int: Number of rounds.
    """
    time_diff = global_duration['end_time'] - global_duration['start_time']
    hours_diff = time_diff.total_seconds() / 3600
    return math.ceil(hours_diff)
