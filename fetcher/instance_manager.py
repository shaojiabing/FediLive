# fetcher/instance_manager.py
import sqlite3
import json
from datetime import datetime, timezone
import logging                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    

logger = logging.getLogger(__name__)

class InstanceManager:
    def __init__(self, db_connection):
        self.connection = db_connection

    def insert_instances(self, instances):
        """Insert or update instances into the database."""
        current_time = datetime.now().isoformat()
        with self.connection:
            insert_query = """
                INSERT OR IGNORE INTO instances (name, statuses, loadtime, processable, round)
                VALUES (?, ?, ?, ?, ?)
            """
            for instance in instances:
                self.connection.execute(insert_query, (
                    instance['name'],
                    int(instance.get('statuses', 0)),
                    current_time,
                    True,  # Default value for 'processable'
                    -1     # Default value for 'round'
                ))
    
    def update_round_id_range(self, instance_name, current_round, current_round_id_range):
        """
        Updates the `round_id_range` column for a specific round in the `instances` table.
        If the column for the current round does not exist, it creates the column dynamically.

        Args:
            instance_name (str): The name of the instance to update.
            current_round (int): The round number to which the `current_round_id_range` belongs.
            current_round_id_range (dict): A dictionary containing the ID range for the current round.
        """
        try:
            column_name = f"round{current_round}_id_range"
            
            cursor = self.connection.cursor()
            cursor.execute("PRAGMA table_info(instances)")
            columns = [row[1] for row in cursor.fetchall()]
            
            if column_name not in columns:
                with self.connection:
                    self.connection.execute(f"""
                        ALTER TABLE instances ADD COLUMN {column_name} TEXT
                    """)
            
            with self.connection:
                self.connection.execute(f"""
                    UPDATE instances
                    SET {column_name} = ?
                    WHERE name = ?
                """, (json.dumps(current_round_id_range), instance_name))
            
        except sqlite3.Error as e:
            self.insert_error_log(
                data_name="round_idrange",
                object_name=instance_name,
                content="ERROR",
                error_message=str(e)
            )
            logger.error(f"SQLite error while updating {instance_name} current_round_id_range: {e}")
        except Exception as e:
            self.insert_error_log(
                data_name="round_idrange",
                object_name=instance_name,
                content="ERROR",
                error_message=str(e)
            )
            logger.error(f"Error while updating {instance_name} current_round_id_range: {e}")

    def update_instance(self, name, **kwargs):
        columns = ", ".join([f"{key} = ?" for key in kwargs.keys()])
        values = list(kwargs.values()) + [name]
        with self.connection:
            self.connection.execute(f"""
                UPDATE instances SET {columns} WHERE name = ?
            """, values)
    
    def save_names_to_file(self, filename):
        """Extract and save instance names to a text file."""
        with open(filename, 'w') as file:
            cursor = self.connection.execute("SELECT name FROM instances")
            names = cursor.fetchall()
            for name_tuple in names:
                file.write(name_tuple[0] + '\n')

    def insert_error_log(self, data_name, object_name, content, response_code=None, error_message=None):
        """
        Inserts an error log entry into the `error_log` table in the database.

        Args:
            data_name (str): The name of the data or process that caused the error.
            object_name (str): The name of the object associated with the error.
            content (str): A brief description or type of the error.
            response_code (int, optional): The HTTP response code, if applicable.
            error_message (str, optional): A detailed error message describing the exception or issue.
        """
        try:
            current_time = datetime.now(timezone.utc).isoformat()
            with self.connection:
                self.connection.execute("""
                    INSERT INTO error_log (loadtime, data_name, object_name, content, response_code, error_message)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (current_time, data_name, object_name, content, response_code, error_message))
                logger.info(f"Saved error log: {(current_time, data_name, object_name, content, response_code, error_message)}")
        except Exception as e:
            logger.error(f"Failed to save error log: {e}")