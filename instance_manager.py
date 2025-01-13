from datetime import datetime
import sqlite3
import json

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
            print(f"SQLite error while updating {instance_name} current_round_id_range: {e}")
        except Exception as e:
            self.insert_error_log(
                data_name="round_idrange",
                object_name=instance_name,
                content="ERROR",
                error_message=str(e)
            )
            print(f"Error while updating {instance_name} current_round_id_range: {e}")

    def update_instance(self, name, **kwargs):
        columns = ", ".join([f"{key} = ?" for key in kwargs.keys()])
        values = list(kwargs.values()) + [name]
        with self.connection:
            self.connection.execute(f"""
                UPDATE instances SET {columns} WHERE name = ?
            """, values)
    
    def save_names_to_file(self, filename="instances_list.txt"):
        """Extract and save instance names to a text file."""
        with open(filename, 'w') as file:
            cursor = self.connection.execute("SELECT name FROM instances")
            names = cursor.fetchall()
            for name_tuple in names:
                file.write(name_tuple[0] + '\n')
