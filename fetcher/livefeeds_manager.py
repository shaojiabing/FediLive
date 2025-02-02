import json
from datetime import datetime, timezone
import logging 

logger = logging.getLogger(__name__)

class LivefeedsManager:
    def __init__(self, db_connection):
        self.connection = db_connection

    def save_ugc(self, ugc, instance_name):
        """
        Saves a single User-Generated Content (UGC) item into the `livefeeds` table in the database.

        Args:
            ugc (dict): A dictionary containing the UGC data, typically retrieved from a livefeed API response.
            instance_name (str): The name of the instance from which the UGC was retrieved.
        """
        sid = f"{instance_name}#{ugc['id']}"
        try:
            if ugc["card"] != None:
                ugc["card"] = json.dumps(ugc["card"])
            with self.connection:
                self.connection.execute("""
                    INSERT INTO livefeeds (sid, id, created_at, in_reply_to_id, in_reply_to_account_id,
                                        sensitive, spoiler_text, visibility, language, uri, url, replies_count, reblogs_count, 
                                        favourites_count, edited_at, content, reblog, application, account, media_attachments, 
                                        mentions, tags, emojis, card, poll)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    sid, ugc["id"], ugc["created_at"], ugc["in_reply_to_id"],
                    ugc["in_reply_to_account_id"], str(ugc["sensitive"]), ugc["spoiler_text"], ugc["visibility"],
                    ugc["language"], ugc["uri"], ugc["url"], ugc["replies_count"], ugc["reblogs_count"],
                    ugc["favourites_count"], ugc["edited_at"], ugc["content"], ugc["reblog"], json.dumps(ugc.get("application", {})),
                    json.dumps(ugc["account"]), json.dumps(ugc["media_attachments"]), json.dumps(ugc["mentions"]), json.dumps(ugc["tags"]),
                    json.dumps(ugc["emojis"]), ugc["card"], ugc["poll"]
                ))
                logger.info(f"Saved a toot from {instance_name}.")
        except Exception as e:
            logger.error(f"Error saving toot: {e}")
            
    def export_to_json(self, output_file, table="livefeeds"):
        """
        Exports all data from the specified database table to a JSON file.

        Args:
            output_file (str): The path to the JSON file where the data will be exported.
            table (str, optional): The name of the database table to export data from.
        """
        try:
            cursor = self.connection.cursor()
            cursor.execute(f"SELECT * FROM {table}")
            rows = cursor.fetchall()
            column_names = [description[0] for description in cursor.description]
            data = [dict(zip(column_names, row)) for row in rows]

            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=4)
            logger.info(f"Successfully saved livefeeds to file: {output_file}")
        except Exception as e:
            logger.error(f"Error exporting livefeeds: {e}")
    
    def save_reblogfavourite(self, instance, status_id, reblogs, favourites):
        sid = f"{instance}#{status_id}"
        try:
            with self.connection:
                self.connection.execute("""
                    INSERT INTO booster_favouriter (sid, boosts, favourites)
                    VALUES (?, ?, ?)
                """, (sid, json.dumps(reblogs), json.dumps(favourites)))
            logger.info(f"Successfully saved reblogs and favourites for {sid}.")
        except Exception as e:
            logger.error(f"Error saving reblogs/favourites for {sid}: {e}")   
