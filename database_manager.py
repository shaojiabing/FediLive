import sqlite3

class DatabaseManager:
    def __init__(self, db_path="mastodon_instances.db"):
        self.connection = sqlite3.connect(db_path)

    def create_tables(self):
        with self.connection:
            # create instances table
            self.connection.execute("""
                CREATE TABLE IF NOT EXISTS instances (
                    name TEXT PRIMARY KEY,
                    statuses INTEGER,
                    loadtime TEXT,
                    processable BOOLEAN,
                    round INTEGER
                )
            """)
            # create livefeeds table
            self.connection.execute("""
                CREATE TABLE IF NOT EXISTS livefeeds (
                    sid TEXT PRIMARY KEY,
                    id TEXT,
                    created_at TEXT,
                    in_reply_to_id TEXT,
                    in_reply_to_account_id TEXT,
                    sensitive BOOLEAN,
                    spoiler_text TEXT,
                    visibility TEXT,
                    language TEXT,
                    uri TEXT,
                    url TEXT,
                    replies_count INTEGER,
                    reblogs_count INTEGER,
                    favourites_count INTEGER,
                    edited_at TEXT,
                    content TEXT,
                    reblog TEXT,
                    application TEXT,
                    account TEXT,
                    media_attachments TEXT,
                    mentions TEXT,
                    tags TEXT,
                    emojis TEXT,
                    card TEXT,
                    poll TEXT
                )
            """)
            # create error log table
            self.connection.execute("""
                CREATE TABLE IF NOT EXISTS error_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    loadtime TEXT,
                    data_name TEXT,
                    object_name TEXT,
                    content TEXT,
                    response_code TEXT,
                    error_message TEXT
                )
            """)

    def close(self):
        self.connection.close()
