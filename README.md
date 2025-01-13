
## Installation

1. **Clone the Repository**

    ```bash
    git clone -b Multi git@github.com:shaojiabing/FediLive.git
    cd FediLive
    ```

2. **Create and Activate a Virtual Environment (Optional)**

    ```bash
    python3 -m venv venv
    source venv/bin/activate
    ```

3. **Install Dependencies**

    ```bash
    pip install -r requirements.txt
    ```

4. **Configure MongoDB and Logging**

    Ensure you have MongoDB installed and running. Edit the `config/config.yaml` file with your MongoDB connection details, API tokens, and logging preferences.

    ```yaml
    mongodb_central:
      username: "central_admin"
      password: "CentralPassword123!"
      host: "central.mongodb.server.com"
      port: 27017

    mongodb_local:
      username: "local_admin"
      password: "LocalPassword456!"
      host: "local.mongodb.server.com"
      port: 27018

    api:
      central_token: "your_central_api_token"
      email: "your_email@example.com"

    paths:
      instances_list: "instances_list.txt"
      token_list: "tokens/token_list.txt"

    logging:
      level: "INFO"
      file: "logs/app.log"
    ```

    - **Logging Configuration**:
      - `level`: Sets the logging level (e.g., DEBUG, INFO, WARNING, ERROR, CRITICAL).
      - `file`: Path to the log file where logs will be stored.

5. **Add API Tokens**

    Populate the `tokens/token_list.txt` file with your API tokens, one per line. Ensure the number of tokens exceeds the number of parallel processes you intend to run.

    ```
    Pp_iUR2ayLGfF06o_af-_AKNW9GXPiwv8SobKSUDZ3c
    I0SqxChmiDhSnUbEhXdAHnBS-XMfC0AuGNd2DoC7_Uw
    56TDjS3vpVrHpkLz76VjNGh71942TpfSG2v0T3mssoQ
    ...
    ```

## Usage

### 1. Fetch Instance Information

Run this on the central node to fetch all Mastodon instances and store their information in MongoDB.

```bash
python -m fetcher.masto_list_fetcher
```

### 2. Fetch Tweets
You can run this on multiple machines in parallel.
```bash
python -m fetcher.livefeeds_worker --id 0 --processnum 2 --start "2024-01-01 00:00:00" --end "2024-01-02 00:00:00"
```
Parameters:

--id: Worker ID (starting from 0), used to select different API tokens.  
--processnum: Number of parallel processes.  
--start: Start time for fetching tweets (format: YYYY-MM-DD HH:MM:SS).  
--end: End time for fetching tweets (format: YYYY-MM-DD HH:MM:SS).  

### 3. Fetch Reblogs and Favourites

```bash
python -m fetcher.reblog_favourite --processnum 3
```
Parameters:

--processnum: Number of parallel processes.  

## Logging
All operations and errors are logged to the file specified in the config/config.yaml under the logging section. By default, logs are saved to logs/app.log. You can adjust the logging level and log file path as needed.

Example configuration:
```bash
logging:
  level: "INFO"
  file: "logs/app.log"
```
Logging Levels:
DEBUG: Detailed information, typically of interest only when diagnosing problems.  
INFO: Confirmation that things are working as expected.
WARNING: An indication that something unexpected happened, or indicative of some problem in the near future.  
ERROR: Due to a more serious problem, the software has not been able to perform some function.  
CRITICAL: A very serious error, indicating that the program itself may be unable to continue running.  