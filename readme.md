## Installation

1. **Clone the Repository**

   ```bash
   git clone -b Single git@github.com:shaojiabing/FediLive.git
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

4. **Configure tokens and Logging**

   Edit the `config/config.yaml` file with your API tokens and logging preferences.

   ```yaml
   api:
     instance_token: "your_instance_api_token"
     livefeeds_token: "your_mastodon_api_token"
     email: "your_email@example.com"
   
   paths:
     instances_list: "instances_list.txt"
   
   logging:
     level: "INFO"
     file: "logs/app.log"
   ```

   - **Logging Configuration**:
     - `level`: Sets the logging level (e.g., DEBUG, INFO, WARNING, ERROR, CRITICAL).
     - `file`: Path to the log file where logs will be stored.


## Usage

### 1. Fetch Instance Information

Run this on the central node to fetch all Mastodon instances and store their information in MongoDB.

```bash
python -m fetcher.masto_list_fetcher
```

### 2. Fetch Tweets

You can run this on multiple machines in parallel.

```bash
python -m fetcher.livefeeds_worker --processnum 2 --start "2024-01-01 00:00:00" --end "2024-01-02 00:00:00"
```

Parameters:

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

