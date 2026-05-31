# Conversion Data Analyzer

A Python utility for importing web analytics data into a PostgreSQL database and computing conversion rate metrics.

---

## Overview

This script loads a CSV file containing session and event data into a PostgreSQL table, then performs conversion rate analysis — overall and by landing page — and exports a sorted copy of the data.

---

## Prerequisites

- Python 3.x
- PostgreSQL database with the table `public.conversion_data` already created
- Required Python package:
  ```
  pip install psycopg2
  ```

---

## Project Structure

```
project/
├── main.py                          # Main script
├── config.conf                      # Database connection config
├── input_file/
│   └── conversion_data.csv          # Input CSV with session/event data
└── output_files/
    └── conversion_data_sorted.csv   # Generated sorted output
```

---

## Configuration

Create a `config.conf` file in the project root with the following structure:

```ini
[RESOURCE]
host     = your_db_host
user     = your_db_user
password = your_db_password
port     = your_db_port
database = your_db_name
```

---

## Input Data

The input CSV (`input_file/conversion_data.csv`) is expected to have at least these columns:

| Column       | Description                              |
|--------------|------------------------------------------|
| `session_id` | Unique identifier for a user session     |
| `page_id`    | Identifier for the page visited          |
| `event_type` | Type of event (e.g., `conversion`)       |

> **Note:** The table `public.conversion_data` must exist in the database before running the script.

---

## Features

### 1. Export Data to PostgreSQL
Loads the input CSV into the `public.conversion_data` table using PostgreSQL's `COPY` command for efficient bulk insertion.

### 2. Task A — Overall Conversion Rate
Calculates the overall conversion rate across all sessions:

```
Overall Conversion Rate = Total Conversion Events / Unique Session Count
```

### 3. Task B — Conversion Rate by Landing Page
Calculates the conversion rate for users whose session started on a specific page (`4903628644844587131`):

```
Page Conversion Rate = Conversions on Page / Unique Users on Page
```

### 4. Task C — Sorted Data Export
Queries all records sorted by `page_id` and `session_id`, then writes them to `output_files/conversion_data_sorted.csv`.

---

## Usage

```bash
python main.py
```

The script will sequentially:
1. Import the CSV into the database
2. Print the overall conversion rate
3. Print the conversion rate for the specified page
4. Generate the sorted output CSV

---

## Notes

- The page ID `4903628644844587131` in Task B is hardcoded. Update `cal_conversion_by_page()` to accept it as a parameter for more flexible use.
- Database connections are opened per function. Consider refactoring to share a single connection for efficiency.
- The output file path uses a Windows-style backslash (`output_files\...`). Update to `os.path.join()` for cross-platform compatibility.

---

© 2020 Conversion Data Analyzer · Apr 6, 2020
