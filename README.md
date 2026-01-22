# Overgrad Connector
A pipeline that loads data from the Overgrad platform into Google Cloud Storage.

## Dependencies
- Python3
- Pipenv
- Docker
- Google BigQuery
- Google Cloud Storage
- MailGun

## Setup

Clone the repo from GitHub:

`git clone https://github.com/kippnorcal/overgrad-connector.git`

### .env file
Create a `.env` file at the projects root directory and add credentials for Mail Gun, Google Big Query, 
Google Cloud Storage, and the Overgrad API key.

```
# Mailgun & email notification variables
MG_DOMAIN=
MG_API_URL=
MG_API_KEY=
FROM_ADDRESS=
TO_ADDRESS=

# Google Cloud Credentials
GOOGLE_APPLICATION_CREDENTIALS=
GBQ_PROJECT=
GBQ_DATASET=
BUCKET=

OVERGRAD_API_KEY=
```

### Google Credentials
Put a copy of the Google credentials JSON file at the root of the repo. Add the filename to
the `GOOGLE_APPLICATION_CREDENTIALS` variable in the `.env` file.

## Build Docker Image

Run the following command from the repo's root dir:

`docker build -t overgrad-connector .`

## Running the Job

### Runtime Arguments

| Flags              | Actions                                                                                                                                                                                                     |
|--------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `--grad-year`      | REQUIRED - provide a year in a YYYY format; example 2026                                                                                                                                                    |
| `--recent-updates` | This is the default workflow and it's argument is not needed. It exists to make commands more explicit. This workflow fetches updated records since the most recent updated timestamp in the data warehouse |
| `--delete-records` | This worklow will compare all of the records in the Overgrad API with the records in the data warehouse; Any records in the data warehouse that is not in the API will be deleted.                          |
| `--updated-since`  | This workflow will look for updates from a specific date. Date must be entered in a YYYY-MM-DD format; example 2026-01-22                                                                                   |

### Example Run Commands

Running the job requires that the `--grad-year` argument is included.

```
docker run --rm -t overgrad-connector --grad-year 2026
```

Without any other arguments, the automation runs the `--recent-updates` workflow by default. Since we schedule many different runs of the overgrad-connector, the `--recent-updates` argument exists to make the command more explicit if desired. To run either of the other workflows, just use their flag instead:

```
docker run --rm -t overgrad-connector --grad-year 2026 --updated-since 2026-01-22
```

or

```
docker run --rm -t overgrad-connector --grad-year 2026 --delete-records
```

