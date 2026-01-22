# Overgrad Connector
A pipeline that loads data from the Overgrad platform into Google Cloud Storage.

## Dependencies
- Python3
- Pipenv
- Docker
- Google Cloud Storage
- Mail Gun

## Setup

Clone the repo from Github:

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

