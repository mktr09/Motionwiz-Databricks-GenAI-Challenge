import requests
import json

DATABRICKS_INSTANCE = 'https://dbc-c29e64b6-6d6d.cloud.databricks.com'

API_TOKEN = 'dapi5630d01e9f1588146c981f0c8c1213cc'

JOB_ID = '190035253520278'

# URL to trigger the job
url = f'{DATABRICKS_INSTANCE}/api/2.0/jobs/run-now'


headers = {
    'Authorization': f'Bearer {API_TOKEN}',
    'Content-Type': 'application/json'
}


payload = {
    "job_id": JOB_ID,
    "notebook_params": {
        "api_token": "dapi5630d01e9f1588146c981f0c8c1213cc",  # the actual API token
        "directory_path": "/Workspace/Users/marin.kutrolli@penske.com/Testing Iterative Approach for LLM Response",
        "max_tokens": "256"
    }
}

# Make the POST request to trigger the job
response = requests.post(url, headers=headers, json=payload)

# Check the response
if response.status_code == 200:
    print("Job started successfully!")
    print("Run ID:", response.json()['run_id'])
else:
    print(f"Failed to start job: {response.content}")
