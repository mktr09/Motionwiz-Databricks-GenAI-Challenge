from flask import Flask, render_template, request, redirect, url_for
import requests
import pandas as pd
from sqlalchemy import create_engine


app = Flask(__name__)

# Databricks instance URL (without the trailing slash)
DATABRICKS_INSTANCE = 'your-databricks-instance-here'

# Databricks Job ID
JOB_ID = '190035253520278'


@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        # Get form data
        api_token = request.form['api_token']
        directory_path = request.form['directory_path']
        max_tokens = request.form['max_tokens']

        # Set up the headers with your API token
        headers = {
            'Authorization': f'Bearer {api_token}',
            'Content-Type': 'application/json'
        }

        # Parameters for the job
        payload = {
            "job_id": JOB_ID,
            "notebook_params": {
                "api_token": api_token,
                "directory_path": directory_path,
                "max_tokens": max_tokens
            }
        }

        # Make the POST request to trigger the job
        response = requests.post(f'{DATABRICKS_INSTANCE}/api/2.0/jobs/run-now', headers=headers, json=payload)

        if response.status_code == 200:
            run_id = response.json()['run_id']
            # Redirect to the result page with the run_id
            return redirect(url_for('result', run_id=run_id))
        else:
            return f"Failed to start job: {response.content}"

    return render_template('index.html')

@app.route('/result')
def result():
    # Get the run_id from the query string
    run_id = request.args.get('run_id')
    return f"Job started successfully! Run ID: {run_id}"

if __name__ == '__main__':
    app.run(debug=True)
