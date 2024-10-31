#!/usr/bin/env python
# coding: utf-8

# In[ ]:


get_ipython().run_line_magic('pip', 'install openai')


# In[ ]:


get_ipython().run_line_magic('restart_python', '')


# In[ ]:


# Add widgets to capture user inputs
dbutils.widgets.text("api_token", "", "Enter OpenAI API Token")
dbutils.widgets.text("directory_path", "/Workspace/Users/marin.kutrolli@penske.com/Testing Iterative Approach for LLM Response", "Enter Directory Path")
dbutils.widgets.dropdown("max_tokens", "256", ["128", "256", "512"], "Max Tokens")

# Retrieve the widget values
api_token = dbutils.widgets.get("api_token")
directory_path = dbutils.widgets.get("directory_path")
max_tokens = int(dbutils.widgets.get("max_tokens"))


# In[ ]:


# Version 3.1

import os
import pandas as pd
from openai import OpenAI

# OpenAI client to call models
client = OpenAI(
    api_key=api_token,  # your personal access token
    base_url='https://dbc-c29e64b6-6d6d.cloud.databricks.com/serving-endpoints/'
)

# Directory where press releases are located
directory_path = directory_path

# Models chosen for testing
models = ["databricks-meta-llama-3-1-70b-instruct", "databricks-dbrx-instruct", "databricks-meta-llama-3-1-405b-instruct"]

# Function to process a single press release through all 3 models
def process_file_through_models(file_path, models):
    with open(file_path, 'r') as file:
        user_content = file.read()

    prompt = (
        "Please review the press release below and try to determine whether there appears to be an opportunity to sell the company mentioned in the article additional trucks. "
        "Classify your answers as 'Possible Opportunity', 'Unlikely Opportunity', or 'Unknown Opportunity' only. Do not change the format or capitalization of the answer choices."
        "Follow a table format for providing the answer and use only these columns: 'COMPANY_NAME', 'OPPORTUNITY_CLASSIFICATION', 'REASONING', 'REPORT_DATE'. Use 'YYYY-MM-DD' for 'REPORT DATE' format. "
        "**The response must strictly follow the table format.**"
    )

    user_content = prompt + user_content

    messages = [
        {"role": "system", "content": "You are a truck leasing analyst reading through press releases to determine if the listed company might have a use for additional vehicles. "
        "Provide only one classification for each text file analyzed."},
        {"role": "user", "content": user_content},
    ]

    responses = {}
    for model_name in models:
        chat_completion = client.chat.completions.create(
            messages=messages,
            model=model_name,
            max_tokens=max_tokens,
            temperature= 0
        )
        responses[model_name] = chat_completion.choices[0].message.content

    return responses

# Convert response to DataFrame
def response_to_dataframe(response, model_name, file_name):
    lines = response.split('\n')
    lines = [line for line in lines if not line.startswith('| ---')]
    data = [line.split('|') for line in lines if line.strip()]
    
    # Remove empty elements from each row
    data = [[element.strip() for element in row if element.strip()] for row in data]
    
    # Ensure the data has exactly 4 columns and then add "MODEL" and "FILE_NAME"
    if len(data) > 1 and len(data[0]) == 4:
        df = pd.DataFrame(data[1:], columns=data[0])
        df['MODEL'] = model_name
        df['FILE_NAME'] = file_name
        
        # Remove empty columns
        df = df.dropna(axis=1, how='all')
        
        # Reorder columns to place 'MODEL' and 'FILE_NAME' first
        cols = df.columns.tolist()
        cols.insert(0, cols.pop(cols.index('MODEL')))
        cols.insert(1, cols.pop(cols.index('FILE_NAME')))
        df = df[cols]
        
        return df
    else:
        # Handle cases where the data does not match the expected format
        return pd.DataFrame(columns=['MODEL', 'FILE_NAME', 'COMPANY_NAME', 'OPPORTUNITY_CLASSIFICATION', 'REASONING', 'REPORT_DATE'])

# Initialize an empty list to store the combined data
all_data = []

# Iterate over each text file in directory and subdirectories
for root, dirs, files in os.walk(directory_path):
    for filename in files:
        if filename.endswith(".txt"):
            file_path = os.path.join(root, filename)
            
            responses = process_file_through_models(file_path, models)
            
            # Convert responses to DataFrames and combine them
            for model_name, response in responses.items():
                df = response_to_dataframe(response, model_name, filename)
                if not df.empty:
                    all_data.append(df)

# Ensure all_data is not empty before concatenating
if all_data:
    final_df = pd.concat(all_data, ignore_index=True)
    display(final_df)

    # Create Spark dataframe
    final_spark_df = spark.createDataFrame(final_df)

    # To build a table we can query/dashboard, we write the DataFrame to a Delta table
    final_spark_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable("PR_Classification_Results")
else:
    print("No valid data to concatenate.")

