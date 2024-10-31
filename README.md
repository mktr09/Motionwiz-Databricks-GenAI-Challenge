# Motionwiz-Databricks-GenAI-Challenge
Repository for APRA (Automated Press Release Analyst)

### For information on business use case, costs, desging & challenges as well as next steps please read the project wiki! ###

## Running APRA's POC

*LLM Classification Engine.py* contains the bulk of the code which utilizes LLM calling through Databricks Serving Endpoints.
OpenAI client will need to be installed before running the code.
api_token, directory_path, max_tokens were coded as widgets so any user can easily input their information or utilize the Engine through a web app interface.

*Consensus_Opportunity_Classification_and_Cleanup.py* contains the code needed for standardizing and cleaning the responses so only desired results are pushed through.

**NOTE**: Both files above were developed as Jupyter Notebook files in Databricks originally -- .py versions were added here for ease of readability and replication.


*job_configuration.json* job can be triggered by utilizing the webapp files.
app.py will trigger this Databricks job which is responsible for a) triggering the Classification Engine and building a Delta Table with all classification results and b) upon successful completion of the classification task Consensus_Opportunity_Classification.py will be triggered.
End result is a curated set of responses containing only Possible Opportunities

For the POC, job results were pushed to Delta Live tables and dashboards were created. ALternatively, you can directly push results to a .csv file to examine the classification results.

