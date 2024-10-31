#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Read the PR_Classification_Results table where all LLM Classification outputs are stored
df = spark.table("PR_Classification_Results")

# Step 1: Remove rows where COMPANY_NAME is blank/null or OPPORTUNITY_CLASSIFICATION is not any of the three choices
valid_opportunities = ["Possible Opportunity", "Unlikely Opportunity", "Unknown Opportunity"]
df_filtered = df.filter(
    (F.col("COMPANY_NAME").isNotNull()) & 
    (F.col("COMPANY_NAME") != "") & 
    (F.col("OPPORTUNITY_CLASSIFICATION").isin(valid_opportunities))
)

# Keep only the specified columns in the desired order
columns_to_keep = ["MODEL", "FILE_NAME", "COMPANY_NAME", "OPPORTUNITY_CLASSIFICATION", "REASONING", "REPORT_DATE"]
df_filtered = df_filtered.select(columns_to_keep)

# Step 2: Group by COMPANY_NAME and OPPORTUNITY_CLASSIFICATION and count the occurrences
df_classification_counts = df_filtered.groupBy("COMPANY_NAME", "OPPORTUNITY_CLASSIFICATION").count()

# Use window function to determine the opportunity with the most votes
window_spec = Window.partitionBy("COMPANY_NAME").orderBy(F.desc("count"))
df_with_rank = df_classification_counts.withColumn("rank", F.row_number().over(window_spec))

# Step 3: Final Decision for each COMPNAY_NAME 
df_final_decision = df_with_rank.filter(F.col("rank") == 1).select(
    "COMPANY_NAME", 
    F.col("OPPORTUNITY_CLASSIFICATION").alias("Final_Decision")
)

df_filtered = df_filtered.withColumn("key", F.concat_ws("_", F.col("COMPANY_NAME"), F.col("OPPORTUNITY_CLASSIFICATION")))
df_final_decision = df_final_decision.withColumn("key", F.concat_ws("_", F.col("COMPANY_NAME"), F.col("Final_Decision")))

# Step 4: Bring back original table info relevant for each Final Decision
window_spec_instance = Window.partitionBy("key").orderBy("REPORT_DATE")
df_filtered_with_instance = df_filtered.withColumn("row_number", F.row_number().over(window_spec_instance))     .filter(F.col("row_number") == 1)     .drop("row_number")


df_final = df_final_decision.alias("final_decision").join(
    df_filtered_with_instance.alias("filtered_instance"),
    on="key",
    how="inner"
).select(
    "filtered_instance.MODEL", 
    "filtered_instance.FILE_NAME", 
    "filtered_instance.COMPANY_NAME", 
    "final_decision.Final_Decision", 
    "filtered_instance.REASONING", 
    "filtered_instance.REPORT_DATE"
)

# Step 5: Compile a dataset to embedd in existing targeting structure
df_possible_opportunity = df_final.filter(F.col("Final_Decision") == "Possible Opportunity")


window_spec_file = Window.partitionBy("FILE_NAME").orderBy("REPORT_DATE")
df_first_instance = df_possible_opportunity.withColumn("row_number", F.row_number().over(window_spec_file))     .filter(F.col("row_number") == 1)     .drop("row_number")

# Save the final DataFrame to a table for dashboarding
df_first_instance.write.mode("overwrite").saveAsTable("Possible_Opportunities")

display(df_first_instance)

