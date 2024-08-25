# Doc
This doc is for containing example scenario and source codes of that scenario. 
Google Cloud Functions and Dataflow usage is provided.

# Scenario
There is a main folder in Google Cloud Storage named Prices. And there is a 3rd party application that runs daily 
and add date folder under Prices folder. After that it loads parquet files in it. 
For example; Prices/23082024/ -> file1.parquet, file2. parquet , file3.parquet and file4.parquet

I will use Google Cloud Functions and Dataflow to get parquet files, do some operations and write them to BigQuery table, 
when new date folder and its files are created.
