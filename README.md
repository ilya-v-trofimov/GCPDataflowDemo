# GCPDataflowDemo

A simple Java ETL application based on Apache Beam framework.
It 
 - reads files using TikaIO transform (based on Apache Tika)
 - parses as CSV 
 - writes to BigQuery table
## Prerequisites
 - Source dataset is a file of xlsx format. 
 - Structure of the file should match the structure of an open dataset available at the following URL:
 https://data.nsw.gov.au/data/dataset/4e51f1d3-4b72-48b8-96ef-8493b7aa9c37/resource/ca5c4a11-64f8-4583-91c9-d9436d38e2e9/download/fa13-2016-17.xlsx
 - Source file should be uploaded to the storage, supported by built-in Apache Beam IO transforms (https://beam.apache.org/documentation/io/built-in/)
 - Google Cloud Platform project must be created
 - Following GCP APIs should be enabled: Cloud Dataflow, Google Cloud Storage, BigQuery
 - Service account should be created to enable access to the project
 - Following access rights should be granted to the Service Account: 
    - BigQuery Admin
    - Dataflow Admin
    - Storage Admin
 - Service account key credentials should be configured for the GCP project  
 - GOOGLE_APPLICATION_CREDENTIALS must be set in the environment which application is run from. For more details on that, please see 
 https://cloud.google.com/dataflow/docs/quickstarts/quickstart-java-maven
 
## Install
Simply clone this repository 
## Run
Apache Beam pipeline can be run on top of many platforms, including local machine and Google Cloud Platform Dataflow

To run on GCP Dataflow, use the following command:
```
mvn compile exec:java \
-Dexec.mainClass=com.octo.datarepexam.GCPDataflowSample \
-Dexec.args=" \
--runner=dataflow \
--source=<path_to_source_file_on_gcs> \ 
--output=<BigData_table> \
--project=<your_gcp_project> \
--tempLocation=<temp_location_on_gcs>"
```

To run on local machine, use the following command:
```
mvn compile exec:java \
-Dexec.mainClass=com.octo.datarepexam.GCPDataflowSample \ 
-Dexec.args=" \
--runner=DirectRunner \
--source=<local_path_to_source_file> \
--output=<BigData_table> \
--project=<your_gcp_project> \ 
--tempLocation=<temp_location_on_gcs>"
```
 