# task5_Airflow
Apache Airflow Introduction Task
Create an Airflow job that will do (ETL):
1. Get all data from the CSV file (at archive.zip)
2. Prepare data for loading (cleaning/preprocessing using Airflow local cluster): 
-clean all unset rows
-replace "null" values with "-"
-sort data by created date
-remove all unnecessary symbols from the content column (for example, smiles and etc.), leave just text and punctuations
3. Push it to MongoDB or other non-relational databases with the same schema (everything should work locally)

After you pushed all data to MongoDB, please write and execute the following queries (directly in Mongo, for example, Aggregations tab in MongoDB Compass):
-Top 5 famous commentaries
-All records, where the length of field “content” is less than 5 characters
-Average rating by each day (should be in timestamp type)

The task should be done in a local or virtual Linux environment.

NO DOCKER.

Technological requirements:
- Airflow 
- Python
- Pandas
- MongoDB
- Other you prefer
