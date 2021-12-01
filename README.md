# Data Engineering Capstone Project
### Lauren Hemelt

#### Project Summary
This project is my capstone submission for the Udacity Data Engineering Nanodegree.  It showcases what I've learned through the program by taking large sets of data and organizing it in such a way that allows others to gain valuable insight from it.

The main set of data I used is the provided immigration data and I enhanced this data by combining it with geographic and demographic data.  Ultimately, I created an ETL process using Spark and Airflow that takes this data from separate parquet and csv files to a queryable data warehouse in Redshift.  

The project follows the following steps:
* Step 1: Scope the Project and Gather Data
* Step 2: Explore and Assess the Data
* Step 3: Define the Data Model
* Step 4: Run ETL to Model the Data
* Step 5: Further Questions

### Step 1: Scope the Project and Gather Data

#### Scope 
My plan was to take immigration data and infuse it with geographic and demographic data in order to come up with valuable insights about immigration to the United States.  The project follows this high level process:  

* The data is cleaned using Spark and uploaded to S3.
* The S3 data is loaded into a Redshift data warehouse using Airflow.   

These are some of the questions my project would be capable of answering:

* For a given country, what cities do immigrants arrive in when they enter the United States?
* Are immigrants arriving in US cities that have a younger or older average population?
* What states are immigrants residing in when living in the United States?

#### Describe and Gather Data 
The following data sets are used in this project:

##### I94 Immigration Data
The immigration data comes from the [US National Tourism and Trade Office](https://travel.trade.gov/research/reports/i94/historical/2016.html).  It includes information about anyone entering the United States, such as their age and year of birth, the date entering and departing the country (if they've departed), what city they arrived at, and the state where they currently reside. Specificially, the data from April 2016 is used to showcase this project, which is over 1 million records.  The data is in parquet format. 

##### U.S. City Demographic Data
The demographic data comes from [OpenSoft](https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/).  It includes demographic information about US cities, such as the median age, total population, and specific populations (male vs female, foreign-born, different races, etc.).  The data is in csv format. 

##### City Data
This data was provided in I94_SAS_Labels_Descriptions.SAS in the provided project and contains a mapping of city names and their I94 codes that are found in the immigration data.  I manually converted this data to csv format.

##### Country Data
This data was provided in I94_SAS_Labels_Descriptions.SAS in the provided project and contains a mapping of country names and their I94 codes that are found in the immigration data.  I manually converted this data to csv format.

##### State Data
This data was provided in I94_SAS_Labels_Descriptions.SAS in the provided project and contains a mapping of state names and their abbreviations that are found in the immigration data.  I manually converted this data to csv format.

##### Travel Mode Data
This data was provided in I94_SAS_Labels_Descriptions.SAS in the provided project and contains a mapping of travel mode descriptions and their number IDs that are found in the immigration data.  I manually converted this data to csv format.

##### Visa Type Data
This data was provided in I94_SAS_Labels_Descriptions.SAS in the provided project and contains a mapping of visa type descriptions and their number IDs that are found in the immigration data.  I manually converted this data to csv format.

### Step 2: Explore and Assess the Data
#### Explore the Data 
The two main datasets that require automated cleanup are the immigration and city demographic data.  I explored the data using Spark in a Jupyter notebook.  Some of my findings included:

##### Immigration Data
* The data spans immigrant arrival dates from 4/1/2016 to 4/30/2016
* The total number of records are 3,096,313
* The dates are provided as the number of days between 1/1/1960 and the given date
* The field cicid is unique to each record

##### City Demographic Data
* There are only 5 potential values for race

#### Cleaning Steps
Document steps necessary to clean the data

I created a python module called etl.py to clean up the data and import the cleaned data to an S3 bucket as parquet files using Spark.  I decided to use Spark because it can easily handle large amounts of data.  S3 was chosen as a storage solution because it is easily accessible by other useful AWS products.

##### Immigration Data
When uploading to S3, I partitioned the immigration data by arrival date becuase it was never null and provided a fairly even distribution of the data for future pipeline steps. 

I completed the following steps in etl.py to clean the immigration data and import it to S3:
* Convert the fields to their appropriate data types (integer)
* Rename the fields to nice, easy to understand names
* Remove any unneeded fields
* Convert the date fields to the format yyyy-mm-dd
* Duplicate the arrival_date field so that the data could be partitioned by arrival_date and still be included in the data files
* Write the data to an S3 bucket as parquet files partitioned by arrival date

##### Date Data
From the cleaned immigration data frame, I extracted the unique arrival and departure dates into their own separate Spark data frames.  Bu combining their distinct values together via a union, I create a list of unique dates used throughout the immigration data to serve as a the basis for a date file.  This data was then written to the S3 bucket as parquet files.  I did not partition this data since it was only 1 single column of a limited amount of dates.

##### City Demographic Data
When uploading to S3, I converted the city demographic data to parquet files as well because it is more efficient than csv.  I did not partition this dataset becuase I did not find an efficient field on which to do so and, following cleanup, the data was not significantly large.  I elimiated duplicates with the same city and state in this data by keeping those records with the greatest race population.  This will allow for the potential to query immigration data based on the majority race in each city.   

I completed the following steps in etl.py to clean the city demographic data and import it to S3:
* Convert the fields to their appropriate data types (integer, double)
* Rename the fields to nice, easy to understand names
* Remove duplicate records with the same city and state by keeping those records with the greatest race population
* Write the data to an S3 bucket as parquet files

The other, smaller datasets that came from I94_SAS_Labels_Descriptions.SAS does not require cleanup using Spark.  I manually examined this data and did not find any significant issues.  Under the assumption that these are official I94 values, I did not change any of the data values, nor did I omit any of it.  I added headers to each of these datasets. I also changed the comma delimter to a pipe for the city and country data due to some data values containing commas.  Once these changes were complete, I manaully uploaded the data files to the S3 bucket.  

### Step 3: Define the Data Model
#### 3.1 Conceptual Data Model
For this project, I used a modified star schema for the data model.  My intention is to keep the queries as simple as possible by not making the tables too normalized and limiting the required number of joins.  An ER diagram of this data model can be found [here](https://github.com/ls9528/DataEngineeringCapstone/blob/main/documentation/ERDiagram.pdf).  

At the center of the data model is a fact table focusing on immigration (fact_immigration), the majority of which can be found in the immigration data.  The dimensions provide additonal information to support the immigration data (dim_date, dim_country, dim_state, dim_city, dim_visa_type, dim_travel_mode) and, other than dim_date, are taken from the other data sources.  The only diversion from the traditional star schema can be found with dim_state, which connects to dim_city as well as fact_immigration.  

All the tables have an integer as their primary key except for dim_date.  

The following tables use newly-created auto-incrementing IDs as primary keys:
* dim_country
* dim_state
* dim_city

The following tables use an existing field from the original data as primary keys:
* fact_immigration
* dim_date
* dim_visa_type
* dim_travel_mode

#### 3.2 Mapping Out Data Pipelines
The data model is realized as a Redshift data warehouse in AWS.  I chose to store it in Redshift due to its ability to efficiently handle large amounts of data.

Some of the S3 data files do not need further transforming and can be inserted directly into their respective dimension tables in Redshift. These are:
* Country data -> dim_country
* State data -> dim_state
* Visa type data -> dim_visa_type
* Travel mode data -> dim_travel_mode

The rest of the S3 data files need further transformation prior to being inserted into the final Redshift tables.  The first step with this data is to insert it into staging tables.  From there, those staging tables can be queried to insert the data into the final tables in the expected format.

##### Date Data
A special query to the date staging table uses date functions to populate the additonal columns in dim_date (month, day, year, week, day_of_week).

##### City Data and City Demographic Data
After copying this data to their respective staging tables, a query joins these tables together on city name and state abbreviation to populate the dim_city table.  This query also makes a join to the already existing dim_state table to include that table's state_id field in dim_city.  

##### Immigration Data
The query to the immigration staging table to populate the fact table includes joins to dim_country, dim_city, and dim_state.  These joins allow for the various newly created ID fields in these dimension tables to be included in fact_immigration.

I chose Airflow to complete this data pipeline process from S3 to Redshift because it's a simple way to visualize the data pipeline process and it can handle processing the data.  It also has has many useful features, such as scheduling processes and adding data quality checks.

### Step 4: Run Pipelines to Model the Data 
#### 4.1 Create the data model
The data model itself is created by manually running a series of [create table statements](https://github.com/ls9528/DataEngineeringCapstone/blob/main/code/airflow/plugins/create_tables.sql) directly in Redshift.  From there, Airflow can complete the rest of the data pipeline process.

As alluded to in the previous section, the data pipeline in Airflow consists of two main operators:
##### StageToRedshiftOperator
This operator takes a file and copies it directly into a Redshift table.  It's capable of handling either parquet or csv files by indicating the file type in a parameter.  Another useful parameter is whether or not to truncate the table prior to loading any data (which is set to true for all staging and dimension table loads).  For csv files, optional csv-specific parameters can also be set, such as whether a header exists and what delimiter is used.  This operator is used to populate all staging tables and some of the dimension tables, specifically the following:
* staging_date
* staging_city
* staging_demographic 
* dim_country
* dim_state
* dim_visa_type
* dim_dim_travel_mode

##### LoadTableOperator
This operator queries tables populated using StageToRedShiftOperator and populates another Redshift table with those query results.  It provides an optional parameter called columns that allows the user to specify the fields in which to enter data rather than assuming every field is being populated.  Another useful parameter is whether or not to truncate the table prior to loading any data (which is set to true for all dimension table loads and set to false for the incremental fact table loads).  There is a [helper file](https://github.com/ls9528/DataEngineeringCapstone/blob/main/code/airflow/plugins/helpers/sql_queries.py) that contains all the queries referenced in this operator.  This operator is used to populate some dimension tables and the fact table, specifically the following:
* dim_date
* dim_city
* fact_immigration

I split the data pipeline into two dags, one for the dimension tables and another for the fact table.  This was done so that the smaller dimension tables data could be loaded together as a whole, while the much larger immigration data could be split up into separate loads by arrival date.  Thus, as new immigration data becomes available going forward, it can be added to the fact_immigration table on a daily basis in small increments. 

##### dim_data_pipeline_dag
![dim_data_pipeline_dag](https://user-images.githubusercontent.com/90398812/144250505-0c7591f1-685f-4682-9e8b-321602b71643.png)

##### fact_data_pipeline_dag
![fact_data_pipeline_dag](https://user-images.githubusercontent.com/90398812/144250535-6f5fe8df-c990-40ad-bd31-1a86ac43630b.png)

#### 4.2 Data Quality Checks
The final operator called in both dags is a data quality check called DataQualityOperator.  It takes in as a parameter a series of queries to run on the newly populated tables to ensure the data pipeline runs as expected.  Two other parameters that this operator accepts is an expected value and any valid comparison (=, <>, >, <, >=, <=).  This allows the user much flexibility when creating data quality checks.  Because the data pipeline process I run is the first time these tables are populated, my currently written queries check that each dimension and fact table is populated with data (that the expected result is greater than 0).  However, due to the operator's flexibility to accept any valid comparison, other data quality checks can easily be included, such as:
* Ensuring that values for a given field are not all null
* Checking that a table has at least a certain amount of records
* Confirming how many distinct values are in a given field

#### 4.3 Data dictionary 
The data dictionary for the final data model can be found [here](https://github.com/ls9528/DataEngineeringCapstone/blob/main/documentation/DataDictionary.md). 

### Step 5: Further Questions

##### Write a description of how you would approach the problem differently under the following scenarios:
###### The data was increased by 100x.
For the existing project, the Spark and Airflow processes are run in the Udacity workspace.  If the data was increased by 100x, I would run these processes on a more powerful environment in AWS, such as Amazon Elastic MapReduce (EMR) for Spark and Amazon Managed Workflows for Apache Airflow (MWAA) for Airflow.
###### The data populates a dashboard that must be updated on a daily basis by 7am every day.
As the data pipeline currently stands, it's built so that the fact table can be infused with new data on a daily basis.  I'd add to Airflow a schedule to run that dag sometime in the early morning and a Service Level Agreement (SLA) to ensure the dag completes in a reasonable amount of time.  Currently, the data pipeline is written assuming only new data will be added in updates.  If the updates include changes to existing data, then the data pipeline will have to be altered to account for this possibility, perhaps changing LoadTableOperator to be able to handle either inserts or updates or creating an entirely new operator for updates.   
###### The database needed to be accessed by 100+ people.
If the database needed to be access by 100+ people, I'd first analyze the performance of the exisitng Redshift data warehouse.  If better performance was needed, I'd consider creating an additonal data pipeline that created smaller, more specific data marts based on the data that people in different departments need.
