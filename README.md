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
* Step 5: Complete Project Write Up

### Step 1: Scope the Project and Gather Data

#### Scope 
Explain what you plan to do in the project in more detail. What data do you use? What is your end solution look like? What tools did you use? etc>

My plan was to take immigration data and infuse it with geographic and demographic data in order to come up with valuable insights about immigration to the United States.  The project follows this high level process:  

* The data is cleaned using Spark and uploaded to S3.
* The S3 data is loaded into a Redshift data warehouse using Airflow.   

These are some of the questions my project would be capable of answering:

* For a given country, what cities do immigrants arrive in when they enter the United States?
* Are immigrants arriving in US cities that have a younger or older average population?
* What states are immigrants residing in when living in the United States?

#### Describe and Gather Data 
Describe the data sets you're using. Where did it come from? What type of information is included? 

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
Identify data quality issues, like missing values, duplicate data, etc.

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

I created a python module called etl.py to clean up the data and import the cleaned data to an S3 bucket as parquet files using Spark.  I decided to use Spark because it can easily handle large amounts of data.

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
Map out the conceptual data model and explain why you chose that model

#### 3.2 Mapping Out Data Pipelines
List the steps necessary to pipeline the data into the chosen data model

### Step 4: Run Pipelines to Model the Data 
#### 4.1 Create the data model
Build the data pipelines to create the data model.

#### 4.2 Data Quality Checks
Explain the data quality checks you'll perform to ensure the pipeline ran as expected. These could include:
 * Integrity constraints on the relational database (e.g., unique key, data type, etc.)
 * Unit tests for the scripts to ensure they are doing the right thing
 * Source/Count checks to ensure completeness
 
Run Quality Checks

#### 4.3 Data dictionary 
The data dictionary can be found [here](https://github.com/ls9528/DataEngineeringCapstone/blob/main/documentation/DataDictionary.md). 


### Step 5: Complete Project Write Up
* Clearly state the rationale for the choice of tools and technologies for the project.
* Propose how often the data should be updated and why.
* Write a description of how you would approach the problem differently under the following scenarios:
 * The data was increased by 100x.
 * The data populates a dashboard that must be updated on a daily basis by 7am every day.
 * The database needed to be accessed by 100+ people.
