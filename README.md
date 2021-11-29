# Data Engineering Capstone Project
### Lauren Hemelt

#### Project Summary
This project is my capstone submission for the Udacity Data Engineering Nanodegree.  It showcases what I've learned through the program by taking large sets of data and organizing it in such a way that allows others to gain valuable insight from it.

The main set of data I used is the provided immigration data and I enhanced this data by combining it with geographic and demographic data.  Ultimately, I created an ETL process using Spark and Airflow that takes this data from separate parquet and csv files to a queryable data warehouse in Redshift.  

The project follows the follow steps:
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

#### Cleaning Steps
Document steps necessary to clean the data

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


#### Step 5: Complete Project Write Up
* Clearly state the rationale for the choice of tools and technologies for the project.
* Propose how often the data should be updated and why.
* Write a description of how you would approach the problem differently under the following scenarios:
 * The data was increased by 100x.
 * The data populates a dashboard that must be updated on a daily basis by 7am every day.
 * The database needed to be accessed by 100+ people.
