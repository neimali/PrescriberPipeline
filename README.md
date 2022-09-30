# US prescribers reports: Project overview
+ Create two reports on detail of prescribers in US. 1. Reported number of prescribers and total prescription prescibed of each city. 2. Reported Top 50 prescribers in each state
+ Preprocessed and transform data on YARN by pyspark and SQL command for report generataion 
+ Persist data in hive and transfering data to AWS S3 and Azure Blobs.
+ Perform an unit test to make sure functions were applied correctly.

## Data Resource
### Click link for data download
+ [Prescribers information in city dimension](https://prescpipeline.blob.core.windows.net/input-vendor-data/city/us_cities_dimension.parquet?st=2022-04-21T14:19:25Z&se=2022-12-31T22:19:25Z&si=read&spr=https&sv=2020-08-04&sr=c&sig=wjY0KtPvyy%2BbIpopBqMKAGmmSHsSvLhqL0n%2BBGFVXOQ%3D)
+ [Fact of priscibers](https://prescpipeline.blob.core.windows.net/input-vendor-data/presc/USA_Presc_Medicare_Data_2021.csv?st=2022-04-21T14:19:25Z&se=2022-12-31T22:19:25Z&si=read&spr=https&sv=2020-08-04&sr=c&sig=wjY0KtPvyy%2BbIpopBqMKAGmmSHsSvLhqL0n%2BBGFVXOQ%3D)

## Data Ingesting
+ use pyspark convert csv and parquet files into dataframes

## Data Preprocessing
### City dimension
+ Select required columns
### Prescribers fact
+ Select required columns
+ Rename the columns
+ Add Contry field
+ Convert 'years of experience' from string to number
+ Combine first name and last name of prescribers
+ clean and check Null/Nan values

## Data Transformation
### city dimension report
#### transform logics:
1. calculate the number of zips in each city
2. calculate the number of distinct prescribers assigned of each city
3. calculate total TRX_CNT prescribed for each city
4. Do not report a city in the final report if no prescriber is assigned to it
#### Layout:
    City Name
    State Name
    Country Name
    City Population
    Number of Zips
    prescriber Counts
    Total Trx counts
    
### Prescribers fact report
Top 5 prescribers with highest trx_cnt per each state consider the prescribers only from 20-50 years of experience
#### layout:
    Prescriber ID
    Prescriber Full Name
    Prescriber State
    Prescriber Country
    Prescriber Years of Experience
    Total TRX Count
    Total Days Supply
    Total Drug Cost
    
## data storage and persisting
Created reports were stored as json and orc files. Files were persisted at hive.
Copys of reports were also transfered to AWS S3 and Azure Blob according to clients' specific requirments.
