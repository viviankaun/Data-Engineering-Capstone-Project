# Data-Engineering-Capstone-Project
## AWS S3, spark, parquet
## file name:  Capstone Project Template.ipynb
### read files from s3 using spark.
Data source: csv files and parquet files

### Cleaning data: 
- Avoid duplicate data using SQL command : "group by" , "count(*) count"
   <p> The GROUP BY statement groups rows that have the same values into summary rows, select a, b, c, count(*) count, When we get count >1, it is mean we got duplicate record  </p>
```
select City, State, cast(`State Code` as varchar(10) ) State_Code, Race, Count(*) count from cities\
        group by City, State,`State Code`, Race\
        order by count desc "
```
- Avoid missing values using SQL command: where Field is not null

```
select City, Country, avg(AverageTemperature) AverageTemperature
from temperature 
where country='United States' and AverageTemperature is not null
group by City, Country
```
### file names:
Capstone Project Template.ipynb
qa_check.py : checking data quality
### Data moding :  save to parquet files 
- fact_immgration:  model
- dim_airport
- dim_cities
- dim_temperature 

### How often the data should be updated 
It depends on our data resource update schedule, if we get new files daily, then we can run them daily. 
I suppose the system will update new files every day, then we just run it daily.  

###
- The data was increased by 100x.
  <p>1. we can use Spark provides an interface for programming entire clusters with implicit data parallelism. Or we can scale out (more cluter) or scale up (resize a runningg cluster). AWS EMR managed scaling. for example, we can add more node in our clusters. </p>
   
- The pipelines would be run on a daily basis by 7 am every day.
   Airflow to schedule routine time 
   ```
   dag = DAG('udac_example_dag', 
          description = ' Airflow',
          schedule_interval = '0 7 * * *',  
          max_active_runs = 1   
     )
   ``` 
- The database needed to be accessed by 10K people.
  <p> Multi-AZ creates a standby database to increase availability. Read Replicas can be used to scale and increase performance for read-heavy workloads. Scaling the database tier with Amazon RDS Read Replicas. Read Replicas are available if you are using MySQL, PostgresSQL, or Amazon Aurora. RDS MySQL and RDS PostgresSQL allow up to five Read Replicas and leverage native replication capability of MySQL and PostgresSQL that are subject to replication lag </p>
   
 
 


### references 
- <a href='https://aws.amazon.com/blogs/big-data/top-10-performance-tuning-techniques-for-amazon-redshift/' target=''>Top 10 performance tuning techniques for Amazon Redshift </a><br>
- <a href='https://aws.amazon.com/blogs/big-data/maximize-data-ingestion-and-reporting-performance-on-amazon-redshift/'>Maximize data ingestion and reporting performance on Amazon Redshift </a><br>
- <a href='https://aws.amazon.com/blogs/big-data/top-8-best-practices-for-high-performance-etl-processing-using-amazon-redshift/'>Top 8 Best Practices for High-Performance ETL Processing Using Amazon Redshift</a><br> 


 

![](https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2019/12/26/redshift-1c.png)
