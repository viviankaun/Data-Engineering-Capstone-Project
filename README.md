# Data-Engineering-Capstone-Project
## AWS S3, spark, parquet
## file name:  Capstone Project Template.ipynb
### read files from s3 using spark.
Data source: csv files and parquet files

### cleaning data: 
- avoid data Duplicate, data null using SQL command like "group by" or "where xxx is not null" 

### data moding 
- save to parquet files

### 
1. The data was increased by 100x.
   > we can scale out (more cluter) or scale up (resize a runningg cluster). AWS EMR managed scaling. 
   
3. The pipelines would be run on a daily basis by 7 am every day.
   > settiing airflow ‘’‘'schedule_interval': ' 0 7 * * *',’
5. The database needed to be accessed by 100+ people.


### references
- Top 10 performance tuning techniques for Amazon Redshift
-https://aws.amazon.com/blogs/big-data/top-10-performance-tuning-techniques-for-amazon-redshift/
- Maximize data ingestion and reporting performance on Amazon Redshift
-https://aws.amazon.com/blogs/big-data/maximize-data-ingestion-and-reporting-performance-on-amazon-redshift/
- Top 8 Best Practices for High-Performance ETL Processing Using Amazon Redshift
-https://aws.amazon.com/blogs/big-data/top-8-best-practices-for-high-performance-etl-processing-using-amazon-redshift/
-Six Steps to Fixing Your Redshift Vacuum
-https://medium.com/@chadlagore/six-steps-to-fixing-your-redshift-vacuum-6aad196d46cf


 

![](https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2019/12/26/redshift-1c.png)
