# Sparkify Data Lake

**Author: Jarome Leslie**

**Date: 2021-10-11**

## Introduction 
With the goal of expanding on Sparkify's existing data warehouse platform, this project involves the creation of a data lake using Apache Spark. First, the raw directories of JSON logs and song files are loaded from an AWS S3 bucket `s3://udacity-dend/`. Next the data is transformed into a five different tables, then written to partitioned parquet files to their respective table directories in the S3 bucket `s3://udacity-workspace/`.

With this tool, the Sparkify team is provided the greater capability of querying their data to ask and more efficiently answer questions about their expanding user base. 


## Database schema design and ETL process
The diagram below illustrates how the Sparkify database is modeled using the Star Schema and is centred around the *songplay* Fact table. Supporting its definition are four Dimension tables: *songs*, *users*, *artists*, and *time*. As highlighted in the diagram, the tables have been partioned in the following way: 
- the songplays table is partitioned by year and month, based on the start_time field; 
- the time table is partitioned by year and month; and
- the songs table is partitioned by year and artist.

![alt text](img/project_pipeline2.png "Sparkify Data Lake Project")

## Files in repository
The source files provided include logs and song data. With respect to logs, event files for each day of November 2018 were provided in `json` format. In terms of songs, a subset of the **Million Song Dataset** is provided as a collection of files in `json` format.


## How to run the python scripts

1. To run the data pipeline, run the `etl.py` script:
    ```python etl.py```


## Run the ETL pipeline using AWS EMR Spark clusters 
Leveraging the computing resources of AWS EMR, can improve the execution from the X hours required locally to the Y minutes achieved using the below prescribed configuration. Prior to executing the below steps, it is important to create an IAM user with the **`AmazonS3FullAccess`** role.

1. Create an EMR cluster as follows:
    ```
    aws emr create-cluster --name udacity-spark-proj --use-default-roles\ 
     --release-label emr-5.28.0 --instance-count 3 --applications Name=Spark\ 
     --ec2-attributes KeyName=AWS_EC2_Personal,SubnetIds=subnet-0f756913b00e7b3ac\
     --instance-type m5.xlarge --profile personal
    ```
    
2. Enable port forwarding as follows:
    ```
    ssh -i <path to local pem file> -N -D 8157 hadoop@<Public IPv4 DNS>
    ```

3. Copy AWS certification file to EMR cluster as follows:
    ```
    scp -i <identity file> <path to local pem file> -N -D 8157 hadoop@<Public IPv4 DNS>:/home/hadoop
    ```

4. Connect to EMR cluster using the command:
    ```
    ssh -i <path to local identity file> hadoop@<Public IPv4 DNS>
    ```

5. Execute Spark job using the command below:
    ```
    /usr/bin/spark-submit --master yarn ./etl.py
    ```
    
5. Terminate EMR cluster after job is completed
    ```
    terminate-clusters --cluster-ids <value> 