Sparkify is a startup company who provides music streaming services. They have JSON metadata on the songs in their app (s3a://udacity-dend/song_data/\*) and user activity data (s3a://udacity-dend/log_data/\*). The data is stored currently in S3. As their users are growing, Sparkify decided to move their data warehouse to a data lake. 

## purpose

Build an ETL pipeline which
1. Extracts data from s3
2. Processes the data using Spark according to the need of data analytics team
3. Loads the transformed dimensional tables back to s3

## designed tables

**Fact Table**

songplays
- songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

**Dimension Tables**

users

- user_id, first_name, last_name, gender, level

songs

- song_id, title, artist_id, year, duration

artists

- artist_id, name, location, latitude, longitude

time

- start_time, hour, day, week, month, year, weekday

## file description

- **config.ini** - stores the aws credentials
- **etl.py** - extracts the data from s3, processes them, creates new tables and stores them back to s3 as parquet
- **queries.py** - stores the queries used in etl to process and create tables
- **test.py** - tests whether the created tables includes the right columns and loaded correctly to s3
- **emr_etl_script.py** - script for running on emr cluster, it extracts the data from s3, processes them, creates new tables, stores them back to s3 and tests loaded tables have correct columns.


## usage

### Testing locally
Before working with the big data, test the codes locally with a subset of the big data:

1. Update the config.ini with your own aws key and secret
2. (Optional) update the output_data path in etl.py according to your preference
3. After having valid aws key and secret in config.ini, you can start the etl by typing the following commands in the command line:
    ```$ python etl.py``` 
4. Step 3 will take a while. After that, test the outcome by typing the following commands in the command line:
    ```$ python test.py``` 

### Move to emr cluster

After testing the code locally, move on to emr cluster to etl the entire data sets:

1. Create a s3 bucket to store the output data and update the output_data path in emr-script

2. Create a emr cluster on aws and **enable the security groups setting of the master EC2 instance to accept incoming SHH protocol from our local computer**. The following is the example of creating emr cluster using aws cli:

    ``` aws emr create-cluster \
            --name sparkify \
            --use-default-roles \
            --release-label emr-5.20.0 \
            --instance-count 4 \
            --applications Name=Spark Name=Hive \
            --ec2-attribute KeyName=YOUR_EC2_KEY_NAME,SubnetId=YOUR_SUBNET_ID \
            --instance-type m5.xlarge
    ```
    
3. Copy the emr_etl_script to your EMR master node

   ``` scp -i PATH_TO_EC2_KEY PATH_TO_ETL_SCRIPT hadoop@YOUR_MASTER_NODE_DNS:/home/hadoop/ ``` 
   
4. Connect to your master node

    ``` ssh -i YOUR_EC2_KEY_PATH hadoop@YOUR_MASTER_NODE_DNS ```
    
5. Submit your script

    ``` usr/bin/spark-submit --master yarn PATH_TO_THE_SCRIPT ```

6. Terminate your emr cluster. You should find all the tables stored in your chosen ouput_data path


## Credits
The codes are written by me and the sparkify data belongs to Udacity.