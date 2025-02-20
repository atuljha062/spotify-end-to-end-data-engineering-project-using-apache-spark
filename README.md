# Spotify End to End Data Engineering Project using Apache Spark

### Introduction
Our project focuses on developing an ETL (Extract, Transform, Load) pipeline that processes data from the Spotify API (spotipy). This pipeline efficiently retrieves JSON data from Spotify, applies transformations using Apache Spark, and loads the refined data into Snowflake for analysis.
Apache Spark is used to clean and transform the raw JSON data, ensuring it is structured and optimized for storage and analysis. The transformed data is then stored in Snowflake using Snowpipe, enabling near real-time ingestion and querying. This architecture ensures efficient data processing, making it easier to analyze music trends and gain insights from Spotify’s dataset.

### Architecture
![Architecture Diagram](https://github.com/atuljha062/spotify-end-to-end-data-engineering-project-using-apache-spark/blob/main/Architecture%20Diagram/Architecture%20Diagram.jpg)

### About Data
In our project, we leverage data from the Spotify API using the spotipy library. Specifically, we extract the top 50 songs Global data in JSON format. The Spotify API provides a rich source of information about music, including details about songs, artists, albums, and more.

By utilizing the spotipy library, we efficiently retrieve the top 50 songs Global data in JSON format from the Spotify API. This data serves as a valuable source of information for our project, enabling us to analyze trends in global music consumption and derive insights that inform our decision-making processes.

Overall, the Spotify API, coupled with the spotipy library, empowers us to access and utilize rich music data, enhancing the functionality and effectiveness of our project.

### Services Used
1. **AWS S3 (Simple Storage Service):** Amazon S3 (Simple Storage Service) is a highly scalable object storage service that can store and retrieve any amount of data from anywhere on the web. It is commonly used to store and distribute large media files, data backups, and static website files.

2. **Apache Spark:** Apache Spark is an open-source distributed computing system that enables high-speed data processing and transformation. We use Spark to clean, transform, and structure the extracted Spotify data before loading it into Snowflake.

3. **AWS Lambda:** AWS Lambda is a serverless computing service that let you run your code without managing servers. You can use Lambda to run code in response to events like change in S3, DynamoDB, or other AWS services.

4. **AWS CloudWatch:** Amazon CloudWatch is a monitoring service for AWS resources and the applications you run on them. You can use CloudWatch to collect and track metrics, collect and monitor log files, and set alarms.

5. **Snowflake:** Snowflake is a scalable and high-performance cloud-based data warehousing platform, offering centralized storage and analytics for structured and semi-structured data.

6. **Snowpipe:** Snowpipe is a feature of Snowflake that enables real-time data ingestion, allowing seamless and automated loading of data into Snowflake Data Warehouse for immediate analysis and processing.


### Project Execution Flow
Lambda Trigger (every week) -> Runs Lambda Function Extracting_and_loading_script (Extract Data from Spotify Top 100 - Global Playlist using spotipy library) -> Stores Raw Data in AWS S3 -> Trigger Transform Fuction -> Runs Glue job spotify_transformation_job (Transforms the data) -> Stores Transformed Data in AWS S3(Which then sends event notification to snowpipe) -> Triggers the Snowpipe which then extracts the data from transformation buckets and loads them in respective tables in Snowflake -> Data stored in snowflake can be queried for Analysis
