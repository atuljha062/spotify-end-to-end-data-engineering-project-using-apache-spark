-- Creating Database
CREATE DATABASE spotify_db;


-- Creating integration which is like a bridge between s3 and snowflake
CREATE OR REPLACE STORAGE INTEGRATION s3_init
    TYPE = "EXTERNAL_STAGE"
    STORAGE_PROVIDER = "S3"
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = "arn:aws:iam::106367026435:role/spotify-spark-snowflake-role"
    STORAGE_ALLOWED_LOCATIONS = ('s3://spotify-project-spark')
        COMMENT = 'Craeting connection to s3'

-- Getting data by describing to create connection with s3
DESCRIBE STORAGE INTEGRATION s3_init;


-- Creating file format
CREATE OR REPLACE FILE FORMAT csv_file_format
    TYPE = "CSV"
    SKIP_HEADER = 1
    FIELD_DELIMITER = ','
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF = ('NULL','null')
    EMPTY_FIELD_AS_NULL = TRUE;


-- Creating stage which actually access s3 using storage integration
CREATE OR REPLACE STAGE spotify_stage
    URL = 's3://spotify-project-spark/transformed_data/'
    STORAGE_INTEGRATION = s3_init
    FILE_FORMAT = csv_file_format


-- using stage listing down all the files and object specified in the url location in stage
LIST @spotify_stage;


-- Creating Tables

-- Album Table
CREATE OR REPLACE TABLE tbl_album(
    album_id STRING,
    name STRING,
    release_date DATE,
    total_track INT,
    url STRING
);

-- Artist Table
CREATE OR REPLACE TABLE tbl_artist(
    artist_id STRING,
    name STRING,
    url STRING
);

-- Songs Table
CREATE OR REPLACE TABLE tbl_song(
    song_id STRING,
    song_name STRING,
    duration_ms INT,
    url STRING,
    popularity INT,
    song_added DATE,
    album_id STRING,
    artist_id STRING
);


-- Accessing tables
SELECT * FROM tbl_album;

SELECT * FROM tbl_artist;

SELECT * FROM tbl_song;


-- Testing Copy command

-- Album
COPY INTO tbl_album
FROM @spotify_stage/album_data/album_transformed_2025-02-16/run-1739716457889-part-r-00000

-- Artist
COPY INTO tbl_artist
FROM @spotify_stage/artist_data/artist_transformed_2025-02-16/run-1739716608715-part-r-00000

-- songs
COPY INTO tbl_song
FROM @spotify_stage/songs_data/songs_transformed_2025-02-16/run-1739716723805-part-r-00000


-- Creating schema for pipe
CREATE OR REPLACE SCHEMA pipe;



-- Creating snowpipe


-- snowpipe for album
CREATE OR REPLACE PIPE spotify_db.pipe.tbl_album_pipe
AUTO_INGEST = TRUE
AS
COPY INTO spotify_db.public.tbl_album
FROM @spotify_db.public.spotify_stage/album_data/;


-- snowpipe for artist
CREATE OR REPLACE PIPE spotify_db.pipe.tbl_artist_pipe
AUTO_INGEST = TRUE
AS
COPY INTO spotify_db.public.tbl_artist
FROM @spotify_db.public.spotify_stage/artist_data/;


-- snowpipe for songs
CREATE OR REPLACE PIPE spotify_db.pipe.tbl_song_pipe
AUTO_INGEST = TRUE
AS
COPY INTO spotify_db.public.tbl_song
FROM @spotify_db.public.spotify_stage/songs_data/;

    
-- Getting information from pipe to add s3 trigger
DESC PIPE spotify_db.pipe.tbl_song_pipe;

DESC PIPE spotify_db.pipe.tbl_artist_pipe;

DESC PIPE spotify_db.pipe.tbl_album_pipe;


-- Counting Number of records in tables
SELECT COUNT(*) FROM tbl_album;
SELECT COUNT(*) FROM tbl_artist;
SELECT COUNT(*) FROM tbl_song;
    