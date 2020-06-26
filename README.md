# BACKGROUND

## Fictional
Sparkify is an online music streaming service, in which users can select songs to listen to and play them. The company records these listening events and has demographic information about their users. Additionally they have information about the music in their collection. The Sparkify team wants to combine information from their listening events log with their song information in order to gain insight into user behavior. This database exists in order to synthesize data from two separate data sets, a log of customer listening events and information describing songs in the library.

This data originally resided on local servers in JSON format, but has since been moved to S3 in the cloud. The data will be tranformed into meaningful tables for the analystics team on S3 as a data lake.

## Real
Sparkify is a model of an online music streaming service, similar to Spotify or Pandora. The dataset for the songs comes from the [Million Song Dataset] (http://millionsongdataset.com/) and the dataset for the user behavior was generated using the [Eventsim event simulator] (http://millionsongdataset.com/). The database is implemented using Python, Spark, and S3.


# STRUCTURE & ETL PIPELINE

The final database consists of 5 tables, a fact table (songplays) and four dimension tables (users, songs, artists, and time). These are assembled in a star schema, with each of the four dimension tables providing attribute information for the songplays fact table. Each table reflects a real-world entity within the scope of the Sparkify project. A "songplay" is an event in which a user plays a song on the app. This event necessarily involves a user and a song, and occurs at a time. Additionally, every song must have an artist.

Data about songs and song play events are found in JSON files on S3. These full JSON files are read into staging tables, and contain more data than is used for the Sparkify project. During the ETL process, relevant columns are used to construct a new "songs" table, while other columns are used to construct the "artists" table. This decomposition into two separate tables is part of the normalization process, in order to separately describe the songs and artists entities. Data about the users is extracted to a separate "users" table. Timestamp data is also used to construct a separate "time" table with derived attributes as part of the date heirarchy.

A *song_title* and an *artist_name* field serve as keys to relate the songplays table to the songs and artists tables respectively. During the ETL process, song and artist data are brought into the songplays table when a relationship exists.

![Star Schema ERD](/sparkify_erd.png)


# INSTRUCTIONS

#The user must use EMR to create an S3 bucket. The bucket should be terminated when not in use to prevent usage charges. 

#This process is run through the terminal. The user **etl.py**.


# CONTENTS

**etl.py** - this file reads the JSON files and performs the steps in the ETL pipeline to populate the tables in the database
**dl.cfg** - this configuration file contains all the attributes necessary to connect to the Amazon resources
