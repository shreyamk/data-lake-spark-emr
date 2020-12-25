## Summary of Project

This project is for the purpose of creating a data lake for 'Sparkify' data - artists, songs, song play, users etc. The input files are read from an S3 bucket, which is processed in Spark and then stored as output files in another S3 bucket. Spark allows parallel processing and easier read of unstructured data - in our case which are in the form of JSON files in a nested directory. The output files are written in Parquet format, with partition keys.

## Files included

- dl.cfg has details of AWS configuration (Access Id and keys)
- etl.py is the main file which contains code for read and write into S3 buckets using Spark.

## Instructions for running the project

To run this project, enter your AWS credentials into dl.cfg.
Enter the S3 bucket file paths in input_data and output_data variables, and run the etl.py script.
