# CoiCap API to AWS Timestream Pipeline - Visualized with Grafana

## Overview, Context & Motivation

Time series databases such as AWS Timestream offer out of the box more efficient storage and retreival options for time series data. This project was my way of getting some hands on experience with a time series database. The service is completely serverless, autoscaling, and highly-available. 

I built a data pipeline to pull data hourly from the CoinCap API into Timestream. I performed a one time backfill of the data using an offline script, which also created the database resources. 



## Implementation 


