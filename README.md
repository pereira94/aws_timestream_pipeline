# CoiCap API to AWS Timestream Pipeline - Visualized with Grafana

## Overview, Context & Motivation

Time series databases such as AWS Timestream offer out of the box more efficient storage and retreival options for time series data. This project was my way of getting some hands on experience with a time series database. The service is completely serverless, autoscaling, and highly-available. 

I built a data pipeline to pull data hourly from the CoinCap API into Timestream. I performed a one time backfill of the data using an offline script, which also created the database resources. 

![alt text](https://github.com/pereira94/aws_timestream_pipeline/blob/main/artifacts/Timestream%20pipeline.drawio.png)

## Implementation 

The utils/ directory contains the helper scripts to spin up resources, backfill data, deploy the lambda function, and a sample query. 

If you want to run it using my default parameters, run the main.py file. Make sure you have the aws cli configured with crednetials. 

### Tools used
- Python
- AWS Lambda
- AWS EC2
- AWS Timestream
- CoinCap API
- Grafana

## Results 

Below is a screenshot of the grafana dashboard deployed on AWS EC2. To avoid wasting unused resources, the instance has been paused. Therefore, you will not be able to reach it using the IP above. If you would like access, let me know, I can set you up with temporary credentials to Grafana. 

![alt text](https://github.com/pereira94/aws_timestream_pipeline/blob/main/artifacts/grafana_viz.png)

Another great feature of this service is that it is integrated with pandas. You can insert and retrieve data, all from pandas. The screenshot below shows a sample query from pandas to the database. 

![alt text](https://github.com/pereira94/aws_timestream_pipeline/blob/main/artifacts/pandas_query.png)




