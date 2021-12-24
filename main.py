import utils.initial_setup as initial_setup
import utils.initial_ingestion as initial_ingestion
import boto3
import argparse
import os
import urllib3
import json

from botocore.config import Config

if __name__ == '__main__':


    session = boto3.Session()

    os.environ['DATABASE_NAME'] = 'final_test'
    os.environ['TABLE_NAME'] = 'test_table'
    os.environ['HT_TTL_HOURS'] = '48'
    os.environ['CT_TTL_DAYS'] = '730'

    session = boto3.Session()
    write_client = session.client('timestream-write', config=Config(read_timeout=20, max_pool_connections=5000,
                                                                        retries={'max_attempts': 10}))
    db_instance = initial_setup.DBSetup(write_client)
    db_instance.create_database()
    db_instance.create_table()

    ingestion_instance = initial_ingestion.Initial_Ingestion(write_client)

    coins = ["ethereum", "bitcoin", "cardano", "solana", "avalanche"]

    for coin in coins:
        http = urllib3.PoolManager()
        r = http.request(
                    "GET",
                    f'https://api.coincap.io/v2/assets/{coin}/history?interval=h1')
        data = json.loads(r.data.decode("utf8").replace("'", '"'))

        dimensions = [
            {'Name': 'coin', 'Value': coin},
        ]

        records = []

        for i in range(1,25):
            datetime = str(data['data'][-i]['time'])
            price = float(data['data'][-i]['priceUsd'])
            circulatingSupply = float(data['data'][-i]['circulatingSupply'])
            records.append(ingestion_instance.prepare_record(datetime, dimensions, 'price', price))
            records.append(ingestion_instance.prepare_record(datetime, dimensions, 'circulatingSupply', circulatingSupply))

        ingestion_instance.write_records(records)