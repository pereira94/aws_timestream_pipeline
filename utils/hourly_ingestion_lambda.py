import urllib3
import json
import boto3

from botocore.config import Config


DATABASE_NAME = 'crypto_test'
TABLE_NAME = 'price'

INTERVAL = 1 # Seconds

def prepare_record(datetime, dimensions, measure_name, measure_value):
    record = {
        'Time': datetime,
        'Dimensions': dimensions,
        'MeasureName': measure_name,
        'MeasureValue': str(measure_value),
        'MeasureValueType': 'DOUBLE'
    }
    return record


def write_records(records):
    try:
        result = write_client.write_records(DatabaseName=DATABASE_NAME,
                                            TableName=TABLE_NAME,
                                            Records=records,
                                            CommonAttributes={})
        status = result['ResponseMetadata']['HTTPStatusCode']
        print("Processed %d records. WriteRecords Status: %s" %
              (len(records), status))
    except Exception as err:
        print("Error:", err)
        
session = boto3.Session()
write_client = session.client('timestream-write', config=Config(read_timeout=20, max_pool_connections=5000, retries={'max_attempts': 10}))


def lambda_handler(event, context):
    
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
    
        datetime = str(data['data'][-1]['time'])
        price = float(data['data'][-1]['priceUsd'])
        circulatingSupply = float(data['data'][-1]['circulatingSupply'])
    
        records.append(prepare_record(datetime, dimensions, 'price', price))
        records.append(prepare_record(datetime, dimensions, 'circulatingSupply', circulatingSupply))

        write_records(records)
