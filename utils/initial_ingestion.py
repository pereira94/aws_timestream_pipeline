import os

from botocore.config import Config

class Initial_Ingestion():
    def __init__(self, client):
        self.client = client

    def prepare_record(self, datetime, dimensions, measure_name, measure_value):
        record = {
            'Time': datetime,
            'Dimensions': dimensions,
            'MeasureName': measure_name,
            'MeasureValue': str(measure_value),
            'MeasureValueType': 'DOUBLE'
        }
        return record


    def write_records(self, records):
        try:
            result = self.client.write_records(DatabaseName=os.environ['DATABASE_NAME'],
                                                TableName=os.environ['TABLE_NAME'],
                                                Records=records,
                                                CommonAttributes={})
            status = result['ResponseMetadata']['HTTPStatusCode']
            print("Processed %d records. WriteRecords Status: %s" %
                (len(records), status))
        except Exception as err:
            print("RejectedRecords: ", err)
            for rr in err.response["RejectedRecords"]:
                print("Rejected Index " + str(rr["RecordIndex"]) + ": " + rr["Reason"])
            print("Other records were written successfully. ")






