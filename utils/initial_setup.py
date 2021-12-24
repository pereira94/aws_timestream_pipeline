import os


class DBSetup:
    def __init__(self, client):
        self.client = client

    def create_database(self):
        print("Creating Database")
        try:
            self.client.create_database(DatabaseName=os.environ['DATABASE_NAME'])
            print("Database [%s] created successfully." % os.environ['DATABASE_NAME'])
        except self.client.exceptions.ConflictException:
            print("Database [%s] exists. Skipping database creation" % os.environ['DATABASE_NAME'])
        except Exception as err:
            print("Create database failed:", err)

    def describe_database(self):
        print("Describing database")
        try:
            result = self.client.describe_database(DatabaseName=os.environ['DATABASE_NAME'])
            print("Database [%s] has id [%s]" % (os.environ['DATABASE_NAME'], result['Database']['Arn']))
        except self.client.exceptions.ResourceNotFoundException:
            print("Database doesn't exist")
        except Exception as err:
            print("Describe database failed:", err)

    def update_database_retention(self, kms_id):
        print("Updating database")
        try:
            result = self.client.update_database(DatabaseName=os.environ['DATABASE_NAME'], KmsKeyId=kms_id)
            print("Database [%s] was updated to use kms [%s] successfully" % (os.environ['DATABASE_NAME'],
                                                                              result['Database']['KmsKeyId']))
        except self.client.exceptions.ResourceNotFoundException:
            print("Database doesn't exist")
        except Exception as err:
            print("Update database failed:", err)

    def list_databases(self):
        print("Listing databases")
        try:
            result = self.client.list_databases(MaxResults=5)
            self._print_databases(result['Databases'])
            next_token = result.get('NextToken', None)
            while next_token:
                result = self.client.list_databases(NextToken=next_token, MaxResults=5)
                self._print_databases(result['Databases'])
                next_token = result.get('NextToken', None)
        except Exception as err:
            print("List databases failed:", err)

    def create_table(self):
        print("Creating table")
        retention_properties = {
            'MemoryStoreRetentionPeriodInHours': int(os.environ['HT_TTL_HOURS']),
            'MagneticStoreRetentionPeriodInDays': int(os.environ['CT_TTL_DAYS'])
        }
        try:
            self.client.create_table(DatabaseName=os.environ['DATABASE_NAME'], TableName=os.environ['TABLE_NAME'],
                                     RetentionProperties=retention_properties)
            print("Table [%s] successfully created." % os.environ['TABLE_NAME'])
        except self.client.exceptions.ConflictException:
            print("Table [%s] exists on database [%s]. Skipping table creation" % (
                os.environ['TABLE_NAME'], os.environ['DATABASE_NAME']))
        except Exception as err:
            print("Create table failed:", err)

    def update_table_retention(self):
        print("Updating table")
        retention_properties = {
            'MemoryStoreRetentionPeriodInHours': os.environ['HT_TTL_HOURS'],
            'MagneticStoreRetentionPeriodInDays': os.environ['CT_TTL_DAYS']
        }
        try:
            self.client.update_table(DatabaseName=os.environ['DATABASE_NAME'], TableName=os.environ['TABLE_NAME'],
                                     RetentionProperties=retention_properties)
            print("Table updated.")
        except Exception as err:
            print("Update table failed:", err)

    def describe_table(self):
        print("Describing table")
        try:
            result = self.client.describe_table(DatabaseName=os.environ['DATABASE_NAME'], TableName=os.environ['TABLE_NAME'])
            print("Table [%s] has id [%s]" % (os.environ['TABLE_NAME'], result['Table']['Arn']))
        except self.client.exceptions.ResourceNotFoundException:
            print("Table doesn't exist")
        except Exception as err:
            print("Describe table failed:", err)

    def list_tables(self):
        print("Listing tables")
        try:
            result = self.client.list_tables(DatabaseName=os.environ['DATABASE_NAME'], MaxResults=5)
            self.__print_tables(result['Tables'])
            next_token = result.get('NextToken', None)
            while next_token:
                result = self.client.list_tables(DatabaseName=os.environ['DATABASE_NAME'],
                                                 NextToken=next_token, MaxResults=5)
                self.__print_tables(result['Tables'])
                next_token = result.get('NextToken', None)
        except Exception as err:
            print("List tables failed:", err)

    def write_records(self):
        print("Writing records")
        current_time = self._current_milli_time()

        dimensions = [
            {'Name': 'region', 'Value': 'us-east-1'},
            {'Name': 'az', 'Value': 'az1'},
            {'Name': 'hostname', 'Value': 'host1'}
        ]

        cpu_utilization = {
            'Dimensions': dimensions,
            'MeasureName': 'cpu_utilization',
            'MeasureValue': '13.5',
            'MeasureValueType': 'DOUBLE',
            'Time': current_time
        }

        memory_utilization = {
            'Dimensions': dimensions,
            'MeasureName': 'memory_utilization',
            'MeasureValue': '40',
            'MeasureValueType': 'DOUBLE',
            'Time': current_time
        }

        records = [cpu_utilization, memory_utilization]

        try:
            result = self.client.write_records(DatabaseName=os.environ['DATABASE_NAME'], TableName=os.environ['TABLE_NAME'],
                                               Records=records, CommonAttributes={})
            print("WriteRecords Status: [%s]" % result['ResponseMetadata']['HTTPStatusCode'])
        except self.client.exceptions.RejectedRecordsException as err:
            self._print_rejected_records_exceptions(err)
        except Exception as err:
            print("Error:", err)

    def delete_table(self):
        print("Deleting Table")
        try:
            result = self.client.delete_table(DatabaseName=os.environ['DATABASE_NAME'], TableName=os.environ['TABLE_NAME'])
            print("Delete table status [%s]" % result['ResponseMetadata']['HTTPStatusCode'])
        except self.client.exceptions.ResourceNotFoundException:
            print("Table [%s] doesn't exist" % os.environ['TABLE_NAME'])
        except Exception as err:
            print("Delete table failed:", err)

    def delete_database(self):
        print("Deleting Database")
        try:
            result = self.client.delete_database(DatabaseName=os.environ['DATABASE_NAME'])
            print("Delete database status [%s]" % result['ResponseMetadata']['HTTPStatusCode'])
        except self.client.exceptions.ResourceNotFoundException:
            print("database [%s] doesn't exist" % os.environ['DATABASE_NAME'])
        except Exception as err:
            print("Delete database failed:", err)

