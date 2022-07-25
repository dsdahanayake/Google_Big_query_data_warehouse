"""
Developer: Darshana Dhanayake
Date: 2021/09/04
Google cloud platform BigQuery ETL tools using GCP API call
"""
import csv
import random
from datetime import timedelta, date
from google.cloud import bigquery

client = bigquery.Client()

start_date = date(2016, 1, 1)
end_date = date(2017, 12, 31)

randfile = open('prod_id.csv', 'w')
writer = csv.writer(randfile, delimiter=',', lineterminator='\n')
header = 'product_id', 'purchase_date'
writer.writerow(header)
order_csv = 'orders.csv'


def orderTransform(csvfile):
    with open(csvfile, 'r') as fin, open('new_' + csvfile, 'w') as fout:
        reader = csv.reader(fin, delimiter=',', lineterminator='\n')
        writer = csv.writer(fout, delimiter=',', lineterminator='\n')
        new_headers = ['product_id', 'purchase_date']
        writer.writerow(next(reader) + new_headers)  # add header product code and purchase date to CSV
        for row in reader:
            pair = [random.randint(1, 49688), createDate(start_date, end_date)]
            writer.writerow(row + pair)


def createDate(start, end):
    time_between_dates = end - start
    days_between_dates = time_between_dates.days
    random_number_of_days = random.randrange(days_between_dates)
    random_date = start + timedelta(days=random_number_of_days)
    return random_date


def csv_injection(rangeint, fileobj):
    for i in range(rangeint):
        pair = random.randint(1, 49688), createDate(start_date, end_date)
        fileobj.writerow(pair)


def loadingDataToTables(table_id, file_path):  # ETL Method to load data to tables
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV, skip_leading_rows=1, autodetect=True,
    )

    with open(file_path, "rb") as source_file:
        job = client.load_table_from_file(source_file, table_id, job_config=job_config)

    job.result()

    table = client.get_table(table_id)  # Make an API request.
    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), table_id
        )
    )


def creatFactTable():
    # name =tableName
    query_job = client.query(
        """
        CREATE TABLE `mda-data-warehouse-324819.mda_demo.NewFact_table1` (
        OrderID INT64 NOT NULL,
        CustomerID INT64 NOT NULL,
        ProductID INT64 NOT NULL,        
        )       
        """
    )

    results = query_job.result()  # Waits for job to complete.
    print("Table created")
    table_id = 'mda-data-warehouse-324819.mda_demo.NewFact_table1'

    table = client.get_table(table_id)  # Make an API request.

    # View table properties
    print(
        "Got table '{}.{}.{}'.".format(table.project, table.dataset_id, table.table_id)
    )
    print("Table schema: {}".format(table.schema))
    print("Table description: {}".format(table.description))
    print("Table has {} rows".format(table.num_rows))


def queryDim_aisle():
    table_id = 'mda-data-warehouse-324819.mda_demo.Dim_aisle'
    table = client.get_table(table_id)
    query_job = client.query(
        """
        SELECT *       
        FROM `mda-data-warehouse-324819.mda_demo.Dim_aisle`
        """
    )

    results = query_job.result()  # Waits for job to complete.

    for row in results:
        print("{}.{}.{}".format(row.aisle_SK, row.aisle_id, row.aisle))


def tableInfo():
    table_id = 'mda-data-warehouse-324819.mda_demo.Dim_aisle'

    table = client.get_table(table_id)  # Make an API request.

    # View table properties
    print(
        "Got table '{}.{}.{}'.".format(table.project, table.dataset_id, table.table_id)
    )
    print("Table schema: {}".format(table.schema))
    print("Table description: {}".format(table.description))
    print("Table has {} rows".format(table.num_rows))


    results = query_job.result()  # Waits for job to complete.

    for row in results:
        print("{} : {} views".format(row.url, row.view_count))


def implicit():
    storage_client = storage.Client()

    # Make an authenticated API request
    buckets = list(storage_client.list_buckets())
    print(buckets)


if __name__ == '__main__':
    # implicit()    
    # tableInfo()
    # queryDim_aisle()
    # creatFactTable()
    #################### load data #################
    table_id ='mda-data-warehouse-324819.mda_demo.Dim_departments'
    # file_path ='\assets\departments.csv'
    loadingDataToTables(table_id, 'departments.csv')
    ####################data ingestion and transformation #################
    # rangei = 100
    # csv_injection(rangei, writer)
    # randfile.close()
    # orderTransform('orders_1.csv')
