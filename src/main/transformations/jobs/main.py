import datetime
import os.path

from resources.dev import config
from src.main.read.database_read import *
from src.main.upload.upload_to_s3 import UploadToS3
from src.main.utility.encrypt_decrypt import *
from src.main.utility.s3_client_object import *
from src.main.utility.logging_config import *
from src.main.utility.my_sql_session import *
from src.main.read.aws_read import *
from src.main.download.aws_file_download import *
from src.main.utility.spark_session import *
import shutil
from src.main.move.move_files import *
from src.main.transformations.jobs.dimension_tables_join import *
from src.main.write.parquet_writer import *

############################### GET S3 client ###########################
aws_access_key = config.aws_access_key
# aws_access_key = ""
aws_secret_key = config.aws_secret_key

s3_client_provider = S3ClientProvider(decrypt(aws_access_key), decrypt(aws_secret_key))
s3_client = s3_client_provider.get_client()

# Now you can use s3 client for your s3 operations
response = s3_client.list_buckets()
print(response)
logger.info("list of buckets: %s", response['Buckets'])

# Check if local directory already has the file, if file is present then check if same file is present in the
# staging area with active status, if so thn don't delete the file rather try rerunning, else give an error
# and don't process the next file

csv_files = [file for file in os.listdir(config.local_directory) if file.endswith(".csv")]
connection = get_mysql_connection()
cursor = connection.cursor()
total_csv_file = []
if csv_files:
    for file in csv_files:
        total_csv_file.append(file)
    statement = f"""
    SELECT DISTINCT file_name
    FROM {config.database_name}.{config.product_staging_table}
    WHERE file_name IN ({str(total_csv_file)[1:-1]})
    AND status = "A"
    """
    logger.info(f"Dynamically statement created:{statement}")
    cursor.execute(statement)

    data = cursor.fetchall()
    if data:
        logger.info("Your Last run failed, Please Check")
    else:
        logger.info("NO RECORD MATCH")
else:
    logger.info("Last run was Successful !!")

# SELF NOTE -  abhi tak humne ye karliya hai ki hume ye pata chal jae ki last process fail hua tha ki success
# agar fail hua tha to hume info log karke rakhni hai ki last process fail ho gaya tha, ab iske aage hum dekhenge ki
# S3 se kaise file download karte hai, uske baad uspe transformations lagate hai, uske baad usko hum PARQUET format
# me write karnege, partition me write karenege, waha saari spark knowledge bhi kaam aaegi abhi tak bas
# python chal rahi thi

try:
    s3_reader = S3Reader()
    folder_path = config.s3_source_directory
    s3_absolute_file_path = s3_reader.list_files(s3_client, config.bucket_name, folder_path=folder_path)

    logger.info(f"Absolute path on S3 bucket for csv file %s", s3_absolute_file_path)
    if not s3_absolute_file_path:
        logger.info(f"NO FILES AVAILABLE AT {folder_path}")
        raise Exception("NO DATA AVAILABLE TO PROCESS.")

except Exception as e:
    logger.error("EXITED WITH ERROR :- %s", e)
    raise e

bucket_name = config.bucket_name
local_directory = config.local_directory

prefix = f"s3://{bucket_name}/"
file_paths = [url[len(prefix):] for url in s3_absolute_file_path]

logging.info("File Path available on s3 under %s bucket and folder name is %s", bucket_name, folder_path)
logging.info(f"File Path available on S3 under {bucket_name} bucket and folder name is {file_paths}")

try:
    downloader = S3FileDownloader(s3_client, bucket_name, local_directory)
    downloader.download_files(file_paths)
except Exception as e:
    logger.error("File download error %s", e)
    sys.exit()

all_files = os.listdir(local_directory)

# filtering out only .csv files and creating absolute path
if all_files:
    csv_file = []
    error_file = []
    for files in all_files:
        if files.endswith(".csv"):
            csv_file.append(os.path.abspath(os.path.join(local_directory, files)))
        else:
            error_file.append(os.path.abspath(os.path.join(local_directory, files)))
    if not csv_file:
        logger.error("No CSV data available to process the Request")
        raise Exception("No CSV data available to process the Request")
else:
    logger.error("There is no data to process")
    raise Exception("There is no data to process")

logger.info("*************** LISTING THE FILES **************")
logger.info("List of CSV files that need to be processed %s", csv_file)
logger.info("*************** CREATING SPARK SESSION ***************")
spark = spark_session()
logger.info("*************** SPARK SESSION CREATED ***************")

# Check the required columns in schema of CSV files. If not required keep it in a list or move it to error files
# Else union all correct data into one data frame

correct_files = []
for data in csv_file:
    data_schema = spark.read.format("csv").option("header", True).load(data).columns
    logger.info(f"Schema for the {data} is {data_schema}")
    logger.info(f"Mandatory column schema is {config.mandatory_columns}")
    missing_columns = set(config.mandatory_columns) - set(data_schema)
    logger.info(f"Missing Columns are {missing_columns}")

    if missing_columns:
        error_file.append(data)
    else:
        logger.info(f"No missing columns for the {data}")
        correct_files.append(data)
logger.info(f"****** List of Correct Files ****** {correct_files}")
logger.info(f"****** List of error Files ****** {error_file}")
logger.info(f"****** Moving Error data to error directory, IF ANY ****** ")
# Move data to error directory on local
Error_folder_local_path = config.error_folder_path_local

if error_file:
    for file_path in error_file:
        if os.path.exists(file_path):
            file_name = os.path.basename(file_path)
            destination_path = os.path.join(Error_folder_local_path, file_path)
            shutil.move(file_path, destination_path)
            logger.info(f"Moved '{file_path}' from S3 file path to '{destination_path}'")

            source_prefix = config.s3_source_directory
            destination_prefix = config.s3_error_directory
            message = move_s3_to_s3(s3_client, config.bucket_name, source_prefix, destination_prefix, file_name)
            logger.info(f"{message}")
        else:
            logger.error(f"'{file_path}' Does Not Exist")
else:
    logger.info("****** THERE ARE NO ERROR FILES AVAILABLE IN OUR DATASET ******")

# Need to take care of additional columns
# first determine extra columns
# but before running the process staging table needs to be updated as active(A) or inactive(I)

logger.info(f"****** Updating product_staging_table, that we have begin the process ******")
insert_statements = []
db_name = config.database_name
current_date = datetime.datetime.now()
formatted_date = current_date.strftime("%Y-%m-%d %H:%M:%S")

if correct_files:
    for file in correct_files:
        filename = os.path.basename(file)
        statements = f" INSERT INTO {db_name}.{config.product_staging_table} " \
                     f"(file_name,file_location, created_date, status)" \
                     f" VALUES ('{filename}','{filename}','{formatted_date}','A')"
        insert_statements.append(statements)
    logger.info(f"Insert Statement created for staging table --- {insert_statements}")
    logger.info("****** CONNECTING TO MYSQL SERVER ******")
    connection = get_mysql_connection()
    cursor = connection.cursor()
    logger.info("****** MYSQL CONNECTION SUCCESSFUL ******")
    for statement in insert_statements:
        cursor.execute(statement)
        connection.commit()
    cursor.close()
else:
    logger.error("****** THERE ARE NO FILES TO PROCESS ******")
    raise Exception("****** NO DATA AVAILABLE IN CORRECT FILES ******")

logger.info("********  STAGING TABLE UPDATED SUCCESSFULLY ********")

logger.info("******** FIXING EXTRA COLUMNS COMING FROM SOURCE *******")
schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("store_id", IntegerType(), True),
    StructField("product_name", StringType(), True),
    StructField("sales_date", DateType(), True),
    StructField("sales_person_id", IntegerType(), True),
    StructField("price", FloatType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("total_cost", FloatType(), True),
    StructField("additional_column", StringType(), True)
])
# Connecting with DatabaseReader
# database_client = DatabaseReader(config.url, config.properties)
# logger.info("****** CREATING EMPTY DATAFRAME ******")
# final_df_to_process = database_client.create_dataframe(spark, "empty_df_create_table")
# final_df_to_process.show()
final_df_to_process = spark.createDataFrame([], schema=schema)

# CREATE A NEW COLUMN WITH CONCATENATED VALUES OF EXTRA COLUMNS
for data in correct_files:
    data_df = spark.read.format("csv").option("header", True).option("inferschema", True).load(data)
    data_schema = data_df.columns
    extra_columns = list(set(data_schema) - set(config.mandatory_columns))
    logger.info(f"Extra Columns present at source are {extra_columns}")

    if extra_columns:
        data_df = data_df.withColumn("additional_column", concat_ws(",", *extra_columns))\
            .select("customer_id", "store_id", "product_name", "sales_date", "sales_person_id",
                    "price", "quantity", "total_cost", "additional_column")
        logger.info(f"Processed {data} and Added 'additional_column'")
    else:
        data_df = data_df.withColumn("additional_column", lit(None))\
            .select("customer_id", "store_id", "product_name", "sales_date", "sales_person_id",
                    "price", "quantity", "total_cost", "additional_column")
    final_df_to_process = final_df_to_process.union(data_df)
logger.info("******  FINAL DATAFRAME FROM SOURCE WHICH WILL BE PROCESSED ******")
final_df_to_process.show()
final_df_to_process.printSchema()

# Enrich data using dimension tables and create a data mart for sales_team containing their incentives, addresses and
# all other details
# and another datamart for customers who made purchases, bifurcated by each day of month, for every month, separate
# files segregated by store_id
# Read data from PARQUET and generate CSV file containing sales_person_name, sales_person_store_id,
# sales_person_total_billing_done_for_each_month and total_incentives

# Connecting with DatabaseReader
database_client = DatabaseReader(config.url, config.properties)

# Creating Dataframe for all tables

# customer table
logger.info("****** LOADING customer_table INTO customer_table_df ******")
customer_table_df = database_client.create_dataframe(spark, config.customer_table_name)

# product table
logger.info("****** LOADING product_table INTO product_table_df ******")
product_table_df = database_client.create_dataframe(spark, config.product_table)

# product staging table
logger.info("****** LOADING product_staging_table INTO product_staging_table_df ******")
product_staging_table_df = database_client.create_dataframe(spark, config.product_staging_table)

# sales team table
logger.info("****** LOADING sales_team_table INTO sales_team_table_df ******")
sales_team_table_df = database_client.create_dataframe(spark, config.sales_team_table)

# store table
logger.info("****** LOADING store_table INTO store_table_df ******")
store_table_df = database_client.create_dataframe(spark, config.store_table)

s3_customer_store_sales_df_join = dimesions_table_join(final_df_to_process, customer_table_df,
                                                       store_table_df, sales_team_table_df)

logger.info("****** FINAL ENRICHED DATA ******")
s3_customer_store_sales_df_join.show()

# Write customer data into customer data mart in PARQUET FILE FORMAT
# File will be wriiten on local first and then the raw data will be moved to s3 for reporting tool
# write reporting data to MYSQL table too

final_customer_data_mart_df = s3_customer_store_sales_df_join\
    .select("ct.customer_id", "ct.first_name", "ct.last_name", "ct.address", "ct.pincode",
            "phone_number", "sales_date", "total_cost")

logger.info("****** FINAL DATA FOR CUSTOMER DATA MART ******")
final_customer_data_mart_df.show()

parquet_writer = ParquetWriter("overwrite", "parquet")

parquet_writer.dataframe_writer(final_customer_data_mart_df,
                                config.customer_data_mart_local_file)

logger.info(f"****** CUSTOMER DATA WRITTEN ON LOCAL DISK AT {config.customer_data_mart_local_file}")

# Move Data on S3 Bucket for customer_data_mart
logger.info("****** DATA MOVEMENT FROM LOCAL TO S3 FOR CUSTOMER DATA MART ******")
s3_uploader = UploadToS3(s3_client)
s3_directory = config.s3_customer_datamart_directory
message = s3_uploader.upload_to_s3(s3_directory, config.bucket_name, config.customer_data_mart_local_file)
logger.info(f"{message}")
#
# # Sales_team_data_mart
# logger.info("****** WRITE DATA INTO SALES TEAM DATA MART ******")
# final_sales_team_data_mart_df = s3_customer_store_sales_df_join\
#     .select("store_id", "sales_person_id", "sales_person_first_name", "sales_person_last_name",
#             "store_manager_name", "manager_id", "is_manager", "sales_person_address", "sales_person_pincode",
#             "sales_date", "total_cost", expr("SUBSTRING(sales_date,1,7) as sales_month"))
#
# logger.info("****** FINAL DATA FOR SALES TEAM DATA MART ******")
# final_sales_team_data_mart_df.show()
#
# parquet_writer.dataframe_writer(final_sales_team_data_mart_df, config.sales_team_data_mart_local_file)
#
# logger.info(f"****** CUSTOMER DATA WRITTEN ON LOCAL DISK AT {config.sales_team_data_mart_local_file}")
#
# # Move Data on S3 Bucket for sales_data_mart
# logger.info("****** DATA MOVEMENT FROM LOCAL TO S3 FOR SALES DATA MART ******")
# s3_directory = config.s3_sales_datamart_directory
# message = s3_uploader.upload_to_s3(s3_directory, config.bucket_name, config.sales_team_data_mart_local_file)
# logger.info(f"{message}")
