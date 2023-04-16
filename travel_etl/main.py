"""
DESCRIPTION:

This is main program file for the project. The main program calls all the functions
that are required for the transformations in this ETL process.
It involves the following steps:
    1. Importing all the necessary packages and libraries
    2. Create a Spark Session
    3. Load the source file
    4. Calling the functions to perform transformations
    5. Load the final output to MySQL database using JDBC
    6. Stop Spark Session

NOTE: For security purpose all the credentials, filepaths etc. are stored in .env file
"""

# import statements
import os

import findspark
findspark.init()

from pyspark.sql import SparkSession

from functions.required_functions import process_date_columns, clean_pax_names, clean_airline_names,\
                                capitalize_first_letter, correct_flight_names

from dotenv import load_dotenv
load_dotenv()

# create spark session
def create_spark_session(app_name, master):

    spark = SparkSession.builder \
            .appName(app_name) \
            .master(master) \
            .config('spark.jars', 'mysql-connector-j-8.0.32.jar') \
            .getOrCreate()
    return spark


if __name__ == '__main__':

    spark = create_spark_session('ABG_CaseStudy', 'local[1]')

    # MySQL environmental variables
    my_sql_user = os.environ.get('MYSQL_USER')
    my_sql_password = os.environ.get('MYSQL_PASSWORD')
    table_name = os.environ.get('TABLE_NAME')

    # Source File Path, Driver, URL of the database to connect
    file_path = os.environ.get('FILE_PATH')
    driver = os.environ.get('DRIVER')
    url = os.environ.get('URL')

    # Loading source data to a DataFrame
    source_data = spark.read.csv(file_path, header=True, inferSchema=True)

    # Transforming Booking Date column into 'dd-MM-yyyy'
    output_booking_date_columns = process_date_columns(source_data, 'Booking Date')

    # Transforming Travel Date column into 'dd-MM-yyyy'
    output_travel_date_columns = process_date_columns(output_booking_date_columns, 'Travel Date')

    # Removing prefixes in Pax Name column
    output_clean_pax_names = clean_pax_names(output_travel_date_columns, 'Pax Name')

    # Removing repetitive airline names from Airline column
    output_clean_airline_names = clean_airline_names(output_clean_pax_names, 'Airline')

    # Capitalizing the first Letter of a value in each column
    output_capitalize_first_letter = capitalize_first_letter(output_clean_airline_names)

    # Checking the correctness of value in Airline column
    final_output = correct_flight_names(output_capitalize_first_letter, 'Airline')

    # Writing the output to MySQL
    try:
        final_output.write \
                    .format("jdbc") \
                    .option("driver", driver) \
                    .option("url", url) \
                    .option("dbtable", table_name) \
                    .option("user", my_sql_user) \
                    .option("password", my_sql_password) \
                    .save()
    except Exception as e:
        print("The error occurred is:", e)

    # Stopping Spark Session
    spark.stop()
