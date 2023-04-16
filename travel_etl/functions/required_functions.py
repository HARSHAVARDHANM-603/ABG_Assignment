# import pickle to read and write to pickle file
import pickle

# import pyspark functions
from pyspark.sql.functions import when, col, to_date, date_format, upper,\
                                  trim, regexp_replace, lower, regexp_extract,\
                                  concat, expr, udf

# import pyspark data types
from pyspark.sql.types import StringType


'''
PROBLEM STATEMENT1 01:
Columns in the format ('dd-MM-yyyy', dd-MM-yyyy HH:mm, dd-MM-yyyy HH:mm:ss) had been
converted to the format 'dd-MM-yyyy' with the help of regular expressions,
to_date() and data_format().
'''

def process_date_columns(source_data, col_name):
    uniform_date = source_data.withColumn(col_name,
                                  when(col(col_name).rlike(r"\d{2}-\d{2}-\d{4}\s\d{2}:\d{2}:\d{2}"),
                                    to_date(col(col_name), "dd-MM-yyyy HH:mm:ss"))
                                  .when(col(col_name).rlike(r"\d{2}-\d{2}-\d{4}\s\d{2}:\d{2}"),
                                    to_date(col(col_name), "dd-MM-yyyy HH:mm"))
                                  .otherwise(to_date(col(col_name), "dd-MM-yyyy")))\
                                  .withColumn(col_name, date_format(col(col_name), "dd-MM-yyyy"))
    return uniform_date


'''
PROBLEM STATEMENT 02:
Removed prefixes in Pax Name column using regular expression and trimming
additional spaces using trim().
'''

def clean_pax_names(uniform_date, col_name):
    pax_name_clean = uniform_date.withColumn(col_name,
                                  upper(trim(regexp_replace(lower(col(col_name)), r"^(mrs\.|mr\.)\s*", ""))))
    return pax_name_clean


'''
PROBLEM STATEMENT 03:
Converted repetitive Airline names (Eg: Indigo/Indigo) to proper format
(Eg: Indigo) using regular expressions.
'''

def clean_airline_names(pax_name_clean, col_name):
    airline_data_repetition = pax_name_clean.withColumn(col_name,
                                  regexp_extract(col(col_name), r"^([^/]+)", 1))
    return airline_data_repetition


'''
PROBLEM STATEMENT 04:
Capitalized the values of all the columns using concat(), lower(), upper()
'''

def capitalize_first_letter(airline_data_repetition):
    first_letter_caps = airline_data_repetition
    for column in first_letter_caps.columns:
        first_letter_caps = first_letter_caps.withColumn(column,
                                  concat(upper(expr("substring(`{}`, 1, 1)".format(column))),
                                  lower(expr("substring(`{}`, 2)".format(column)))))
    return first_letter_caps


'''
PROBLEM STATEMENT 05:
Created an UDF(User DefineD Function) to correct the spelling mistakes in 
the Airline column. In this we calculate the percentage of similarity between 
the correct flight names which are loaded from flights.pkl file in flight_data 
directory with the value of the column and if it greater than 60%, we correct 
the name and else we considered it be a unique name and append it to correct 
flight names and write this updated list into the pickle file.
'''

# writing updated flights list into a pickle file

def write_flights_pickle(updated_list):
    try:
        with open(r".\functions\flight_data\flights.pkl", 'wb') as file:
            pickle.dump(updated_list, file)
    except Exception as e:
        print("The error is:", e)


# reading flights list from a pickle file

def read_flights_pickle():
    try:
        with open(r".\functions\flight_data\flights.pkl", 'rb') as file:
            correct_flight = pickle.load(file)
        return correct_flight
    except Exception as e:
        print("The error is:", e)


# udf function to find correctness of the flight name

def my_udf_func(col_value):

    correct_flights = read_flights_pickle()

    if col_value in correct_flights:
        return col_value

    maximum_percentage = 0
    output_value = ""
    len_col_value = len(col_value)

    for flight in correct_flights:
        len_flight = len(flight)

        if len_col_value > len_flight:
            len_diff = len_col_value - len_flight
            longer_word = col_value
            shorter_word = flight
        else:
            len_diff = len_flight - len_col_value
            longer_word = flight
            shorter_word = col_value

        match_count = sum(1 for index, charac in enumerate(shorter_word) if charac == longer_word[index])
        match_percentage = (match_count + len_diff) / len(longer_word) * 100

        if match_percentage > maximum_percentage:
            maximum_percentage = match_percentage
            output_value = flight

    if maximum_percentage > 60:
        return output_value
    else:
        correct_flights.append(col_value)
        write_flights_pickle(correct_flights)
        return col_value


def correct_flight_names(first_letter_caps, col_name):
    convert_udf = udf(lambda val: my_udf_func(val), StringType())
    flight_name_correction = first_letter_caps.withColumn(col_name, convert_udf(col_name))
    final_output = flight_name_correction
    return final_output
