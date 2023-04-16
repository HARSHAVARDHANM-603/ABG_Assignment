**ETL ON TRAVEL DATASET:** <br>
This project contains a set of required functions to perform transformations on Travel Dataset.
The 'main.py' is the main program that uses functions from 'required_functions.py' from 'functions'
directory to perform transformations on the dataset provided in 'source_data' directory.

**SETUP** <br>
To run this project I used Python 3.10, Java 19.0, Spark 3.3.2 and PyCharm 2023.1 version.

**INSTALLATION** <br>
1. Firstly clone the repository or download the files
2. Create a virtual environment using the following command in command prompt: <br> 
   python -m venv ABG_CaseStudy
3. Activate the Environment using the following command in command prompt:<br>
   ABG_CaseStudy/Scripts/activate
4. Install the 'requirement.txt' file using the following command in command prompt:<br>
   pip install -r requirements.txt
5. Set the proper values for the environmental variables defined with upper case words in the 'main.py' file

**RUN**<br>
Open 'travel_etl' directory in command prompt and run the following commands: 
1. with python: <br>
   python main.py
2. with spark submit: <br>
   spark-submit --master local[*] --jars <jar_file_path> <main.py_file_path>
   
   