# Yasodharapura –– Instructions and Notes
This repository contains Team Yasodharapura's code for the Data Pipeline Knowledge Transfer Project (36-614 &amp; 36-611).

To create the database, run the 2 code cells in order in the "yaso_schema_final.ipynb". The first code cell deletes the tables in the database if they do not exist, and the second code cell creates the tables. 

There are two Python code scripts for loading the necessary data into our designed tables (we are working with the College Scorecard data.) The first Python script, named "load-ipeds.py", contains code for loading data from the Integrated Postsecondary Education Data System (IPEDS); the second script, named "load-scorecard.py", contains code for loading data from the College Scorecard data files.

Make sure to also download the appropriate CSV files containing the data to be loaded from the apt sources (U.S. DOE). They must be renamed such that College Scorecard data follows the format "col_scor_20XX_XX__PP.csv", and the IPEDS data follows the format "ipeds20XX.csv". 

Before running either of these two Python scripts to load to the database, make sure to change the database name, username, and password of choice in the "credentials_proj.py" file, as both scripts use these information to access the database where the designed tables would be stored. Without changing the database, username, and/or password, the scripts would not be able to load the data to the correct database, so make sure that this is done before running them.

Afterward, to run either script, open the command line shell and change the working directory to the one with the CSV file(s) containing the relevant data. Then, type "python" in the command line, followed by the name of the Python script to run, followed by the name of the data file to be loaded. (Each item in the command line should be separated by a space.) Once this is done, the data from the CSV file should be successfully loaded into various tables, which would be stored in the database specified by the aforementioned credentials.

The two scripts utilize a few helper functions in order to complete their respective tasks, located in the "loading_helper_functions.py" file. The first function, named "ingest_data", reads in a dataset and dictionary, and handles any missing data in the dataset. The second function, named "conn_cur", creates a connection and cursor for the SQL server where the resulting tables can be stored. The third function, named "load_small_table_scorecard", loads the corresponding information from the College Scorecard data into a small table (i.e. one with a primary key and another variable). The fourth function, named "load_small_table_ipeds", loads the corresponding information from the IPEDS data into a small table.

The scripts can be run in any order. For the "institutions_static" table, if another data file containing an institution whose unique identifier (UNITID) already exists in "institutions_static", the information will be overwritten. Thus, "institutions_static" will always contain information from the most recently loaded in IPEDS CSV file by "load-ipeds.py". Every time "load-scorecard.py" is ran with a new CSV file, a new row for a school corresponding to the new year associated with that CSV is added. The program is structured such that, if a College Scorecard CSV file is accidentally run again, there will be no duplicates. 

Some variable names in the institutions_static table might be ambiguous, namely the Carnegie Classfication variable names. A more elaborate description of what those variables represent can be found in the "ipeds_dict.xlsx" file under the "Description" tab. 

Finally, note that schools in the College Scorecard data are only labeled by the agencies by which they were accredited starting in the 2021-2022 school year, meaning that the "accredited agency" variable would consist of null values in the preceding years.
