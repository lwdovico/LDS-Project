#!/usr/bin/env python
# coding: utf-8

## PREPROCESSING FUNCTIONS
# As external library we used only ***re*** to split according to a regular expression pattern and ***pycountry_convert*** to fetch the countries and continents


import re
import pycountry_convert as pc


#### Importing the Dataset


#function to import the csv file into python
def import_csv(file_name):
    
    with open(file_name) as f:
        ds = f.readlines()
        
    return ds


#### Adjusting the rows


#function to clean and split a string from the csv format to a list of columns
def adjust_row(string):
    #splitting elements in the list when encountering a comma or a group delimited by quotes
    splitted_string = re.split(r',|"([^"]*)"', string.rstrip('\n'))
    
    #I have some None elements because I matched two expression and the second (within quotes) return mostly None
    #when it doesn't return None it returns empty string delimiting the match (because I am splitting)
    #thus I filter None and remove all this stuff
    filtered_row = list(filter(None, splitted_string))
    
    return filtered_row


#### Getting a clean list from the CSV


#function to get a list of lists from a csv dataset
def list_from_csv(ds, header):
    new_rows = list()

    for row in ds[1:]:
        #clean the row
        adjusted_row = adjust_row(row)
        #check if the columns per row corresponds to the header's columns
        assert len(adjusted_row) == len(header), f'{adjusted_row}\n\nThe length of the row must be equal to the length of the header!'

        new_rows.append(adjusted_row)
    return new_rows


#### Function to import and process everything


#wrapping the functions above to read the csv properly, it returns the header and the rows of the dataset
def read_csv(file_name):
    ds = import_csv(file_name)
    header = adjust_row(ds[0])
    fixed_rows = list_from_csv(ds, header)
    return header, fixed_rows


#### Creating the column index from the header


#function to get the index of the columns
def header_idx(header):
    #to retrieve the index of the column from its name
    header_dict = dict([(feat, n) for n, feat in enumerate(header)])
    return header_dict


#### List the column values from the index of the column


#function to extract a single column from a list of lists by inputting the index
def extract_col(rows, idx):
    return [row[idx] for row in rows]


#### Get the DataFrame into a Dictionary format


#function to get a dictionary with keys the column name in the header
#and as values the correspondeing columns (in a list format)

def dict_from_header(header, rows_ds):
    header_dict = header_idx(header) #get index from the header
    dict_columns = dict()

    #for every column get its index and extract its rows
    for key in header_dict.keys():
        idx = header_dict[key] #get index of col
        dict_columns[key] = extract_col(rows_ds, idx) #extract column's values
        
    return dict_columns



#final wrapper to import and preprocess the csv directly into a dictionary
#it can also return the header if needed

def preprocess_csv_to_dict(file_name, get_header = False):
    header, new_rows = read_csv(file_name) #preprocess and read the file
    dict_columns = dict_from_header(header, new_rows) #getting the proper dataframe
    
    if get_header == True:
        header_dict = header_idx(header) #to look at the index of the columns
        return dict_columns, header_dict
    
    return dict_columns


#### Create Tables


#input the dataset in the dictionary format and the columns for which a table is requested
#multiple coluns are accepted, in order to allow creating tables with multiple columns

def unify_rows(dict_columns, *columns_to_merge):
    merging = list()
    
    #for every column to merge
    for col in columns_to_merge:
        row_of_col = dict_columns[col] #get the rows
        merging.append(row_of_col) #append the rows to a list
    
    return zip(*merging) #zip the list of lists



#to create a table from multiple columns
def gen_table(dict_columns, *columns):
    table_records = list(unify_rows(dict_columns, *columns)) #get the table of records
    return [columns] + table_records #returns the full table with the header and the records

#it is the same as before but it accepts a list of columns instead of multiple column parameters
def gen_table_from_list(dict_columns, columns):
    table_records = list(unify_rows(dict_columns, *columns))
    return [columns] + table_records

#it gets the table of distinct elements and it returns it sorted
def gen_table_distinct(dict_columns, *columns):
    #here a set is created, then it is made as a list in order to sort it
    table_records = sorted(list(set(unify_rows(dict_columns, *columns))))
    #finally the header is added
    return [columns] + table_records


#### Create Primary Key Table


#input the table and the primary key column's name
#it adds the primary key to the table as its first column

def set_primary_key(table, name_primary_key):
    
    #get the primary key column (with header the input name) by ranging across the rows
    id_col = [name_primary_key] + [str(i) for i in range(1, len(table)+1)]
    #zipping the primary key and the previous table, it has to be unzipped though
    table_to_unzip = list(zip(id_col, table))
    #get the number of columns (length of a row)
    len_rows = len(table[0])
    
    #the output
    outres = list()

    #for each row in the table
    for row in table_to_unzip:
        #get the first element (the primary key)
        unzipped = [row[0]]
        
        #loop across the second element with the original columns
        for n in range(len_rows):
            #add the columns to the unzipped table
            unzipped.append(row[1][n])
            
        #add the current row to the output
        outres.append(tuple(unzipped))
        
    return outres


#### Writing the CSV


#it makes the csv to a list of strings (from list of lists)
def make_string_csv(table):
    output = list()
    
    #for each row join the columns
    for row in table:
        row = list(row) #make list from tuple
        output.append(','.join(row)+'\n') #join list of cols and add commas and new line
        
    return output

#input a name of the file and the table to save
def create_csv(name_file, table):
    
    #open the file to write
    file_opened = open(name_file, 'w')

    #write each row
    for row in make_string_csv(table):
        file_opened.write(row)

    #close the file
    file_opened.close()
    
    return


#### Get DataFrame Length


#get the length of the dataframe by extracting and counting elements in the first column
def get_len_df(dict_df):
    return len(dict_df[list(dict_columns.keys())[0]])


#### Mapping Function
# Used to map the primary keys created where the corresponding values appears in other tables


#function to map from a specific table's column mapping sources a destination column
#input are: 
#            the table considered, the column to map and
#            multiple parameters indicating the single columns to map on


#converting the feature to map on by joining the columns into a single string per row
def join_multi_columns(*list_of_lists):
    #if it is a single column it returns it without alteration, otherwise it joins them with a dash in between
    return ['-'.join(list(el)) if type(el) != str else el for el in zip(*list_of_lists)]

def map_values(table, destination_column, *col_mapping_sources):
    
    #get the header of the table in order to have the corresponding indexes
    header_index = header_idx(table[0])
    #getting the list of indexes inserted in the 
    idx_source_columns = [header_index[col] for col in col_mapping_sources]
    
    list_id = [x[0] for x in table[1:]] #getting the ids (conveniently always at the first position)
    
    #getting the stuff to merge on (it must have corresponding values in the destination column)
    #I used the extract col function which is just a loop that extract a column according to a index
    #Then I loop across all the indexes to consider (retrieved by the parameters in col_mapping_sources)
    list_feat_origin = [extract_col(table[1:], idx) for idx in idx_source_columns]
    #If multiple col_mapping_sources: -> list_feat_origin is a list of lists
    
    #joining the features considered into a single one
    #the same thing should be applied on multiple destination_column (only one input is accepted here though)
    list_feat = join_multi_columns(*list_feat_origin)
    
    #creating the dictionary with the structure value_to_map:primary_key
    id_dict = {feat : idx for idx, feat in zip(list_id, list_feat)}
    
    try:
        #mapped column result
        mapped_col = list(map(lambda x: id_dict[x], destination_column))
    except:
        #if an error is raised, probably there is no correspondence between the col_mapping_sources and destination
        raise Exception('The columns to map must have exact correspondence to the destination column')
    
    return mapped_col


#### Add columns to a table


#function used to add multiple columns to a table
def add_columns(table, *columns):
    #zip every columns to a list
    zipped_input = list(zip(*columns))
    
    #get the number of columns
    len_rows = len(table[0])
    len_zipped = len(zipped_input[0])
    
    output_table = list()
    
    #loop across every column
    #get the rows of that column
    #append to table
    #for both table and *columns to add
    
    #add each column of the original table to the list
    for col in range(len_rows):
        origin = extract_col(table, col)
        output_table.append((origin))
    
    #add each column of the zipped new columns to the list
    for col in range(len_zipped):
        origin = extract_col(zipped_input, col)
        output_table.append((origin))
        
    return list(zip(*output_table))


### Geography Table Functions

#### Mapping Continents to the Country Code


#function to convert the alpha2 code to the continent
def country_to_continent(country_alpha2):
    country_alpha2 = country_alpha2.upper() #making the country code uppercase
    
    #correcting the uk to gb (as it is accepted by the library)
    if country_alpha2 == 'UK':
        country_alpha2 = 'GB'
        
    #get continent code from country code
    country_continent_code = pc.country_alpha2_to_continent_code(country_alpha2)
    #get continent name from continent code
    country_continent_name = pc.convert_continent_code_to_continent_name(country_continent_code)
    return country_continent_name


#### Mapping Country Names to the Country Code


def country_to_country_name(country_alpha2):
    country_alpha2 = country_alpha2.upper() #making the country code uppercase
    
    #correcting the uk to gb (as it is accepted by the library)
    if country_alpha2 == 'UK':
        country_alpha2 = 'GB'
        
    #get country name from country code
    country_name = pc.country_alpha2_to_country_name(country_alpha2)
    return country_name


#############################################
# 
## PROCESSING THE DATASET


dict_columns = preprocess_csv_to_dict('answerdatacorrect.csv')


### Geography Table

#### Add the Continent and CountryName Columns to the DataFrame

temp_geo_tab = gen_table(dict_columns, 'Region', 'CountryCode')



country_codes = set([x[1] for x in temp_geo_tab[1:]])
## {'au', 'be', 'ca', 'de', 'es', 'fr', 'ie', 'it', 'nz', 'uk', 'us'}
map_country_to_cont = {cc : country_to_continent(cc) for cc in country_codes}
## {'es': 'Europe', 'ie': 'Europe', 'ca': 'North America', ..., 'au': 'Oceania'}



country_codes_list = [x[1] for x in temp_geo_tab[1:]]
## ['es', 'fr', 'uk', ..., 'de', 'de', 'au']
continent_column = list(map(lambda x: map_country_to_cont[x], country_codes_list))
## ['Europe', 'Europe', 'Europe', ...,'Europe', 'Europe', 'Oceania']



map_country_to_name = {cc : country_to_country_name(cc) for cc in country_codes}
## {'us': 'United States', 'nz': 'New Zealand', 'fr': 'France', ...,'it': 'Italy', 'au': 'Australia'}



country_name_column = list(map(lambda x: map_country_to_name[x], country_codes_list))
## ['Germany', 'United States', 'Ireland', ...,'Ireland', 'Germany', 'United Kingdom']


# There is a perfect correspondence between RegionId and CountryCode, thus the RegionId is probably a CountryId instead, and I don't need it in the data I will upload


dict_columns['Continents'] = continent_column
dict_columns['CountryName'] = country_name_column


#### Saving the Geography Final Table


geography_table = gen_table_distinct(dict_columns, 'Region', 'CountryCode', 'CountryName', 'Continents')
geography_table = set_primary_key(geography_table, 'GeoId')
create_csv('Geography.csv', geography_table)


### Computing the IsCorrect Measure


len_df = get_len_df(dict_columns)


# With a loop through the length of the list checking if the rows with the same index have identical values and return not a boolean but 0 or 1


#it checks if the answer value is correct and it adds 0 if not or 1 if it is
iscorrect = list()

for n in range(len_df):
    first_col = dict_columns['CorrectAnswer'][n]
    second_col = dict_columns['AnswerValue'][n]
    
    boolean_val = first_col == second_col
    iscorrect.append(str(int(boolean_val)))


#### Adding IsCorrect to the DataFrame


dict_columns['IsCorrect'] = iscorrect


### Subject Table


#create a new column for the subjectid
format_sub_col_to_list = list()

for x in dict_columns['SubjectId']:
    #make the string a list
    adjust_split = x.strip('][').split(', ')
    
    #append the values to the new column as integer
    format_sub_col_to_list.append([int(sub_id) for sub_id in adjust_split])



#get the csv of the subject metadata to a dictionary
meta = preprocess_csv_to_dict('subject_metadata.csv')



#fixing the wrong naming of the first column
meta['SubjectId'] = meta['ï»¿SubjectId']
del meta['ï»¿SubjectId']



#make the a dictionary to call the subject description, parent and level from its single subject code
subject_caller = dict(zip([int(x) for x in meta['SubjectId']], 
                          list(zip(meta['Name'], meta['ParentId'], [int(x) for x in meta['Level']]))))


#### Reorder subject lists according to level


#reordering the original list according to the parent levels (ascending order)
new_formatted_subject_col = list()
for row in format_sub_col_to_list:
    #sorting the rows according to the level (third element of the value in the dict)
    new_row = sorted(row, key=lambda x: subject_caller[x][2])
    new_formatted_subject_col.append(new_row)


#### Subject Description
# Writing only the level indicated in the SubjectId


description_column = list()

for row in new_formatted_subject_col:
    description_row = list()
    
    #for each subject in the subject id column
    for col_val in row:
        #unpack the values in the dictionary
        description, parent, level = subject_caller[col_val]
        
        #adding the description of the each value to the row
        new_row = f'{description} ({col_val})'
        
        #ppend the row to the description row
        description_row.append(new_row)
        
    #append the new description to the new description column
    description_column.append(description_row)



#adjusting the format of the description column to a single string

subject_column = list()

for row in description_column:
    #get the first element and add the quotation mark
    subjects = f'"{row[0]}'
    
    #get the other elements and add them in sequence
    for el in row[1:]:
        subjects += f' - {el}'
    
    #add a last quotation mark to the end of the new row
    subjects += '"'
    #append the new description to the final subject column
    subject_column.append(subjects)


#### Storing to DF and Saving the Subject Table into a File


dict_columns['Description'] = subject_column



subject_table = gen_table_distinct(dict_columns, 'Description')
subject_table = set_primary_key(subject_table, 'SubjectId')
create_csv('Subject.csv', subject_table)


### Dates

#### Get Date Table


double_date = gen_table(dict_columns, 'DateOfBirth', 'DateAnswered')



date_to_clean = dict_columns['DateAnswered']
space_pointer = date_to_clean[0].find(' ') #get the space position to discard the time
date_cleaned = [x[:space_pointer] for x in date_to_clean]



#Concatenating the two date columns into one
single_col_date = ['Dates'] + dict_columns['DateOfBirth'] + date_cleaned


#### Adjust Dates


year, month, day, quarter = list(), list(), list(), list()

dict_temp_quarter = {}

#write a temporary dictionary for each month pertaining to a quarter
for i in range(1, 5):
    dict_temp_quarter[f'Q{i}'] = [3*i-j for j in range(2, -1, -1)]
    #{'Q1': [1, 2, 3], 'Q2': [4, 5, 6], 'Q3': [7, 8, 9], 'Q4': [10, 11, 12]}
    
#reverse the previous dictionary unpacking each month into a key with its respective quarter
dict_quarters = dict()
for k, v in dict_temp_quarter.items():
    for i in range(len(v)):
        dict_quarters[v[i]] = k
        #{1: 'Q1', 2: 'Q1', 3: 'Q1', 
        # 4: 'Q2', 5: 'Q2', 6: 'Q2', 
        # 7: 'Q3', 8: 'Q3', 9: 'Q3',
        # 10: 'Q4', 11: 'Q4', 12: 'Q4'}

for row in single_col_date[1:]:
    #split the single date according to each dash -
    splitted_date = row.split('-')
    
    #add the year to its own column
    year.append(splitted_date[0])
    #add the year-month to the month column
    month.append(splitted_date[0]+'-'+splitted_date[1])
    #add the full date to the day column
    day.append(row)
    #add the year-quarter to its column, the quarter is retrieved by dict_quarters[month]
    quarter.append(splitted_date[0]+'-'+dict_quarters[int(splitted_date[1])])


#### Compile new Date Table


header_date = tuple(['Dates', 'Year', 'Month', 'Day', 'Quarter'])
date_without_header = single_col_date[1:]
data_date = sorted(list(set(zip(date_without_header, year, month, day, quarter))))

date_table = [header_date]+data_date


#### Saving to CSV


date_table = set_primary_key(date_table, 'DateId')
create_csv('Date.csv', date_table)


### Organization (quick)


organization_table = gen_table_distinct(dict_columns, 'GroupId', 'QuizId', 'SchemeOfWorkId')
organization_table = set_primary_key(organization_table, 'OrganizationId')
create_csv('Organization.csv', organization_table)


### User (Merging Tables)

#### GeoId


geoid = map_values(geography_table, dict_columns['Region'], 'Region')


#### DateId

#### Mapping on DateOfBirth


dateid_birth = map_values(date_table, dict_columns['DateOfBirth'], 'Dates')


#### Getting UserId Table
# 
# Non torna il numero di user


user_header = ('UserId', 'DateId', 'GeoId', 'Gender')
user_table_zip = list(zip(dict_columns['UserId'], dateid_birth, geoid, dict_columns['Gender']))
user_table_full = [user_header] + user_table_zip

#making the zip a set to remove duplicates, then again a list to sort it and add the header
user_table_distinct = [user_header] + sorted(list(set(user_table_zip)))



user_table = user_table_distinct
create_csv('User.csv', user_table)


### Writing the Fact Table

#### Map organization Id


organization_feat = [dict_columns['GroupId'], 
                     dict_columns['QuizId'], 
                     dict_columns['SchemeOfWorkId']]

org_to_join_on = join_multi_columns(*organization_feat)



organizationid = map_values(organization_table, org_to_join_on, 'GroupId', 'QuizId', 'SchemeOfWorkId')


#### Mapping on DateAnswered


dateid_answered = map_values(date_table, date_cleaned, 'Dates')


#### Map SubjectId


subjectid = map_values(subject_table, dict_columns['Description'], 'Description')


#### Creating the table and adding the columns to it


answer_table = gen_table(dict_columns, 
                         'AnswerId', 
                         'QuestionId', 
                         'AnswerValue', 
                         'CorrectAnswer', 
                         'IsCorrect', 
                         'Confidence', 
                         'UserId')



h_organizationid = ['OrganizationId'] + organizationid
h_dateid = ['DateId'] + dateid_answered #mapping answer dates on the fact table
h_subjectid = ['SubjectId'] + subjectid



answer_final = add_columns(answer_table, 
                           h_organizationid, 
                           h_dateid, 
                           h_subjectid)



create_csv('Answers.csv', answer_final)


## LOADING ON THE SERVER
# To manage the data we used mainly functions written in the previous step and a Class we wrote to upload to the actual server

from datetime import datetime
from tqdm import tqdm
import pyodbc
import copy
import os

#listing the csv files in the same folder that do not have data in the name
csv_files = [x for x in os.listdir() if x[-4:] == '.csv' and 'data' not in x]
print(csv_files)

#add the files to a dictionary to have them ready to be uploaded by calling just the file name
tables = dict()
for file_name in csv_files:
    tables[file_name] = preprocess_csv_to_dict(file_name)


## Upload tables on the Data Warehouse
# 
# This class enstablish a connection to the server, adjust the table according to the type required by the schema on the server, it drops previous values from the table and then it load the files on the server (committing every 100 records uploaded)

#this class will empty the remote table to load the one from a dictionary

class Upload_Table():
    
    def __init__(self, table_dict, table_name):
        #to avoid editing the original dictionary (if error occurs)
        self.table = copy.deepcopy(table_dict)
        #it removes ambiguities in the case of reserved keywords (e.g. User)
        self.table_name = "["+table_name+"]" 
        
        #Create a connection and a cursor in the database
        self.conn = self.get_connection()
        self.cursor = self.conn.cursor()
        
        #adjust the table input in the class to the types in the SQL Server Schema
        self.table = self.adjust_types()
        
        #try to upload the table
        try:
            self.insert_into_table()
            
        #close connection if an exception occurs
        except Exception as e:
            self.cursor.close()
            self.conn.close()
            raise e
            
        #close connection if it is a success
        self.cursor.close()
        self.conn.close()
        
        #delete the connection variables from the class
        del self.cursor
        del self.conn
        
    #function to get the credentials and perform the connection to the database
    def get_connection(self):
        
        #a file with the ip, userid and credentials must be in the same folder
        with open('credentials.txt', 'r') as f:
            ip, uid, pwd = f.read().splitlines()
            
        driver = 'ODBC Driver 17 for SQL Server'
        self.db = 'Group_10_DB' #the name of the database to which I am operating

        conn = pyodbc.connect(f'DRIVER={driver};SERVER=tcp:{ip};DATABASE={self.db};UID={uid};PWD={pwd}')
        
        return conn
    
    def adjust_types(self):
        self.cursor.execute(f'SELECT * FROM {self.table_name}')
        
        #using a dictionary to cast the correct types to the data
        #the lambda functions is there to cast the correct types
        cast_types = {'int': lambda x: int(float(x)), #some strings have values with a dot
                      'float': float, 
                      'str': str, 
                      'datetime.date': lambda x: datetime.strptime(x, '%Y-%m-%d').date(), 
                      'bool': lambda x: bool(int(float(x)))}
        
        
        col_type = dict()

        #looping across the information get by the cursor
        for name_col, type_col, _, len_char1, len_char2, _, accept_none in self.cursor.description:
            #getting the type from the type_col response string
            str_type = re.findall("'.*'", str(type_col))[0].strip("'")
            #saving the column with the corresponding type to cast into a dictionary
            col_type[name_col] = cast_types[str_type]

        #get the header of the local table
        self.header_table = list(self.table.keys())
        #check if the header of the local table corresponds to the header in the server
        assert list(col_type.keys()) == self.header_table, f'The header ({self.header_table}) of the table and the table in the Server ({list(col_type.keys())}) do not match!'

        
        table_list = list()

        #cast the correct types to the local table
        for col in self.header_table:
            to_type = col_type[col] #get the stored type recast function from the col_type dictionary
            self.table[col] = [to_type(el) for el in self.table[col]] #recast each element of the column
            table_list.append([col] + self.table[col]) #save a copy and add the header to the column
            
        table_list = list(zip(*table_list)) #rebuild the table from the recasted columns (list of lists)
        
        return table_list

    def sql_query_maker(self):
        #add the first part of the query with table name and the rest
        sql_query = f"INSERT INTO {self.table_name} ({', '.join(self.header_table)}) VALUES (?"
        
        #for each element in the header add the parametric question mark (except for the first, thus -1)
        for i in range(len(self.header_table)-1):
            sql_query += ",?"
        sql_query += ")" #close the row to upload

        return sql_query

    def delete_previous_vals_from_table(self, table_name):
        #try to delete the values from the table considered to upload
        try:
            self.cursor.execute(f'DELETE FROM {table_name}')
            
        #Every data in the hierarchy of the table will be deleted to avoid the Integrity Error
        except pyodbc.IntegrityError as ierr:
            #looking for the table to DELETE FROM in the error with regex
            table_prefix = self.db[:self.db.rfind('_')]
            start_idx = re.search(f'The conflict occurred in database "{self.db}", table "{table_prefix}\.', str(ierr)).end()
            end_idx = str(ierr)[start_idx:].find('"')+start_idx
            
            new_table = str(ierr)[start_idx:end_idx]
            new_table_name = "["+new_table+"]"
            
            #recursively remove from the tables in the higher hierarchy
            self.delete_previous_vals_from_table(new_table_name)
            #retry removing from the table (it should work now)
            self.cursor.execute(f'DELETE FROM {table_name}')
            
    def insert_into_table(self):
        #getting the query
        sql_query = self.sql_query_maker()
        #removing all the values from table to upload it
        self.delete_previous_vals_from_table(self.table_name)
        
        print("Query:\n" + sql_query)

        #tqdm gives the progress bar, I looped across the rows (avoiding the header)
        for n, row in enumerate(tqdm(self.table[1:], ascii=True, desc='Uploading Progress')):
            tupla = tuple(el for el in row) #making the row a tuple if it is not
            
            # Try to reconnect at least 10 times if the execution fails
            for attempt in range(10):
                try:
                    #executing the query with the values in the tuple
                    self.cursor.execute(sql_query, (tupla))
                    break
                    
                #if it reaches the 10th execution raise the error I blocked
                except Exception as e:
                    if attempt == 9:
                        raise e
                    else:
                        continue
            
            #to commit every 1000 records (in case it crashes and I avoid the delete statement to finish uploading later)
            if n == (n // 1000) * 1000:
                self.conn.commit()
                
        #commit at the end
        self.conn.commit()


Upload_Table(tables['Geography.csv'], 'Geography')
Upload_Table(tables['Subject.csv'], 'Subject')
Upload_Table(tables['Organization.csv'], 'Organization')
Upload_Table(tables['Date.csv'], 'Date')
Upload_Table(tables['User.csv'], 'User')
Upload_Table(tables['Answers.csv'], 'Answers')
