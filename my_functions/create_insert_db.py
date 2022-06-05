import pandas as pd
import numpy as np
import datetime
from random import choice


def create_entries(table_name,df):
    '''
    Function is returning a string to enter values in database\n
    First argument is the table name. Second argument is dataframe which will be looped
    '''

    # Initial string for inserting values
    first = 'INSERT INTO {} VALUES '.format(table_name)

    # creating a list based on tuples extracted from the dataframe
    list_of_inserts = [x[:] for x in list(df.itertuples(index=False))]

    # returning string for inserting values. List of listers is converted to string and first and last character are deleted
    return first + str(list_of_inserts)[1:-1]


def create_table(columns,types,table_name):
    '''
    Function is returning the string which can be used to create table in databases (Postgres).\n
    First argument is list of columns, second is list of types of data in database, last is name of the table\n
    '''
    starting_string = f'''CREATE TABLE {table_name} ('''
    for counter, item in enumerate(columns):
        if counter ==0:
            string_to_add = f'{item.lower()} {types[counter].upper()} PRIMARY KEY, '
        else:
            string_to_add = f'{item.lower()} {types[counter].upper()}, '
        starting_string +=string_to_add
    final_string = starting_string[:-2] + ')'
    return final_string


class DBGenerator:
    def __init__(self,df=None,table_name=None,types=None):
        self.df = df
        self.table_name = table_name
        self.types = types

        # check if dataframe was passed
        if  self.df != None:
            
            # assign columns names and types
            self.columns = df.columns.to_list()
            self.data_types = df.dtypes.astype(str).to_dict()

            # looping over all items. If int spotted -> assign integer, if float spotted -> assign float, otherwise -> check for date, timestamp or str
            for key,value in self.data_types.items():
                if 'int' in value:
                    self.data_types[key] = 'INTEGER'
                elif 'float' in value:
                    self.data_types[key] = 'FLOAT'
                else:
                    try:
                        
                        # check if there is time part or only date (only date => assign DATE, date with time => assign TIMESTAMP)
                        if pd.to_datetime(self.df[key]).astype(str).str.len()[0] >12:
                            self.data_types[key] = 'TIMESTAMP'
                        else:
                            self.data_types[key] = 'DATE'
                    except ValueError:
                        
                        # check the longest string in column and assign VARCHAR with max characters
                        self.data_types[key] = f'VARCHAR({self.df[key].str.len().max()})'
            print(self.data_types)
            # print('ok')

        
    def create_table(self):
        '''
        Function is returning the string which can be used to create table in databases (Postgres).\n
        First argument is list of columns, second is list of types of data in database, last is name of the table\n
        '''
        if not self.df.empty:
            starting_string = f'''CREATE TABLE {self.table_name} ('''
            
            # looping over dictionary
            for counter, (key, value) in enumerate(self.data_types.items()):
                
                # for first column check if unique values are present, true -> return name, type and PRIMARY KEY, False -> return just name and type
                if counter ==0:
                    if self.df[key].is_unique == True:
                        string_to_add = f'{key} {value} PRIMARY KEY, '
                    else:
                        string_to_add = f'{key} {value}, '
                else:
                    
                    # return column and type
                    string_to_add = f'{key} {value}, '
                
                # add to string which will be return at the end of looping over all columns in DataFrame
                starting_string +=string_to_add
            final_string = starting_string[:-2] + ')'
            return final_string
        else:
            return "You need to first specify the parameters"
    
    def create_entries(self):
        '''
        Function is returning a string to enter values in database\n
        First argument is the table name. Second argument is dataframe which will be looped
        '''
        if not self.df.empty:
            # Initial string for inserting values
            first = 'INSERT INTO {} VALUES '.format(self.table_name)

            # creating a list based on tuples extracted from the dataframe
            list_of_inserts = [x[:] for x in list(self.df.itertuples(index=False))]

            # returning string for inserting values. List of listers is converted to string and first and last character are deleted
            return first + str(list_of_inserts)[1:-1].replace(',)',")")
        else:
            return "You need to first specify the parameters"

    def create_db(self):

        # if df is not empty
        if not self.df.empty:

            # return results from for functions
            table = self.create_table()
            entries = self.create_entries()

            # return tuple with strings for creating table in database and for inserting values
            return table,'',entries
        else:
            return "You need to first specify the parameters"



    def random_dates(self,start_date, end_date, how_many, seed=1, replace=True):

        # generating date range and converting to series
        dates = pd.date_range(start_date, end_date).to_series()

        # selecting only  sample of dates from series
        # return [dt.strftime('%Y-%m-%d') for dt in dates.sample(how_many, replace=replace, random_state=seed)]

        return (dates.sample(how_many, replace=replace, random_state=seed).index).strftime('%Y-%m-%d')

    def randomtimes(self,start_time, end_time, n):

        
        # getting start and end timestamp
        try:
            # establishing format of entered data (with seconds)
            format = '%Y-%m-%d %H:%M:%S'
            stime = datetime.datetime.strptime(start_time, format)
            etime = datetime.datetime.strptime(end_time, format)
        except ValueError:
                # in case of value error (missing seconds) -> take only hours and minutes 
            format = '%Y-%m-%d %H:%M'
            stime = datetime.datetime.strptime(start_time, format)
            etime = datetime.datetime.strptime(end_time, format)
        # getting the difference between two timestamps
        td = etime - stime

        # getting list of random timestamps 
        return [(np.random.random() * td + stime).strftime('%Y-%m-%d %X') for _ in range(n)]

        random_timestamps = [np.random.random() * td + stime for _ in range(n)]

        # returning list of timestamps adjusted to be entered in SQL
        return [x.strftime('%Y-%m-%d %H:%M:%S') for x in random_timestamps]


    def create_random_db(self,add_id= (False,None),table_name= None,column_names_and_types=None,how_many=None,shuffle_dict=None,range_dict=None):
        """
        
        DB Generator
        ----------
        >>> A class with a function allowing you to create a string that you can put to database query tool to create a table with values

        Parameters
        ----------
        >>> add_id= (False,None) = > Tuple of two values. First is True or False indicating if 
        we want to have id column (with PRIMARY KEY) and what should be the name of this column
        table_name= None = > string for table name

        >>> column_names_and_types=None = > dictionary of column names and data types. Below available data types:
        "i" - INTEGER
        "f" - FLOAT
        "d" - DATE
        "dt" - TIMESTAMP
        "s" - VARCHAR (number of characters will be determined by checking the longest string available)

        >>> how_many=None = > integer indicating how many values we want to generate

        >>> shuffle_dict=None = > dictionary with column_name:list of values pairs

        >>> Example: {'country':['USA','Germany','UK','France','Poland']}
        
        >>> range_dict=None = > dictionary with column_name:two values tuple with min and max range


        Returns
        -------
        >>> Returned value is a tuple with two strings (separated by empty string):
            1. CREATE TABLE string
            2. INSERT VALUES string


        """

        # assigning table name for SQL 
        self.table_name = table_name

        # initiate dictionary for datatypes
        self.data_types = {}

        # creating empty dataframe for appending values
        frame = pd.DataFrame(index=range(1,how_many+1))
        
        # if ID was declared as True - generate first  column with unique values and passed naming
        if add_id[0]:
            frame[add_id[1]] = range(1,how_many+1)

            # assigning first column with INTEGER type
            self.data_types[add_id[1]] = 'INTEGER'


        # looping over all columns 
        for key,value in column_names_and_types.items():
            # np.random.uniform(low=range_dict[key][0], high=range_dict[key][1], size=(how_many,)).round(2)
            
            # if integer - change type and add column of integers to dataframe
            if value == 'i':
                self.data_types[key] = 'INTEGER'
                frame[key] = np.random.randint(low=range_dict[key][0],high=range_dict[key][1]+1, size=(how_many,))

            # if float - change type and add column of floats to dataframe  
            elif value == 'f':
                self.data_types[key] = 'FLOAT'
                frame[key] = np.random.uniform(low=range_dict[key][0], high=range_dict[key][1], size=(how_many,)).round(2)


            # if date - change type and add column of dates to dataframe  
            elif value == 'd':
                self.data_types[key] = 'DATE'
                frame[key] = self.random_dates(range_dict[key][0],range_dict[key][1],how_many)

            # if timestamp - change type and add column of timestamps to dataframe  
            elif value == 'dt':
                self.data_types[key] = 'TIMESTAMP'
                frame[key] = self.randomtimes(range_dict[key][0],range_dict[key][1],how_many)

            # if string - change type and add column of strings to dataframe  
            elif value =='s':
                self.data_types[key] = f'VARCHAR({len(max(shuffle_dict[key],key=len))})'
                frame[key] = [choice(shuffle_dict[key]) for _ in range(how_many)]
        
        # assigning create frame to object dataframe
        self.df = frame

        # return results from for functions
        table = self.create_table()
        entries = self.create_entries()

        # correction for "PRIMARY KEY". If it was generated but not declared - remove it from SQL statement
        if not add_id[0] and "PRIMARY KEY" in table:
            table = table.replace(' PRIMARY KEY',"")

        # return tuple with strings for creating table in database and for inserting values
        return table,'',entries