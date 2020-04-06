import psycopg2
import csv
import argparse
import configparser
import operator
import os


def parse_config(config_path):
    ''' Used to parse config parameters for database connection'''

    config_dict ={}
    config = configparser.ConfigParser()
    config.optionxform = lambda option: option
    config.read(config_path)

    config_dict ['host'] =  config['RESOURCE']['host']
    config_dict ['user'] =  config['RESOURCE']['user']
    config_dict ['password'] =  config['RESOURCE']['password']
    config_dict ['port'] =  config['RESOURCE']['port']
    config_dict ['database'] =  config['RESOURCE']['database']
    return config_dict


def export_to_db(configuration,file_path):
    #Assuming table already exists (can also create it using another function)

    conn =  psycopg2.connect(host=configuration['host'],database=configuration['database'],\
         user=configuration['user'], password=configuration['password'])
    
    cur= conn.cursor()

    COPY_COMMAND = """COPY {} FROM '{}' delimiter ',' csv header"""
    print('>>Starting exporting data to postgres database\n')
    cur.execute(COPY_COMMAND.format('public.conversion_data', file_path))
    print('>> Exported the file contents to table conversion.data table\n')

def find_conversion(configuration):
    conn =  psycopg2.connect(host=configuration['host'],database=configuration['database'],\
         user=configuration['user'], password=configuration['password'])
    
    cur= conn.cursor()

    UNIQUE_USER_COUNT_QUERY = """ select count(distinct session_id) from public.conversion_data """

    cur.execute(UNIQUE_USER_COUNT_QUERY)

    for result in cur:
        user_count = int(''.join(map(str, result)))
    
    NUMBER_OF_CONVERSIONS_QUERY = """ select count(*) from public.conversion_data where event_type= 'conversion' """
    cur.execute(NUMBER_OF_CONVERSIONS_QUERY)
    for result in cur:
        conversion_count = int(''.join(map(str, result)))
 
    print('Number of Total Conversions',conversion_count)
    print('Unique Users Count:',user_count)

    return int(conversion_count)/int(user_count)


def cal_conversion_by_page(configuration):
    conn =  psycopg2.connect(host=configuration['host'],database=configuration['database'],\
         user=configuration['user'], password=configuration['password'])
    
    cur= conn.cursor()

    print('>>Calculating Overall conversion')
    USER_ON_PAGE_QUERY = """ 
    select count(distinct session_id) 
    from public.conversion_data 
    where page_id = {}"""

    cur.execute(USER_ON_PAGE_QUERY.format('4903628644844587131'))

    for result in cur:
        user_on_page_count = int(''.join(map(str, result)))
    
    NUMBER_OF_CONVERSIONS_ON_PAGE_QUERY = """ 
    select count(*) 
    from public.conversion_data 
    where event_type= 'conversion' 
    and page_id = {} """

    cur.execute(NUMBER_OF_CONVERSIONS_ON_PAGE_QUERY.format('4903628644844587131'))

    for result in cur:
        conversion_on_page_count = int(''.join(map(str, result)))


    return int(conversion_on_page_count)/ int(user_on_page_count)


def sort_file_generate(configuration):
    conn =  psycopg2.connect(host=configuration['host'],database=configuration['database'],\
         user=configuration['user'], password=configuration['password'])
    
    cur= conn.cursor()

    SORTED_RESULTS_QUERY= '''select * from public.conversion_data order by page_id, session_id'''
    cur.execute(SORTED_RESULTS_QUERY)

    colnames = [desc[0] for desc in cur.description]

    with open(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'output_files\conversion_data_sorted.csv'), 'w'\
        , newline='') as write_obj:
        csv_writer = csv.DictWriter(write_obj, fieldnames= colnames)
        csv_writer.writeheader()
        for row in cur:
            csv_writer.writerow(dict(zip(colnames,row)))

    if(conn):
        cur.close()
        conn.close()    



if __name__ == '__main__':

    #Initializing file paths
    config_path = os.path.join(os.path.dirname(os.path.realpath(__file__)),'config.conf')
    input_file_path = os.path.join(os.path.dirname(os.path.realpath(__file__)),'input_file\conversion_data.csv')
    
    config_dict = parse_config(config_path)



#Exporting data to Postgres db table
    
    export_to_db(config_dict, input_file_path)


#Task a : find overall conversion rate
    print('Task a: Overall conversion is :  ',find_conversion(config_dict))
    print('\n')

#Task b : conversion rate of users that started their session on page '4903628644844587131'
    print('Task b: Conversion rate of users that started their session on page "4903628644844587131":  '\
        , cal_conversion_by_page(config_dict))
    print('\n')
#Task c: Sort data by page_id and user and write in file

    print('>> Starting to sort the conversion_data file\n')
    sort_file_generate(config_dict)
    print('>>File Sorting completed new file in output folder')
