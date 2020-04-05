import psycopg2
import csv
import argparse
import configparser


def parse_config(config_path):
    ''' Used to parse config parameters'''

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
    #Assuming table already exists can also create it using another function
    conn =  psycopg2.connect(host=configuration['host'],database=configuration['database'],\
         user=configuration['user'], password=configuration['password'])
    
    cur= conn.cursor()

    COPY_COMMAND = """COPY {} FROM '{}' delimiter ',' csv header"""
    cur.execute(COPY_COMMAND.format('public.conversion_data', file_path))
    print('>> Exported the file contents to table conversion.data table')

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

    if(conn):
        cur.close()
        conn.close()

    return int(conversion_count)/int(user_count)



if __name__ == '__main__':
    args = argparse.ArgumentParser()
    args.add_argument('--config_file_path', required= True )
    args.add_argument('--conversion_file_path', required= True )
    arguments = vars(args.parse_args())

    config_path = arguments['config_file_path']
    file_path = arguments['conversion_file_path']
    
    config_dict = parse_config(config_path)


    #Exporting data to Postgres db table
    # export_to_db(config_dict, file_path)

    #Ques 1 : find overall conversion rate
    print('Overall conversion is:',find_conversion(config_dict))
