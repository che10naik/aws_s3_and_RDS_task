import configparser

config = configparser.ConfigParser()

#read the config file 
config.read('config.ini')

#Usage
num_records = config.getint('DATA_GENERATION', 'num_rows')
salary_min = config.getfloat('DATA_GENERATION', 'salary_min')
bucket_name = config.get('AWS', 'bucket_name')

print(num_records, salary_min, bucket_name)
