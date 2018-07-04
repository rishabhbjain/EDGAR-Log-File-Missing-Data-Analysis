#importing libraries
import requests
import zipfile,io
import pandas as pd
import csv
import re
import os
import logging
import sys
import shutil
import glob # to get the file recursively
import json
import time
import datetime
import boto3

## aws s3
def upload_to_s3(Inputlocation,Access_key,Secret_key):
    try:
        buck_name='edgarlogdataset'
        S3_client = boto3.client('s3',Inputlocation,aws_access_key_id= Access_key, aws_secret_access_key= Secret_key)
        if Inputlocation == 'us-east-1':
            S3_client.create_bucket(Bucket=buck_name)
        elif Inputlocation == 'us-west-1':
            S3_client.create_bucket(Bucket=buck_name)
        elif Inputlocation == 'us-east-2':
            S3_client.create_bucket(Bucket=buck_name)
        elif Inputlocation == 'us-west-2':
            S3_client.create_bucket(Bucket=buck_name)
        elif Inputlocation == 'ap-northeast-1':
            S3_client.create_bucket(Bucket=buck_name)
        elif Inputlocation == 'ap-northeast-2':
            S3_client.create_bucket(Bucket=buck_name)
        elif Inputlocation == 'ap-northeast-3':
            S3_client.create_bucket(Bucket=buck_name)
        elif Inputlocation == 'ap-south-1':
            S3_client.create_bucket(Bucket=buck_name)
        elif Inputlocation == 'ap-southeast-1':
            S3_client.create_bucket(Bucket=buck_name)
        elif Inputlocation == 'ap-southeast-2':
            S3_client.create_bucket(Bucket=buck_name)
        elif Inputlocation == 'ca-central-1':
            S3_client.create_bucket(Bucket=buck_name)
        elif Inputlocation == 'cn-north-1':
            S3_client.create_bucket(Bucket=buck_name)
        elif Inputlocation == 'cn-northwest-1':
            S3_client.create_bucket(Bucket=buck_name)
        elif Inputlocation == 'eu-central-1':
            S3_client.create_bucket(Bucket=buck_name)
        elif Inputlocation == 'eu-west-1':
            S3_client.create_bucket(Bucket=buck_name)
        elif Inputlocation == 'eu-west-2':
            S3_client.create_bucket(Bucket=buck_name)
        elif Inputlocation == 'eu-west-3':
            S3_client.create_bucket(Bucket=buck_name)
        elif Inputlocation == 'sa-east-1':
            S3_client.create_bucket(Bucket=buck_name)
        else:
            logging.info("Please enter valid location")
            sys.exit()
        S3_client.upload_file("edgarlogdataset.zip", buck_name,"edgarlogdataset.zip")

    except Exception as e:
        print("Error uploading files to Amazon s3")
        sys.exit()



##log file initialization
root = logging.getLogger()
root.setLevel(logging.DEBUG)

#output the  log to a file
log = logging.FileHandler('edgar_log.log')
log.setLevel(logging.DEBUG)
#creation of formatter
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
log.setFormatter(formatter)
root.addHandler(log)

#print the logs in console
console = logging.StreamHandler(sys.stdout )
console.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(levelname)s - %(message)s')
console.setFormatter(formatter)
root.addHandler(console)

# Enter the year

try:
    def incheck(arg):
        if re.match('[0-9]{4}',arg):
            return arg
except Exception as e:
    logging.info(str(e))
    sys.exit()

## input from user
argLen=len(sys.argv)
yr=''
Access_key=''
Secret_key=''
Inputlocation=''

for i in range(1,argLen):
    val=sys.argv[i]
    if val.startswith('yr='):
        pos=val.index("=")
        yr=val[pos+1:len(val)]
        continue
    elif val.startswith('accessKey='):
        pos=val.index("=")
        Access_key=val[pos+1:len(val)]
        continue
    elif val.startswith('secretKey='):
        pos=val.index("=")
        Secret_key=val[pos+1:len(val)]
        continue
    elif val.startswith('location='):
        pos=val.index("=")
        Inputlocation=val[pos+1:len(val)]
        continue
try:
    year = incheck(yr)
except Exception as e:
    logging.error(str(e))
    quit()

#make directories
try:
    if not os.path.exists('downloaded_zips_unzipped'+year):
        os.makedirs('downloaded_zips_unzipped'+year, mode=0o777)
    else:
        shutil.rmtree(os.path.join(os.path.dirname(__file__),'downloaded_zips_unzipped'+year),ignore_errors=False)
        os.makedirs('downloaded_zips_unzipped'+year, mode=0o777)
    logging.info('Directories cleanup completed')
except Exception as e:
    logging.error(str(e))
    exit()

#download the zip file
url ='https://www.sec.gov/dera/data/Public-EDGAR-log-file-data/'

date ='01'
month_link = list()
for month in range(1,13):
    if month < 4:
        qtr = 'Qtr1'
    elif month <7:
        qtr = 'Qtr2'
    elif month <10:
        qtr = 'Qtr3'
    else:
        qtr = 'Qtr4'

    try:
        l_month = url+year+'/'+qtr+'/log'+year+str(month).zfill(2)+date+'.zip'
        print(l_month)
        month_link.append(l_month)
        rzip = requests.get(l_month)
        zf = zipfile.ZipFile(io.BytesIO(rzip.content))
        zf.extractall('downloaded_zips_unzipped'+year)
        logging.info(' Downloaded Log file %s for First date of month.', l_month)

    except Exception as e:
        logging.info('File not available for %s of %s',month,year)


logging.info(' Downloaded all the Log file for %s',year)

# Loading and Merging of csv Files into Pandas DataFrame

try:
    filelists = glob.glob('downloaded_zips_unzipped'+year +"/*.csv")
    dataset = pd.DataFrame()
    lista = []
    for filea in filelists:
        df=pd.read_csv(filea,index_col=None,header=0)
        lista.append(df)
    dataset = pd.concat(lista)
    logging.info('All the csv read into individual dataframes')
except Exception as e:
    logging.info(str(e))
    sys.exit()
#Detecting Anomalies
#Handling Missing Values and Computing
try:
    #removing rows with no ip, date, time, cik , accession
    dataset.dropna(subset=['ip'])
    dataset.dropna(subset=['date'])
    dataset.dropna(subset=['time'])
    dataset.dropna(subset=['cik'])
    dataset.dropna(subset=['accession'])
    logging.info('Rows removed where ip, date, time, cik or accession were null')

    #dropping empty column browser
    try:
        max_browser = pd.DataFrame(dataset.groupby('browser').size()).idxmax()[0]
        dataset['browser'] = dataset['browser'].fillna(max_browser)
        logging.info('NaN values in browser replaced with maximum count browser')

    except:
        dataset= dataset.dropna(axis='columns',how='all')
        logging.info('All the values in browser are NaN so column browser is deleted')


    #replace nan idx with max idx
    max_idx = pd.DataFrame(dataset.groupby('idx').size()).idxmax()[0]
    dataset['idx'] = dataset['idx'].fillna(max_idx)
    logging.info('NaN values in idx replaced with maximum idx')

    #replace nan code with max code
    max_code = pd.DataFrame(dataset.groupby('code').size()).idxmax()[0]
    dataset['code'] = dataset['code'].fillna(max_code)
    logging.info('NaN values in code replaced with maximum code')

    #replace nan norefer with one
    dataset['norefer'] = dataset['norefer'].fillna('1')
    logging.info('NaN values in norefer replaced')

    #replace nan noagent with one
    dataset['noagent'] = dataset['noagent'].fillna('1')
    logging.info('NaN values in noagent replaced')

    #replace nan find with min find
    min_find = pd.DataFrame(dataset.groupby('find').size()).idxmax()[0]
    dataset['find'] = dataset['find'].fillna(min_find)
    logging.info('NaN values in find replaced with minimum find')

    #replace nan crawler with zero
    dataset['crawler'] = dataset['crawler'].fillna('0')
    logging.info('NaN values in crawler replaced')

    #replace nan extension with max extension
    max_extention = pd.DataFrame(dataset.groupby('extention').size()).idxmax()[0]
    dataset['extention'] = dataset['extention'].fillna(max_extention)
    logging.info('NaN values in extension replaced with maximum extension')

    #replace null values of size with mean of the size
    dataset['size']=dataset['size'].fillna(dataset['size'].mean(axis = 0))
    logging.info('NaN values in size replaced with mean value of size')

    #replace nan zone with max zone
    max_zone = pd.DataFrame(dataset.groupby('zone').size()).idxmax()[0]
    dataset['zone'] = dataset['zone'].fillna(max_zone)
    logging.info('NaN values in zone replaced with maximum zone')

    ##### Summary Metrics #####
    data = pd.DataFrame()
    data = dataset
    logging.debug("Summary Metrices Computation")
    ## grouping ipaddress by date
    summary_ip=data['ip'].groupby(data['date']).describe()
    summary_ip_describe = pd.DataFrame(summary_ip)
    s=summary_ip_describe.transpose()
    s.to_csv("summaryipbydate.csv")
    ## grouping cik by accession
    summary_cik = data['accession'].groupby(data['cik']).describe()
    summary_cik_describe= pd.DataFrame(summary_cik)
    summary_cik_describe.to_csv("summarycikbyaccession.csv")
    ## per code count
    code_count = data.groupby(['code']).count()
    summary_code = pd.DataFrame(code_count)
    summary_code.to_csv("summarycodecount.csv")
except Exception as e:
    logging.info(str(e))
    exit()

# Combining all dataframe to master_csv
dataset.to_csv('master_csv.csv')
logging.info('All dataframes of csvs are combined and exported as csv: master_csv.csv')

# zip the csvs and log files
def zipdir(path,ziph):
    ziph.write(os.path.join('master_csv.csv'))
    ziph.write(os.path.join('summaryipbydate.csv'))
    ziph.write(os.path.join('summarycikbyaccession.csv'))
    ziph.write(os.path.join('summarycodecount.csv'))
    ziph.write(os.path.join('edgar_log.log'))

zipf = zipfile.ZipFile('edgarlogdataset.zip','w',zipfile.ZIP_DEFLATED)
zipdir('/',zipf)
zipf.close()
logging.info('Compiled csv and log file zipped')

##uploading files to aws s3
upload_to_s3(Inputlocation,Access_key,Secret_key)
logging.info("Files uploaded successfully")
