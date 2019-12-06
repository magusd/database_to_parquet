import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
from fastparquet import write
import datetime
import boto3

##AWS
bucket = 'teste-upload-caraio'
aws_key = ''
aws_secret = ''

####Query
query = 'select id, link, title from links'
batch_size = 1000

s3 = boto3.client(
    's3',
    # Hard coded strings as credentials, not recommended.
    aws_access_key_id=aws_key,
    aws_secret_access_key=aws_secret
)


engine = create_engine(URL(
	drivername="mysql+pymysql", #mssql
	username="root",
	password="",
	host="localhost",
	database="hackernews"
))

conn = engine.connect()

generator_df = pd.read_sql(sql=query,  # mysql query
						   con=conn,
						   chunksize=batch_size)  # size you want to fetch each time
i = 0
files = []
for dataframe in generator_df:
	i+=1
	name = '{}_part_{}.parq'.format(datetime.datetime.now().strftime("%Y-%m-%d"),i)
	files.append(name)
	write(name, dataframe)

for f in files:
	s3.upload_file(f, bucket, f)
	print(f)	