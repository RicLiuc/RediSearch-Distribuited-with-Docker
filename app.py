import pandas as pd 
import json
from tqdm import tqdm
from redisearch import Client, TextField, NumericField, Query
from time import sleep
from rediscluster import StrictRedisCluster

sleep(15)
i=0

nodes = [{'host': "173.17.0.2", 'port': "7000"}]
rc = StrictRedisCluster(startup_nodes=nodes, decode_responses=True)


client=Client('week1', conn=rc)
#client.create_index([TextField('day'), TextField('filename'), TextField('protocol'), TextField('task_monitor_id'), TextField('task_id'), TextField('job_id'), TextField('site_name')])
client.create_index([TextField('protocol'), TextField('site_name')])
dat = pd.read_csv("results_2018-05-01.csv.gz")


for idx, row in tqdm(dat.iterrows()):
	#client.add_document(f"{row['index']}", day=f"{row['day']}", filename = f"{row['filename']}", protocol = f"{row['protocol']}", task_monitor_id = f"{row['task_monitor_id']}", task_id = f"{row['task_id']}", job_id = f"{row['job_id']}",  site_name = f"{row['site_name']}")
	client.add_document(f"{row['day']:0.0f}_{row['index']}", replace=True, partial=True, protocol = f"{row['protocol']}", site_name = f"{row['site_name']}")
	i +=1
	if i==1000:
		break

#print(client.search("@protocol:local"))


#payload=f"{row['index']}",
#replace=True, partial=True,
#f"{row['day']:0.0f}_{row['index']}"