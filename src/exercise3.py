'''
Created on 10/05/2014

@author: davidreyblanco
'''
import pandas as pd
import numpy as np
import time
import matplotlib.pyplot as plt
import config

chunk_size = 30000
sample_searches = pd.read_csv(config.data_folder + 'searches.csv.bz2',usecols=['Date','Destination'],sep='^',iterator=True,chunksize=chunk_size)
record_count = 0
index = 0
block_limit = 0
t_0 = time.time()
aggregated = None

# define an inline function to extrat the month code

get_month_code = lambda x : x[:4]+x[5:7]

aggregated = None

# For each Chunk in datataset
for chunk in sample_searches:
    t0 = time.time()
    index = index + 1
    record_count = index * chunk_size
# Filter all records arriving to MADRID, MALAGA and BARCELONA (MAD,MAL,BCN) by Destination
    searches_chunk = chunk.query("Destination == 'MAD' or Destination == 'AGP' or Destination == 'BCN'")
    searches_chunk['Date'] = searches_chunk['Date'].apply(get_month_code)
    searches_chunk['count'] = 1
   
    grouped = searches_chunk.groupby(['Destination','Date'], as_index=False).sum()
    if index == 1:
        aggregated = grouped.reset_index()
    else:
        aggregated = aggregated.append(grouped.reset_index()) 
    t1 = time.time()
    lapse = (t1-t0)
    print("Index: " , index , " Record count (K-records):" , record_count/1000,'time:',lapse,' ETA:',(t1-t_0),' tp (rec/sec):',chunk_size/lapse)
    if (block_limit > 0 and index > block_limit) : break 

aggregated = aggregated.groupby(['Destination','Date'], as_index=False).sum()
del aggregated['index']
aggregated
aggregated.to_csv(path_or_buf='searches_series.csv',sep='^',index=False)

# filter out by Destination and transpose 

aggregated = pd.read_csv('searches_series.csv',sep='^')

rng = pd.period_range('2013-JAN', '2013-DEC', freq='M')
data_frame_madrid = aggregated.query("Destination == 'MAD'").rename(columns = {'count' : 'Madrid'})
del data_frame_madrid['Destination']
data_frame_madrid = data_frame_madrid.drop('Unnamed: 0',1)
data_frame_bcn = aggregated.query("Destination == 'BCN'").rename(columns = {'count' : 'Barcelona'})
del data_frame_bcn['Destination']
data_frame_bcn = data_frame_bcn.drop('Unnamed: 0',1)
data_frame_malaga = aggregated.query("Destination == 'AGP'").rename(columns = {'count' : 'Malaga'})
del data_frame_malaga['Destination']
data_frame_malaga= data_frame_malaga.drop('Unnamed: 0',1)

# Merge all tables
tmp = pd.merge(data_frame_madrid,data_frame_bcn,on='Date')
final = pd.merge(tmp,data_frame_malaga,on='Date')
final.index = rng
del final['Date']
final

# Show the  series on screen    
final.plot()
plt.show()

