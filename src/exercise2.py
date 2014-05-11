'''
Created on 10/05/2014

@author: davidreyblanco
'''
'''
Created on 10/05/2014

@author: davidreyblanco
'''
import pandas as pd
import numpy as np
import time
import config

debug_on = True # For debugging
chunk_size = 30000
sample_bookings = pd.read_csv(config.data_folder + 'bookings.csv.bz2', usecols=['pax','arr_port'], compression='bz2',sep='^',iterator=True,chunksize=chunk_size)
record_count = 0
index = 0
block_limit = 20000
t_0 = time.time()
aggregated = None
total_pax = 0
# For each Chunk in datataset
for chunk in sample_bookings:
    t0 = time.time()
    index = index + 1
    record_count = index * chunk_size
# Tidy data up
    chunk.fillna(0, inplace=True)
# Create aggreted metrics    
    total_pax = total_pax + chunk['pax'].sum()
    airport_data = chunk.groupby('arr_port').sum()
# Append da
    if index == 1:
        aggregated = airport_data.reset_index()
    else:
        aggregated = aggregated.append(airport_data.reset_index())    
    t1 = time.time()
    lapse = (t1-t0)
    if debug_on and index % 5 == 0:
        print("Index: " , index , " Record count (K-records):" , record_count/1000,'time:',lapse,' ETA:',(t1-t_0),' tp (rec/sec):',chunk_size/lapse)
    if (index > block_limit) : break 

# Remove spaces, done here because it's costly
aggregated['arr_port'] = aggregated['arr_port'].apply(lambda x : x.strip())

sorted_data = aggregated.groupby('arr_port').sum().sort(columns='pax',ascending=False)
sorted_data.to_csv(path_or_buf=config.data_folder + 'sorted_list.csv',sep='^')
final_data = sorted_data[:10]
final_data

# Bonus point: Look up the names for each airport 

from GeoBases import GeoBase
record_count = 200
airport_data = pd.read_table(config.data_folder + 'sorted_list.csv', sep='^')
airport_data['city_name'] = 'None'
geo_o = GeoBase(data='ori_por', verbose=False)
# Define a Lambda function
find_city = lambda x,geo_o : geo_o.get(x.strip(),'city_name_ascii')

for i, row in enumerate(airport_data.values):
    try:
        city_name = find_city(airport_data['arr_port'][i],geo_o)
        airport_data['city_name'][i] = city_name
        print city_name
    except:
        print "Oops!  That was no valid number.  Try again...",i,airport_data['arr_port'][i]

# Save it for using it later on in the bonus exercise (Web service)
airport_data.to_csv(path_or_buf= config.data_folder + 'sorted_named_list.csv',sep='^')
airport_data[:10]