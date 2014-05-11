'''
Created on 11/05/2014

NOTE: Unfinished so far, to be migrated to  HDF5 (pytables) in order to achieve better performance

@author: davidreyblanco
'''

import pandas as pd
import numpy as np
import time
import config
#from pandas.io.pytables import Term
import sqlite3

# Disable messages appearing
sqlite3.enable_callback_tracebacks(False)

chunk_size = 30
sample_searches = pd.read_csv(config.data_folder + 'searches.csv.bz2',sep='^',iterator=True,chunksize=chunk_size)
record_count = 0
index = 0
block_limit = 5
t_0 = time.time()
aggregated = None
# Open the database
conn = sqlite3.connect(config.data_folder + 'bookings.db')  
c = conn.cursor()
# For each Chunk in datataset
for chunk in sample_searches:
    t0 = time.time()
    index = index + 1
    record_count = index * chunk_size
    origin = chunk['Origin']
    destination = chunk['Destination']
    date = chunk['Date']
    chunk['generate_booking'] = 0
    for i, row in enumerate(chunk.values):
        key_date = date[i].replace('-','');
        key_origin = origin[i]
        key_destination = destination[i]        
        query = 'SELECT n_txn from bookings WHERE ts = ' + key_date + ' AND dep="' + key_origin + '" AND arr="' + key_destination + '";'
        c.execute(query)
        if c.rowcount > 0:  # If there is a booking mark this search as one generating a booking
            chunk['generate_booking'][i] = 1
    # Save chunk
    if (index == 1):
        chunk.to_csv(config.data_folder + 'searches.bookings.csv',sep='^',mode='w',index=False)
    else:
        chunk.to_csv(config.data_folder + 'searches.bookings.csv',sep='^',mode='a',index=False)
    t1 = time.time()
    lapse = (t1-t0)
    print("Index: " , index , " Record count (K-records):" , record_count/1000,'time:',lapse,' ETA:',(t1-t_0),' tp (rec/sec):',chunk_size/lapse)
    if (block_limit > 0 and index > block_limit) : break 

conn.close()

# Create an index containing the bookings with the following data (date,origin,destination,bookings) 
def create_sqlite_bookings_index():
    # Create a SQLite database
    conn = sqlite3.connect(config.data_folder + 'bookings.db')    
    
    c = conn.cursor()
    # Create the schema if it does not exists
    c.execute('CREATE TABLE IF NOT EXISTS bookings (ts integer,dep char(3),arr char(3),n_txn integer,PRIMARY KEY (ts,dep,arr));')
    c.execute('DELETE FROM bookings;')
    
    debug_on = True     # For debugging
    chunk_size = 30000
    sample_bookings = pd.read_csv(config.data_folder + 'bookings.csv.bz2', usecols=['act_date           ','dep_port','arr_port'], compression='bz2',sep='^',iterator=True,chunksize=chunk_size)
    record_count = 0
    index = 0
    block_limit = 0
    t_0 = time.time()
    # For each Chunk in datataset
    for chunk in sample_bookings:
        t0 = time.time()
        index = index + 1
        record_count = index * chunk_size
        dates = chunk['act_date           ']
        departure = chunk['dep_port']
        arriving = chunk['arr_port']
        for i, row in enumerate(chunk.values):
            key = dates[i].replace('-','').replace(' ','').replace(':','')[0:8];
            q = 'UPDATE OR IGNORE bookings set n_txn = n_txn + 1 WHERE ts = '+key+' AND dep = "'+departure[i].strip() +'" AND arr = "'+arriving[i].strip()+'";'
            try:
                c.execute(q)
            except:
                print 'error'
            #print q
            q = 'INSERT OR IGNORE into bookings values ('+key+',"'+departure[i].strip() +'","'+arriving[i].strip() +'",1);'
            try:
                c.execute(q)
            except:
                print 'error'
            #print q
            c.execute(q)
        conn.commit()
        t1 = time.time()
        lapse = (t1-t0)
        if debug_on:
            print("Index: " , index , " Record count (K-records):" , record_count/1000,'time:',lapse,' ETA:',(t1-t_0),' tp (rec/sec):',chunk_size/lapse)
        if (index > block_limit) : break 
    
    conn.commit()
    c.close()

'''
# create index
def create_hdfs_index():
    store = pd.HDFStore('/Users/davidreyblanco/Desktop/data/bookings_store.h5')
    debug_on = True # For debugging
    chunk_size = 30000
    #usecols=['act_date           ','dep_port','arr_port']
    sample_bookings = pd.read_csv('/Users/davidreyblanco/Desktop/data/bookings.csv.bz2', usecols=['act_date           ','dep_port','arr_port'], compression='bz2',sep='^',iterator=True,chunksize=chunk_size)
    record_count = 0
    index = 0
    block_limit = 2
    t_0 = time.time()
    # For each Chunk in datataset
    for chunk in sample_bookings:
        t0 = time.time()
        index = index + 1
        record_count = index * chunk_size
        # insert data in the chunk for minute
        chunk['d_key'] = chunk['act_date           '].apply(lambda x : x.replace('-','').replace(' ','').replace(':','')[0:12])
        chunk.reindex(['d_key'])
        chunk
        store.append('bookings',chunk)
        # Save chunk in hdf5
        t1 = time.time()
        lapse = (t1-t0)
        if debug_on:
            print("Index: " , index , " Record count (K-records):" , record_count/1000,'time:',lapse,' ETA:',(t1-t_0),' tp (rec/sec):',chunk_size/lapse)
        if (index > block_limit) : break 

create_index()
'''