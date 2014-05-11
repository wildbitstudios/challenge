#!/usr/bin/env python

# ----------------------------------------------------------------------------------------------------------
# Simple RESTful Web Service that returns the list of the Airports with most bookings 
# 
#    Input parameters: n -> the number of airports looked up (ordered from the most booked to the least)
#    Response: (in json format)
#            status -> 'ko' | 'ok'
#            data -> response payload, if error it'll contain a brief description of the issue
#                    if success, it'll contain the list of airports along with the pax booked for each one

#    Examples (for debugging purpases you can use http://jsonlint.com to analyze JSON):
#
#    Sample ok response payload:
#        { "status" : "ok", "data" : "{"arr_port":{"0":"LHR     ","1":"MCO     ","2":"LAX     "},"pax":{"0":88809.0,"1":70930.0,"2":70530.0}}"}
#
#    Sample ko response:
#        { "status" : "ko", "data" : "Parameter n must not be empty and integer"}
#
#    This WS is based on CGI and uses a csv resulted of the bookings file processing
#    for simplicity uses panda for parsing and accessing the data
# ----------------------------------------------------------------------------------------------------------

#    Configuration settings, the following variable contains the path to the file containing the data

import config

airports_data_file = config.data_folder + 'sorted_named_list.csv'

import cgitb
import cgi
import pandas as pd
import numpy as np

cgitb.enable()

# Set response MIME-TYPE
print "Content-Type: text/plain;charset=utf-8"
print

# Init the filter with a default value
record_count=10 

# Init returning payload data
data_payload = ''
status_code = 'ok'

# Get the parameter, checking is a valid integer
arguments = cgi.FieldStorage()
if 'n' in arguments and arguments['n'].value.isdigit():
    record_count = int(arguments['n'].value)
    if (record_count > 0):
        sample_bookings = pd.read_table(airports_data_file, sep='^',nrows=record_count)
        data_payload=sample_bookings.to_json()
    else:
        status_code = 'ko'
        data_payload = '"Record number must be greated than zero"'
else:
    status_code = 'ko'
    data_payload = '"Parameter n must not be empty and integer"'

response = "{ \"status\" : \""+status_code+"\", \"data\" : " + data_payload +"}";

print response
