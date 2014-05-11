'''
Read files in a bz2-compressed csv file, go through the compressed file counting every line

Total lines in bookings file: 10000011

Created on 10/05/2014

@author: davidreyblanco
'''
import config
import bz2 as bz2

def count_lines_bz2(filename):
    f_bookings = bz2.BZ2File(filename=filename,mode='r')
    lines = 0
    while f_bookings.readline() != '':
        lines = lines + 1
    return lines

lines = count_lines_bz2(config.data_folder + 'bookings.csv.bz2')
print 'Total lines in bookings file:',lines

lines = count_lines_bz2(config.data_folder + 'Desktop/data/searches.csv.bz2')
print 'Total lines in searches file:',lines