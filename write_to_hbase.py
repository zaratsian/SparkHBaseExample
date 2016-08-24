
###################################################################################################################################################
#
#   This script will create an HBase table and populate X number of records (based on simulated data)
#
#   http://happybase.readthedocs.io/en/latest/api.html#table
#   https://docs.hortonworks.com/HDPDocuments/HDP2/HDP-2.3.4/bk_installing_manually_book/content/ref-2a6efe32-d0e1-4e84-9068-4361b8c36dc8.1.html
#
#   NOTE: HBase Thrift Server must be running for this to work
#
#   Starting the HBase Thrift Server:
#   Foreground: hbase thrift start -p <port> --infoport <infoport>
#   Background: /usr/hdp/current/hbase-master/bin/hbase-daemon.sh start thrift -p <port> --infoport <infoport> 
#
###################################################################################################################################################


import happybase
import os,sys,csv
import random
import time, datetime


# User Inputs (could be moved to arguments)
hostname            = 'localhost'
port                = 9999
table_name          = 'customer_table'
columnfamily        = 'demographics'
number_of_records   = 1000000
batch_size          = 2000


print '[ INFO ] Trying to connect to the HBase Thrift server at ' + str(hostname) + ':' + str(port)
connection = happybase.Connection(hostname, port=port, timeout=10000)
table = connection.table(table_name)
print '[ INFO ] Successfully connected to the HBase Thrift server at ' + str(hostname) + ':' + str(port)


# https://happybase.readthedocs.io/en/latest/api.html#happybase.Connection.create_table
print '[ INFO ] Creating HBase table:  ' + str(table_name)
time.sleep(1)
families = {
    columnfamily: dict(),  # use defaults
}

connection.create_table(table_name,families)
print '[ INFO ] Successfully created HBase table:  ' + str(table_name)


print '[ INFO ] Inserting ' + str(number_of_records) + ' into ' + str(table_name)
start_time = datetime.datetime.now()
with table.batch(batch_size=batch_size) as b:
    for i in range(number_of_records):
        rowkey  = i 
        custid  = random.randint(1000000,9999999)
        gender  = ['male','female'][random.randint(0,1)]
        age     = random.randint(18,100)
        level   = ['silver','gold','platimum','diamond'][random.randint(0,3)]
        
        b.put(str(rowkey), {b'demographics:custid': str(custid),
                          b'demographics:gender': str(gender),
                          b'demographics:age': str(age),
                          b'demographics:level': str(level)})

print '[ INFO ] Successfully inserted ' + str(number_of_records) + ' into ' + str(table_name) + ' in ' + str((datetime.datetime.now() - start_time).seconds) + ' seconds'


print '[ INFO ] Printing data records from the generated HBase table'
for key, data in table.rows([b'1', b'2', b'3', b'4', b'5']):
    try:   
        print(key, data)  # prints row key and data for each row
    except:
        pass

#ZEND
