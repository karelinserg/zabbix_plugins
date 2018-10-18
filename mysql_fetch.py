#!/usr/bin/env python

import MySQLdb
import sys

if len(sys.argv) <> 4:
    print 'ERROR: Invalid number of arguments'
    sys.exit()

dbhost = 'ip'
dbuser = 'user'
dbpassword = 'password'
arg1 = sys.argv[1]
target = sys.argv[2]
interval = sys.argv[3]

mydb = MySQLdb.connect(host = dbhost, user = dbuser, passwd = dbpassword, db = 'dbname')
cur = mydb.cursor()

cur.execute("SELECT PARTITION_NAME FROM information_schema.PARTITIONS WHERE TABLE_NAME = 'messages_log_channels' AND PARTITION_DESCRIPTION <> 'MAXVALUE' AND PARTITION_DESCRIPTION > (SELECT message_id FROM smsline.messages_log_channels order by message_id desc limit 1) limit 1")

partname = (cur.fetchone()[0])

mydb.close()
cur.close()

mydb = MySQLdb.connect(host = dbhost, user = dbuser, passwd = dbpassword, db = 'dbname')
cur = mydb.cursor()

if arg1 == 'total':
     sql = 'SELECT count(message_state) FROM messages_log_channels partition('+partname+') WHERE target_id = '+target+' AND sent_date_time > date_sub(now(),interval '+interval+' hour)'

elif arg1 == 'rejected':
     sql = 'SELECT count(message_state) FROM messages_log_channels partition('+partname+') WHERE target_id = '+target+' AND sent_date_time > date_sub(now(),interval '+interval+' hour) AND message_state in (SELECT external_id FROM smsline.message_state_channels WHERE message_state_id = "8" and external_id <> "36057")'

elif arg1 == 'quotaisover':
     sql = 'SELECT count(message_state) FROM messages_log_channels partition('+partname+') WHERE target_id = '+target+' AND sent_date_time > date_sub(now(),interval '+interval+' hour) AND message_state = "36057"'
else:
    print 'ERROR: Invalid first argument, use: total|rejected|quotaisover'
    sys.exit()

cur.execute(sql)
result = (cur.fetchone()[0])
print result

mydb.close()
cur.close()
