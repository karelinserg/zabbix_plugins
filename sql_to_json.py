#!/usr/bin/env python

import MySQLdb
import sys

dbhost = 'ip'
dbuser = 'user'
dbpassword = 'password'
i = 0
comma = ","

mydb = MySQLdb.connect(host = dbhost, user = dbuser, passwd = dbpassword, db = 'db', charset='utf8')
cur = mydb.cursor()

cur.execute("sql")

res = cur.fetchall()
count = len(res) - 1

print "{\n   \"data\": ["

for data in cur:
    type_id = str(data[0])
    type_name = (data[1])
    type_name.encode('utf-8')
    res1 = "\t{\n\t\t\"{#TYPE_ID}\":\""+type_id+"\","
    res2 = "\t\t\"{#TYPE_NAME}\":\""+type_name+"\"\n\t}"
    if i < count:
        print res1
        print res2+comma
        i = i + 1
    else:
        print res1
        print res2

print "   ]\n}"

mydb.close()
cur.close()
