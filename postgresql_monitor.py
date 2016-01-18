#!/usr/bin/env python
#
# A tool to monitor any changes for a postgresql database, written in python
#
# How to use:
# Set you own settings in settings.json, wich should be located at the same folder as this file. 
# If you want to change settings, you don't need to restart the script, it take effect immediately. 
# 
# Author: yanzexuan@163.com
# Date: created in 2016.1
# 

import psycopg2 # easy_install psycopg2
import sys
import os
import json
from datetime import datetime
from time import sleep


# write contents to a file
class FileWriter:
    def __init__(self, filename):
        self.fileName = filename
        self.fileContent = ""

    def append(self, line):
        self.fileContent += "\n"
        self.fileContent += line;
        
    def writeFile(self):
        with open(self.fileName, "a") as f:
            f.write(self.fileContent)
            self.fileContent = ""
        print "Wrote to: %s" % self.fileName


# stores tables/views for db
class DbSnapshot:
    def __init__(self):
        self.startTime = datetime.now()
        self.tables = [] # (name, records count, records)
        self.views = [] # (name, records count, records)
        pass
    
    def flush(self):
        self.dirName = "/tmp/pg_%s/" % self.startTime.strftime("%Y-%m-%d-%H:%M:%S")
        os.mkdir(self.dirName)
        for t in self.tables + self.views:
            if t[2]: # if table content not none
                fw = FileWriter(self.dirName + t[0] + ".txt")
                fw.writeFile()


def findTableByName(tables, tableName):
    for t in tables:
        if t[0] == tableName:
            return t
    return None


def isTableIncluded(tableName):
    return ((OnlyDetectTablesAndViews is None or len(OnlyDetectTablesAndViews) == 0) or (tableName in OnlyDetectTablesAndViews)) \
        and (tableName not in ExcludedTablesAndViews)


# compare two snapshots
# assume s2 is newer than s1, thus this method can detect db changes
def compareSnapshots(s1, s2):
    #print "Snapshot1 started time: %s, Snapshot2 started time: %s" % (s1.startTime, s2.startTime)
    if len(s1.tables) != len(s2.tables):
        print "Table count: %d, %d" % (len(s1.tables), len(s2.tables))
    if len(s1.views) != len(s2.views):
        print "Table count: %d, %d" % (len(s1.views), len(s2.views))
    for t1 in s1.tables + s1.views:
        tableName = t1[0]
        if not isTableIncluded(tableName):
            continue
        t2 = findTableByName(s2.tables + s2.views, tableName)
        if not t2: # means table/view is deleted
            print "Table deleted: " + tableName
            continue
        if t1[1] != t2[1]:
            print "%s has different records count: %d -> %d" % (tableName, t1[1], t2[1])
        rows1 = t1[2]
        rows2 = t2[2]
        # the compare is not so smart, but let it be
        if rows1 != rows2:
            print "%s has different records content: " % (tableName)
            if type(rows1) != list or type(rows2) != list:
                print str(rows1) + " -> " + str(rows2)
            else:
                i = 0
                while len(rows1) > i and len(rows2) > i:
                    if rows1[i] != rows2[i]:
                        print str(rows1[i]) + " -> " 
                        print str(rows2[i])
                    i += 1
                while len(rows1) > i:
                    print str(rows1[i]) + " -> None"
                    i += 1
                while len(rows2) > i:
                    print " -> None" + str(rows2[i])
                    i += 1
            
    # find if any new table/view created
    for t2 in s2.tables + s2.views:
        tableName = t2[0]
        if not isTableIncluded(tableName):
            continue
        t1 = findTableByName(s1.tables + s1.views, tableName)
        if not t1:
            print "Table created: " + tableName


db_database = None
db_user = None
db_password = None
db_snapshorts =[]
OnlyDetectTablesAndViews = None
ExcludedTablesAndViews = None
ExecuteTimeInterval = 5.0

def loadSettings():
    path = os.path.dirname(os.path.abspath(__file__)) + '/settings.json'
    try:
        with open(path, 'r') as f:
            # read info from json
            settings = json.loads(f.read())
            global db_database, db_user, db_password, OnlyDetectTablesAndViews, ExcludedTablesAndViews, ExecuteTimeInterval
            db_database = settings["database"]
            db_user = settings["user"]
            db_password = settings["password"]
            OnlyDetectTablesAndViews = settings["OnlyDetectTablesAndViews"] if settings.has_key("OnlyDetectTablesAndViews") else None
            ExcludedTablesAndViews = settings["ExcludedTablesAndViews"] if settings.has_key("ExcludedTablesAndViews") else None
            ExecuteTimeInterval = float(settings["ExecuteTimeInterval"]) if settings.has_key("ExecuteTimeInterval") else 5.0
    except Exception as e:
        print str(e)

def tryReadTableOrView(cursor, tableName):
    tableContent = None
    try:
        if isTableIncluded(tableName):
            cursor.execute("select * from " + tableName)
            tableContent = cursor.fetchall()
            tableContent.sort()
    except Exception as e: 
        print ('Error: %s' % str(e))
        tableContent = str(e)
    return tableContent

def do():
    db_con = None
    try:
        loadSettings()
        
        dbSnapshot = DbSnapshot()

        #Create a database session
        db_con = psycopg2.connect(database=db_database, user=db_user, password=db_password)
    
        #Create a client cursor to execute commands
        cursor = db_con.cursor()
        cursor.execute("SELECT * FROM pg_catalog.pg_tables WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema'")
        rows = cursor.fetchall()
        rows.sort()
        for row in rows:
            table_name = row[1]
            cursor.execute("select count(*) from " + table_name)
            count = cursor.fetchone()[0]
            tableContent = tryReadTableOrView(cursor, table_name)
            dbSnapshot.tables.append((table_name, count, tableContent))
    

        cursor.execute("SELECT * FROM pg_catalog.pg_views WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema'")
        rows = cursor.fetchall()
        rows.sort()
        for row in rows:
            table_name = row[1]
            cursor.execute("select count(*) from " + table_name)
            count = cursor.fetchone()[0]
            tableContent = tryReadTableOrView(cursor, table_name)
            dbSnapshot.views.append((table_name, count, tableContent))

        global db_snapshorts
        db_snapshorts.append(dbSnapshot)
        snapshotsCount = len(db_snapshorts)
        if (snapshotsCount > 1):
            compareSnapshots(db_snapshorts[snapshotsCount - 2], db_snapshorts[snapshotsCount - 1])
        if (snapshotsCount >= 100):
            tmp = []
            tmp.append(db_snapshorts[snapshotsCount - 1])
            del db_snapshorts[0 : snapshotsCount]
            db_snapshorts = tmp

    except psycopg2.DatabaseError as e: 
        print ('Error: %s' % e)
    finally:
        if db_con:
            db_con.close()

    sleep(ExecuteTimeInterval)

if __name__ == "__main__":
    print "Process started..."
    while True:
        do()
        sys.stdout.write(".") # print . in one line, doesn't work this way?! print ".",
        sys.stdout.flush() # force terminal to update
    print "Process ended..."
