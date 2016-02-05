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

# utils class
class Utils:
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

    BLACK = '\033[0;30m'
    RED = '\033[0;31m'
    GREEN = '\033[0;32m'
    BROWN = '\033[0;33m'
    BLUE = '\033[0;34m'
    PURPLE = '\033[0;35m'
    CYAN = '\033[0;36m'
    GREY = '\033[0;37m'

    DARK_GREY = '\033[1;30m'
    LIGHT_RED = '\033[1;31m'
    LIGHT_GREEN = '\033[1;32m'
    YELLOW = '\033[1;33m'
    LIGHT_BLUE = '\033[1;34m'
    LIGHT_PURPLE = '\033[1;35m'
    LIGHT_CYAN = '\033[1;36m'
    WHITE = '\033[1;37m'

    RESET = "\033[0m"

    @staticmethod
    def highlight(text):
        return Utils.BOLD + text + Utils.RESET

    @staticmethod
    def red(text):
        return Utils.RED + text + Utils.RESET

    @staticmethod
    def green(text):
        return Utils.GREEN + text + Utils.RESET

    @staticmethod
    def gray(text):
        return Utils.GREY + text + Utils.RESET

    @staticmethod
    def strike(text):
        return u'\u0336'.join(text) + u'\u0336'


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


# find the table from tables by table name
def findTableByName(tables, tableName):
    for t in tables:
        if t[0] == tableName:
            return t
    return None


# check if we should care about this table/view. A table/view can be excluded by settings.json
def isTableIncluded(tableName):
    return ((OnlyDetectTablesAndViews is None or len(OnlyDetectTablesAndViews) == 0) or (tableName in OnlyDetectTablesAndViews)) \
        and (tableName not in ExcludedTablesAndViews)


# compare two table records, compare fields one by one so we can find which field is changed
# records are tuples (or list), they should have the same length
# e.g.
# (58L, 62L, 10L, Decimal('1'), Decimal('3'), 'N', 'N') -> 
# (57L, 93L, 15L, Decimal('0'), Decimal('3'), 'N', 'N')
def compareTableRecords(r1, r2):
    if (type(r1) == tuple and type(r2) == tuple and len(r1) == len(r2)):
        string2 = '('
        i = 0
        while i < len(r1):
            # kind of tricky here, because we want to print out with tuple format, 
            # and also want to insert font style. 
            # So we compare each element in tuple,a nd convert that one to a tuple 
            # (that only has one element), just want to keep the output string format
            strTmp = str((r2[i],))
            strTmp = strTmp[1 : len(strTmp) - 2] # delete the '(' in front and ',)' at end
            if r1[i] != r2[i]:
                strTmp = Utils.green(strTmp) # add font style if different
            if 0 != i:
                string2 += ", " # split each item
            string2 += strTmp
            i += 1
        string2 += ')'
        print Utils.gray(str(r1)) + " -> " 
        print string2
    else: # if not tuple or len are different
        print Utils.gray(str(r1)) + " -> " 
        print str(r2)


# compare two snapshots, which contains all tables and views
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
            print "Table deleted: " + Utils.highlight(tableName)
            continue
        if t1[1] != t2[1]:
            print "%s has different records count: %d -> %d" % (Utils.highlight(tableName), t1[1], t2[1])
        rows1 = t1[2]
        rows2 = t2[2]
        if rows1 == rows2: # no change, then continue
            continue

        # the compare is not so smart, but let it be
        print "%s has different records content: " % (Utils.highlight(tableName))
        if type(rows1) != list or type(rows2) != list:
            print str(rows1) + " -> " + str(rows2)
        else:
            i = 0
            while len(rows1) > i and len(rows2) > i:
                if rows1[i] != rows2[i]:
                    compareTableRecords(rows1[i], rows2[i])
                i += 1
            while len(rows1) > i:
                print Utils.red("- " + str(rows1[i])) # deleted items
                i += 1
            while len(rows2) > i:
                print Utils.green("+ " + str(rows2[i])) # newly added items
                i += 1
            
    # find if any new table/view created
    for t2 in s2.tables + s2.views:
        tableName = t2[0]
        if not isTableIncluded(tableName):
            continue
        t1 = findTableByName(s1.tables + s1.views, tableName)
        if not t1:
            print "Table created: " + Utils.highlight(tableName)


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
            db_database = db_user = db_password = OnlyDetectTablesAndViews = ExcludedTablesAndViews = ExecuteTimeInterval = None
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
        print ('DatabaseError: %s' % e)
    except Exception as e: 
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
