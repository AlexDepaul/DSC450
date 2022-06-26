#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jun 10 19:09:17 2022

@author: alex
"""


#FINAL  


#Import url link ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
import json
import urllib
import time
tweetlink = 'http://dbgroup.cdm.depaul.edu/DSC450/OneDayOfTweets.txt'
webFD = urllib.request.urlopen(tweetlink)


#Part1a ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def part1a50k():
    'this function will write 50k of the tweets to a .txt file'
    print('starting text file for 50k tweets: ')
    startTime = time.time() #get time from start
    infile = open('Part1a50k.txt', 'w')
    
    for i in range(50000): #writing 50,000 lines to .txt file
        tweetinfo = webFD.readline().decode('utf8')
        #print(tweetinfo)
        #tweetinfo = json.loads(tweetinfo)
        #print(tweetinfo)
        infile.write(tweetinfo + '\n')
        #infile.write('/n')
    
    endTime = time.time()#get time when over
    elapsedtime = endTime - startTime #calculate elapsed time
    infile.close() #close text file
    print('Elapsed Time: ' + str(elapsedtime) + ' seconds.') #print the elapsed time
    
        
def part1a250k():
    'this function will write 250k tweets to a .txt file and calculate the time it takes'
    print('starting text file for 250k tweets: ')
    startTime = time.time() #get start time
    infile = open('Part1a250k2.txt', 'w')
    
    for i in range(250000):
        tweetinfo = webFD.readline().decode('utf8') #get tweet
        infile.write(tweetinfo + '\n') #write tweet to line
        
    endTime = time.time() #get time when function over
    elapsedtime = endTime - startTime
    infile.close()
    print('Elapsed Time: ' + str(elapsedtime) + ' seconds.')
        
'''  
        
friendcountList = []

#tdata is our read data
for line in tdata:
    try:
        tweetd = json.loads(line)
        fcount = tweetd['user']['friends_count']
        if fcount not in friendcountList:
            friendcountList.append(fcount)
        else: 
            pass
        
    except:
        pass

endD = time.time()
print('part1d. Time Elapsed: ' + str(endD-startD)+ ' seconds')
'''

#Tables ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

import sqlite3

usertable = '''CREATE TABLE TweetUser(

    id int,
    name text,
    screen_name text,
    description text,
    friends_count int,
    
    CONSTRAINT User_PK PRIMARY KEY (id)

    );'''

tweetstable = '''CREATE TABLE TweetsTable (

    id_str int,
    created_at text,
    text text,
    source text,
    in_reply_to_user_id int,
    in_reply_to_screen_name text,
    in_reply_to_status_id int,
    retweet_count int,
    contributors text,
    user_id int,
    geo_id int,
    
    CONSTRAINT Tweets_pk PRIMARY KEY (id_str),
    
    CONSTRAINT tweets_fk1 FOREIGN KEY (user_id) REFERENCES TweetUser(id),
    
    Constraint tweets_fk2 FOREIGN KEY (geo_id) REFERENCES GEOTable(ID)
    );'''
#geo_id text, Flipped geo and user id
    
    
GEOTable = '''CREATE TABLE GEOTable(
    id int,
    type text,
    longitude text,
    latitude text,
    
    CONSTRAINT GEO_PK PRIMARY KEY (id)

    );'''

dt = 'Drop TABLE IF EXISTS TweetsTable;'
duser = 'DROP TABLE IF EXISTS TweetUser;'
dGeo = 'DROP TABLE IF EXISTS GEOTable;'

#queries
insertUserQuery = 'INSERT OR IGNORE INTO TWEETUSER VALUES(?,?,?,?,?)'
insertTweetQuery = 'INSERT OR IGNORE INTO TweetsTable VALUES(?,?,?,?,?,?,?,?,?,?,?)'
insertGeoQuery = 'INSERT OR IGNORE INTO GEOTable VALUES(?,?,?,?)'

#part1b~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#tweetlink = 'http://dbgroup.cdm.depaul.edu/DSC450/OneDayOfTweets.txt'
#webFD = urllib.request.urlopen(tweetlink)

def part1b(tweetamount):
    'populate sqlite tables with tweet amount'
    conn = sqlite3.connect('dsc450FinalTweets.db') #connect to db
    cursor = conn.cursor()
    
    cursor.execute(dt) #drop tables if they already exist
    conn.execute(duser)
    conn.execute(dGeo)
    conn.execute(usertable) #build tables if they already exist
    conn.execute(tweetstable)
    conn.execute(GEOTable)
    
    startTime = time.time() #timing
    
    uList = [] #temporary variable storage to be entered into tables
    tList = []
    gList = []
    geoID = 0
    for i in range(tweetamount): #run several times 50k and 250k tweets
        
        try:
            tweetData = webFD.readline().decode('utf8')
            tweet = json.loads(tweetData)
            
        except: 
            pass
        else:
            userData = []
            userPoints = ['id', 'name','screen_name','description','friends_count']
            userDict = tweet['user']
            
            for point in userPoints:
                if userDict[point] == 'null' or userDict[point] == '':
                    userData.append(None)
                
                else:
                    userData.append(userDict[point])
                    
            uList.append(userData)
                
            tweetData = []
            tweetPoints = ['id_str','created_at','text','source','in_reply_to_user_id','in_reply_to_screen_name','in_reply_to_status_id','retweet_count','contributors'] #,'geo_id','user_id'] 
            geoID += 1
            
            for point in tweetPoints:
                if tweet[point] == 'null' or tweet[point] == '':
                    tweetData.append(None)
                else:
                    tweetData.append(tweet[point])
                    #print(tList)    
                    #geoData = []
                    #geoDict = tweet['geo']
                    #tweetData.append(userData[0])
            tweetData.append(userData[0])    
            if tweet ['geo']:
                gList.append((geoID, tweet['geo']['type'], tweet['geo']['coordinates'][0],tweet['geo']['coordinates'][1]))
                             
            tweetData.append(geoID)
            tList.append(tweetData)
            #print(tList)
                  
    conn.executemany('INSERT OR IGNORE INTO TweetUser VALUES(?,?,?,?,?)',uList)
    conn.executemany(insertTweetQuery,tList)
    conn.executemany('INSERT OR IGNORE INTO GEOTable VALUES(?,?,?,?)',gList)
    
    conn.commit()
    print('User Count: ')
    print(conn.execute('select count(*) from TweetUser').fetchall())
    print('Tweet Count: ')
    print(conn.execute('select count(*) from TweetsTable').fetchall())
    print('Geo Count:')
    print(conn.execute('select count(*) from GEOTable').fetchall())
    
    
    endTime = time.time()
    elapsedtime = endTime - startTime
    print('Elapsed Time: ' + str(elapsedtime) + ' seconds.')
        
    conn.close()


#part1C~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def part1c(tweetamount):
    'This function will read tweets from a .txt file to populate a table'
    
    infile = open('part1a250k.txt','r')
    conn = sqlite3.connect('dsc450FinalTweets.db')
    cursor = conn.cursor()
    cursor.execute(dt)
    conn.execute(dt)
    conn.execute(duser)
    conn.execute(dGeo)
    conn.execute(usertable) #build tables if they already exist
    conn.execute(tweetstable)
    conn.execute(GEOTable)
    
    startTime = time.time() #timing
    
    uList = [] #temporary variable storage to be entered into tables
    tList = []
    gList = []
    geoID = 0
    
    for line in range(tweetamount):
        
        
        try: 
            tweetData = infile.readline()#.decode('utf8')
            tweet = json.loads(tweetData)        
        except: 
            pass
        else:
            userData = []
            userPoints = ['id', 'name','screen_name','description','friends_count']
            userDict = tweet['user']
            
            for point in userPoints:
                if userDict[point] == 'null' or userDict[point] == '':
                    userData.append(None)
                
                else:
                    userData.append(userDict[point])
                    
            uList.append(userData)
                
            tweetData = []
            tweetPoints = ['id_str','created_at','text','source','in_reply_to_user_id','in_reply_to_screen_name','in_reply_to_status_id','retweet_count','contributors'] #,'geo_id','user_id'] 
            geoID += 1
            
            for point in tweetPoints:
                if tweet[point] == 'null' or tweet[point] == '':
                    tweetData.append(None)
                else:
                    tweetData.append(tweet[point])

            tweetData.append(userData[0])    
            if tweet ['geo']:
                gList.append((geoID, tweet['geo']['type'], tweet['geo']['coordinates'][0],tweet['geo']['coordinates'][1]))
                             
            tweetData.append(geoID)
            tList.append(tweetData)
            #print(tList)
                  
    conn.executemany('INSERT OR IGNORE INTO TweetUser VALUES(?,?,?,?,?)',uList)
    conn.executemany(insertTweetQuery,tList)
    conn.executemany('INSERT OR IGNORE INTO GEOTable VALUES(?,?,?,?)',gList)
    
    conn.commit()
    #print('User Count: ')
    #print(conn.execute('select count(*) from TweetUser').fetchall())
    endTime = time.time()
    elapsedtime = endTime - startTime
    print('Elapsed Time: ' + str(elapsedtime) + ' seconds.')
    
    infile.close()    
    conn.close()       





#part1D~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def part1d(tweetamount):
    'populate sqlite tables with tweet amount'
    conn = sqlite3.connect('dsc450FinalTweets.db') #connect to db
    cursor = conn.cursor()
    
    cursor.execute(dt) #drop tables if they already exist
    conn.execute(duser)
    conn.execute(dGeo)
    conn.execute(usertable) #build tables if they already exist
    conn.execute(tweetstable)
    conn.execute(GEOTable)
    
    startTime = time.time() #timing
    
    uList = [] #temporary variable storage to be entered into tables
    tList = []
    gList = []
    geoID = 0
    #batch = []
    
    for i in range(tweetamount): #run several times 50k and 250k tweets
        
        try:
            tweetData = webFD.readline().decode('utf8')
            tweet = json.loads(tweetData)
            
        except: 
            pass
        else:
            userData = []
            userPoints = ['id', 'name','screen_name','description','friends_count']
            userDict = tweet['user']
            
            for point in userPoints:
                if userDict[point] == 'null' or userDict[point] == '':
                    userData.append(None)
                
                else:
                    userData.append(userDict[point])
                    
            uList.append(userData)
                
            tweetData = []
            tweetPoints = ['id_str','created_at','text','source','in_reply_to_user_id','in_reply_to_screen_name','in_reply_to_status_id','retweet_count','contributors'] #,'geo_id','user_id'] 
            geoID += 1
            
            for point in tweetPoints:
                if tweet[point] == 'null' or tweet[point] == '':
                    tweetData.append(None)
                else:
                    tweetData.append(tweet[point])
                    #print(tList)    
                    #geoData = []
                    #geoDict = tweet['geo']
                    #tweetData.append(userData[0])
            tweetData.append(userData[0])    
            if tweet ['geo']:
                gList.append((geoID, tweet['geo']['type'], tweet['geo']['coordinates'][0],tweet['geo']['coordinates'][1]))
                             
            tweetData.append(geoID)
            tList.append(tweetData)
            
            if len(tList) > 4000: #our batch loading
                conn.executemany(insertTweetQuery,tList)
                tList = []
                
            if len(uList) > 4000:
                cursor.executemany('INSERT OR IGNORE INTO TweetUser VALUES(?,?,?,?,?)',uList)
                uList = []
                
            if len(gList) > 100:   
                conn.executemany('INSERT OR IGNORE INTO GEOTable VALUES(?,?,?,?)',gList)
                gList = []

    conn.commit()
       
    endTime = time.time()
    elapsedtime = endTime - startTime
    print('Elapsed Time: ' + str(elapsedtime) + ' seconds.')
        
    conn.close()

#part1e~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#import matplotlib as mpl
import matplotlib.pyplot as plt
#import pandas
#import numpy as np

def part1e():
    
    parta50 = 11.03
    parta250 = 53.29
    partb50 = 9.53
    partb250 = 40.65
    partc50 = 4.3
    partc250 = 17.37
    partd50 = 8.98
    partd250 = 38.95
    
    parta = [parta50, parta250]
    partb = [partb50, partb250]
    partc = [partc50, partc250]
    partd = [partd50, partd250]
    
    tweetnum = [50000,250000]
    
    print('Plot 1a: ')
    plt.scatter(tweetnum, parta, color = 'red')
    plt.scatter(tweetnum, partb, color = 'blue')
    plt.scatter(tweetnum, partc, color = 'Green')
    plt.scatter(tweetnum, partd, color = 'orange')
    
    plt.xticks([0, 50000, 100000, 150000, 200000, 250000, 300000])
    plt.title('Time vs. Querying Daily tweets')
    plt.xlabel('Tweets')
    plt.ylabel('Time (seconds)')
    
    plt.show()
    
    
#part2a~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def part2a():
    'this will query for the user_id, avg latitude and long'
    #NOTE Make sure DB IS operational before testing function
 
    geoQuery = '''
                SELECT user_id, avg(longitude), avg(latitude)
                From TweetsTable, GEOTable
                WHERE TweetsTable.geo_id = GeoTable.ID
                GROUP BY user_id;
                
                '''
    #our test query            
    #geoTestquery = '''Select Count(id) From GEOTable;'''
                    
    conn = sqlite3.connect('dsc450FinalTweets.db') #connect to db
    cursor = conn.cursor()
   
    cursor.execute(geoQuery).fetchall() #EXECUTE QUERY 
    print('-----------------------------------------------------')
    print(cursor.execute(geoQuery).fetchall()) #PRINT QUERY

    conn.close()
    
def part2b(runs):
    'part2b function will a query in a for loop x amount of times(runs = x)'
    conn = sqlite3.connect('dsc450FinalTweets.db') #connect DB
    cursor = conn.cursor()    
    #The query 
    geoQuery = '''
                SELECT user_id, avg(longitude), avg(latitude)
                From TweetsTable, GEOTable
                WHERE TweetsTable.geo_id = GeoTable.ID
                GROUP BY user_id;
                
                '''
    startTime = time.time() #timing
    
    for i in range(runs): #for loop iterate through our code
        cursor.execute(geoQuery).fetchall()
    
    endTime = time.time()
    ElapsedTime = endTime - startTime #finish timing and print output
    print('Finished running query: ' + str(runs) + ' and it took ' + str(ElapsedTime) + ' seconds.')
    
    conn.close()
    
#cursor.execute(geoQuery)fetchall()


#part2c~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#first read the txt file
#buid a dictionary key will be user ID, then values will be average long and latitude
    
    #going to need to get an id, lat and long coordinates
    
from statistics import mean

def part2c():
    'this functon will open and read a .txt file and return the equivelant of sql query'
    'in this case it will be a dictionary with keys of ids and values of avg long and latitude'
    infile = open('part1a250k2.txt', 'r') #open file
    
    tweetInfo = infile.readlines() #read file
    
    infile.close() #close file
    
    latDict = {} #dict to get a list of or latitudes for each id
    longDict = {}#dict to get a list of or longitudes for each id
    avgDict = {}#this will be our final dictionary including averages
    
    
    for line in tweetInfo: #start our loop to read lines and start getting our query values
        
        try:
            data = json.loads(line)
        except:
            pass
        
        if data['geo']: # if the tweet has geo add username to our dictionary 
            
            user = data['user']['id'] #get the user
            
            latDict[user] = data['geo']['coordinates'][0] #add user and lat to lat dictionary
            longDict[user] = longDict[user] = data['geo']['coordinates'][1] #add user and long coord. to long dictionary
             
    #calculate average lats for each user

    
    for key in latDict: #since noth dict have the same key should be able to calc long with this loop as well
      
        #print(key)
        lats = [] #list of our lat values
        longs = [] #list of our long values 
        lats.append(int(latDict[key])) #add values from dictionary to a list
        longs.append(int(longDict[key])) #add values from dictionary to a list
        avglat = mean(lats) #get the average coordinates
        avglong = mean(longs) #get the average coordinates
        
        avgDict[key] = [avglat, avglong] #add these to our final dictionary
        

    return(avgDict) #print/return our dictionary
 
        
#part2d~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~    


def part2d(runs):
    'this functon will open and read a .txt file and return the equivelant of sql query'
    'in this case it will be a dictionary with keys of ids and values of avg long and latitude'
    infile = open('part1a250k2.txt', 'r') #open file
    
    tweetInfo = infile.readlines() #read file
    
    infile.close() #close file
    
    latDict = {} #dict to get a list of or latitudes for each id
    longDict = {}#dict to get a list of or longitudes for each id
    avgDict = {}#this will be our final dictionary including averages
    
    startTime = time.time()
    for i in range(runs):
        for line in tweetInfo: #start our loop to read lines and start getting our query values
        
            try:
                data = json.loads(line)
            except:
                pass
        
            if data['geo']: # if the tweet has geo add username to our dictionary 
            
                user = data['user']['id'] #get the user
            
                latDict[user] = data['geo']['coordinates'][0] #add user and lat to lat dictionary
                longDict[user] = longDict[user] = data['geo']['coordinates'][1] #add user and long coord. to long dictionary
             
    #calculate average lats for each user

    
        for key in latDict: #since noth dict have the same key should be able to calc long with this loop as well
      
            #print(key)
            lats = [] #list of our lat values
            longs = [] #list of our long values 
            lats.append(int(latDict[key])) #add values from dictionary to a list
            longs.append(int(longDict[key])) #add values from dictionary to a list
            avglat = mean(lats) #get the average coordinates
            avglong = mean(longs) #get the average coordinates
        
            avgDict[key] = [avglat, avglong] #add these to our final dictionary
        

       #return(avgDict) #print/return our dictionary
 
    endTime = time.time()
    elapsedtime = endTime - startTime
    
    print('Ran: ' + str(runs) + ' times, it took: ' + str(elapsedtime) + 'seconds.')



#part2e~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#tweetlink = 'http://dbgroup.cdm.depaul.edu/DSC450/OneDayOfTweets.txt'
#webFD = urllib.request.urlopen(tweetlink)
import re

def part2e():
    
   # tweetLine = webFD.readline()
   # tstring = tweetLine.decode()
    Data2e = {}
    
    startTimee = time.time()
    
    for tweet in range(250000):
        
        tweetLine = webFD.readline()
        tstring = tweetLine.decode()
        #tdict = json.loads(tweetLine.decode())
        #print(tdict)
        #test user id
        regex1 = re.compile('id_str":"[^"]*"')
        Userid = regex1.findall(tstring)
        regeo = re.compile('geo":"[^"]*"')
        rgeo = regeo.findall(tstring)
        regCord = re.compile('coordinates":"[^"]*"')
        rcoord = regCord.findall(tstring)
        
        '''
        print(Userid)
        print('_-------------------------------------__')
        print(rgeo)
        print('_-------------------------------------__')
        print(rcoord)
        '''
        
        if rgeo == None or rcoord == None:
            pass
        else:
            Data2e[Userid[0]] = rcoord
            #regCord = re.compile('coordinates":"[^"]*"')
            #rcoord = regCord.findall(tstring)
            #print(rcoord)
        
        #Data2e[Userid] = rcoord
        #print(Userid)
        #print('_-------------------------------------__')
        #print(rgeo)
        #print('_-------------------------------------__')
       # print(rcoord)
        
       # print(' ')
        #print(regex1.findall(tstring))
        
        #if regex1.findall(tstring) == 'null':
        #    print('no geo')
        
        #if regex1.findall(t)
       # print(regex1.findall(tstring))
    
        #tweetinfo = webFD.readline().decode('utf8')
    endTimee = time.time()
    elapsedTime = endTimee - startTimee
    print('Elapsed Time: ' + str(elapsedTime) + ' seconds.')
    return(elapsedTime)

def part2f(runs):
    'this will be the one function that uses a secondary function (part2e) to get the '
    
    TimesList = []
    #will use a for loop to run it through runs amount of iterations
    #startTimef = time.time()
    for i in range(runs):
        x = part2e()
        print(x)
        TimesList.append(x)
        
    #endTimef = time.time()
    elapsedTime = sum(TimesList)
    print('Elapsed Time is : ' + str(elapsedTime) + ' seconds, for ' + str(runs) + ' runs.')
    
        




#part3a~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


def part3a():
    'we will create a new table that corresponds to all 3 tables in the twitter DB'
    'part2b function will a query in a for loop x amount of times(runs = x)'
    conn = sqlite3.connect('dsc450FinalTweets.db') #connect DB
    #cursor = conn.cursor()    
    
    #the query to build our new table
    viewQuery = ''' 
                CREATE TABLE tweets3a AS
                SELECT * FROM TweetsTABLE
                LEFT JOIN tweetUSER ON tweetUSER.id = TweetsTable.user_id 
                LEFT JOIN GEOTABLE ON GEOTable.id = TweetsTable.geo_id
                '''
    conn.execute('Drop TABLE IF EXISTS tweets3a;') #DRop the table incase
    conn.execute(viewQuery) #construct view as table
    
    conn.commit() # Commit than Close
    conn.close()




def part3b():
    'this will take the data from 3a and save it to a json file'
    outfile = open('finalJson.json', 'w', encoding='utf8')#open file to write in
    conn = sqlite3.connect('dsc450FinalTweets.db') #connect to db
    
    # our dictionary template
    '''
    jdict = {'id': None, 'created_at': None, 'text': None, 'reply_id': None, 'reply_sn': None,
             'reply_status': None, 'rt_count': None, 'Contributors': None, 'user_id': None, 'geoID': None,
             'id': None, 'name': None, 'screen_name': None, 'descriptor': None, 'friends': None, 'id2': None,
             'type': None, 'long': None, 'lat': None}
    '''
    
    tableData = conn.execute('SELECT * FROM tweets3a;').fetchall() #how many rows we have on the table
    for row in tableData: #cycle through all the rows 
        
        #add them to our dictionary 
        jdict = {'id_str': row[0], 'created_at': row[1], 'text': row[2], 'source': row[3], 'reply_id': row[4], 'reply_sn': row[5],
             'reply_status': row[6], 'rt_count': row[7], 'Contributors': row[8], 'user_id': row[9], 'geoID': row[10],
             'id': row[11], 'name': row[12], 'screen_name': row[13], 'descriptor': row[14], 'friends': row[15], 'id_2': row[16],
             'type': row[17], 'long': row[18], 'lat': row[19]}
        
        
        jsonString = json.dumps(jdict) #put into json
        outfile.write(jsonString) #write to file
        #print(jdict)
        #print('_---------------------------------------------------')
        jdict = {} #reset dictionary
                              
    outfile.close()
    conn.close()
    


def part3c():
     'this function will export the table from 3-a into a csv file'  
     outfile = open('final2CSV.csv', 'w') #open our outfile where we write data too
     conn = sqlite3.connect('dsc450FinalTweets.db') #connect to the sqlite db
     cursor = conn.cursor() #set up corsor
     content = cursor.execute('SELECT * FROM tweets3a;').fetchall() #get all our data from our rows
     for row in content: #for loop to cycle through the rows
         
         outfile.write(str(row) + '\n') #write our content to our csv file
         
     outfile.close() #close .csv file
     conn.close() #disconnect from sqlite db
       
       
       
       
       
       
       
       
       
