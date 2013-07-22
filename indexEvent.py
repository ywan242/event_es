'''
Created on Jun 27, 2013

@author: Yu Wang
'''

import httplib
import sys
import json
import datetime
import MySQLdb as mdb

def getDBConnection():
    con = mdb.connect('tritanium.mathcs.emory.edu', 'eventgenie', 'eventgeniepass', 'eventgenie')
    return con

def getConnection():
    conn = httplib.HTTPConnection("hadoop005.mathcs.emory.edu:9200")
    return conn

def documentExist(conn, eid):
    if not conn:
        conn = getConnection()
        
    conn.request('GET', '/event_index/event/' + str(eid))
    response = conn.getresponse()
    dataResponse = response.read()
    dataJson = json.loads(dataResponse)
    if 'exists' in dataJson and not dataJson['exists']:
        return False
    else:
        return True

def getDocumentCount(conn):
    if not conn:
        conn = getConnection()
        
    conn.request('GET', '/event_index/event/_count')
    response = conn.getresponse()
    dataResponse = response.read()
    dataJson = json.loads(dataResponse)
    if 'count' in dataJson:
        return dataJson['count']
    else:
        return -1
    
def composeEvent(data):
    event = {}
                
    if 'name' in data:
        if not data['name'] or data['name'] == 'null':
            return None
        
        event['name'] = data['name']
        
    if 'start' in data and data['start'] != "null":
        event['start'] = data['start'] + ' 00:00:00'
        
    if 'end' in data and data['end'] != "null":
        event['end'] = data['end'] + ' 00:00:00'
        
    if 'city' in data and data['city'] != "null":
        event['city'] = data['city']
        
    if 'url' in data and data['url'] != "null":
        event['url'] = data['url']
        
    if 'hashtags' in data and len(data['hashtags']) > 0:
        event['hashtags'] = data['hashtags']
        
    event['created'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return event

def indexES(conn, eventid, event):
    if not conn:
        conn = getConnection()
        
    conn.request('PUT', '/event_index/event/%s' %eventid, json.dumps(event))
    response = conn.getresponse()
    dataResponse = response.read()
    print('Indexed: ' + str(eventid) + '\t' + dataResponse)
    
def indexESDump(conn, eventid, dataDump):
    if not conn:
        conn = getConnection()
    dump = {}
    dump['id'] = eventid
    dump['raw'] = json.dumps(dataDump)
    conn.request('PUT', '/retrain_index/event/%s' %eventid, json.dumps(dump))
    response = conn.getresponse()
    dataResponse = response.read()
    
def indexDB(cur, eventid, event):
    cur.execute('INSERT INTO search_event(id, created, modified, version, views, trained, type) VALUES (%s, \'%s\', \'%s\', 0, 0, 0, 0) ON DUPLICATE KEY UPDATE created = VALUES(created)' %(eventid, event['created'], event['created']))

def index():
    
    conn = getConnection()
    eventid = getDocumentCount(conn)
    conn.close()
    
    if eventid < 0:
        return
    
    count = 0
    lineList = []
    
    for line in sys.stdin:
        
        lineList.append(line.strip())
        count = count + 1
        
        if count == 10:
            
            conn = getConnection()
            con = getDBConnection()
            cur = con.cursor(mdb.cursors.DictCursor)
            
            for line in lineList:
        
                if not line:
                    continue
                
                try:
                    dataAll = json.loads(line)
                except:
                    continue
                
                data = dataAll['extracted']
                dataDump = dataAll['raw']
                
                event = composeEvent(data)
                
                if not event:
                    continue
                    
                while documentExist(conn, eventid):
                    eventid += 1
                    
                event['id'] = eventid
                
                indexES(conn, eventid, event)
                indexESDump(conn, eventid, dataDump)
                indexDB(cur, eventid, event)
                
                
                
            con.commit()    
            count = 0
            lineList = []
            conn.close()
            con.close()
    
if __name__ == '__main__':
    index()
    