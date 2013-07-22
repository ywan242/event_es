'''
Created on Jul 17, 2013

@author: Yu Wang
'''
import httplib
import sys
import json
import datetime

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

def indexES(conn, eventid, event):
    if not conn:
        conn = getConnection()
        
    conn.request('PUT', '/event_index/event/%s' %eventid, json.dumps(event))
    response = conn.getresponse()
    dataResponse = response.read()
    
def index(event): 
    conn = getConnection()
    eventid = getDocumentCount(conn)
    while documentExist(conn, eventid):
        eventid += 1
        
    event['id'] = eventid
    if 'start' in event:
        event['start'] = datetime.datetime.strptime(event['start'], '%m/%d/%Y').strftime('%Y-%m-%d') + ' 00:00:00'
        
    if 'end' in event:
        event['end'] = datetime.datetime.strptime(event['end'], '%m/%d/%Y').strftime('%Y-%m-%d') + ' 00:00:00'
    
    event['created'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    
    indexES(conn, eventid, event)
    
    conn.close()
    
    return eventid
    
        
    