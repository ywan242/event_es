'''
Created on Jul 18, 2013

@author: Yu Wang
'''
import httplib
import json
import re
import datetime

def getConnection():
    conn = httplib.HTTPConnection("hadoop005.mathcs.emory.edu:9200")
    return conn

def getEvent(conn, eid):
    conn.request('GET', '/event_index/event/%s' %eid)
    response = conn.getresponse()
    dataResponse = response.read()

    dataJson = json.loads(dataResponse)
    if 'exists' in dataJson and dataJson['exists']:
        result = dataJson['_source']
        return result
    
    return None

def indexDump(conn, event, version):
    
    event['version'] = version
    
    eventid = str(event['id']) + 'v' + str(version)
        
    conn.request('PUT', '/dump_index/event/%s' %eventid, json.dumps(event))
    response = conn.getresponse()
    dataResponse = response.read()

def updateEvent(event_id, version, key_str, value_str):
    conn = getConnection()
    
    event = getEvent(conn, event_id)

    if key_str == 'start' or key_str == 'end':
        value_str = value_str.strftime('%Y-%m-%d') + ' 00:00:00'
    
    if key_str in event and event[key_str] == value_str:
        return
    
    indexDump(conn, event, version)
    
    script = 'ctx._source.' + key_str + ' = new_value'
    parameter = {'new_value': value_str.strip()}
    valueDict = {'script': script, 'params': parameter}
    valueStr = json.dumps(valueDict)
    conn.request('POST', '/event_index/event/' + str(event_id) + '/_update', valueStr)
    response = conn.getresponse()
    dataResponse = response.read()
    
if __name__ == '__main__':
    updateEvent(0, 0, 'city', 'no\'\"')
    
    
    
