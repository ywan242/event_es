'''
Created on Jul 17, 2013

@author: Yu Wang
'''
import httplib
import time
import sys
import json

def getConnection():
    conn = httplib.HTTPConnection('hadoop005.mathcs.emory.edu:9200')
    return conn

def getEvent(eid):
    conn = getConnection()
    conn.request('GET', '/event_index/event/%s' %eid)
    response = conn.getresponse()
    dataResponse = response.read()

    dataJson = json.loads(dataResponse)
    if 'exists' in dataJson and dataJson['exists']:
        result = dataJson['_source']
        return result
    
    return None
    
    
if __name__ == '__main__':
    print(json.dumps(getEvent(1)))
    