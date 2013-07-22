'''
Created on Jun 25, 2013

@author: Yu Wang
'''
import httplib
import time
import sys
import json

def getConnection():
    conn = httplib.HTTPConnection("hadoop005.mathcs.emory.edu:9200")
    return conn


def index(filename):
    
    f = open(filename, 'r')
    
    conn = getConnection()
    
    for line in f:
        a = line.split("\t")
        
        eventJson = {}

        name = a[2]
        
        loc = a[6]
        
        url = a[8]
        
        eventJson['name'] = name
        eventJson['address'] = loc
        eventJson['url'] = url
        
        start = a[9]
        end = a[10]
        
        try:
            startd = time.strftime('%Y-%m-%d %H:%M:%S', time.strptime(start, '%m/%d/%Y'))
        except:
            startd = None
        
        if startd:
            eventJson['start'] = startd
        
        try:
            endd = time.strftime('%Y-%m-%d %H:%M:%S', time.strptime(end, '%m/%d/%Y'))
        except:
            endd = None
            
        if endd:
            eventJson['end'] = endd
            
        eventStr = json.dumps(eventJson)
        conn.request('POST', '/event_index/eventturk/', eventStr)
        response = conn.getresponse()
        dataResponse = response.read()
        
if __name__ == '__main__':
    index(sys.argv[1])
        