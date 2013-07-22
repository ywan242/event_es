'''
Created on Jun 27, 2013

@author: Yu Wang
'''
import httplib
import time
import sys
import json

def getConnection():
    conn = httplib.HTTPConnection('hadoop005.mathcs.emory.edu:9200')
    return conn

def search(queryText, frompos, size):
    queryText = queryText.strip()
    if not queryText:
        return []
    
    conn = getConnection()
    
    if conn:
        queryJson = {'query': {'query_string': {'default_field': 'name', 'query': queryText}}, 'size': size, 'from': frompos}
                     
        queryStr = json.dumps(queryJson)
        conn.request('GET', '/event_index/event/_search?pretty=true', queryStr)
        
        response = conn.getresponse()
        dataResponse = response.read()
        
        print(dataResponse)

        dataJson = json.loads(dataResponse)
        
        resultList = []
        
        for event in dataJson['hits']['hits']:
            eventEntity = event['_source']
            resultList.append(eventEntity)
    
    conn.close()

    return resultList
    
if __name__ == '__main__':
    search('', 0, 10)
    