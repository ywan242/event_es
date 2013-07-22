'''
Created on Jun 25, 2013

@author: Yu Wang
'''
import httplib
import time
import sys
import json

def getConnection():
    conn = httplib.HTTPConnection('hadoop005.mathcs.emory.edu:9200')
    return conn

def search(queryText):
    conn = getConnection()
    
    if conn:
        queryJson = {'query': {'query_string': {'default_field': 'name', 'query': queryText}}, 'size': 50}
                     
        queryStr = json.dumps(queryJson)
        conn.request('GET', '/event_index/eventturk/_search?pretty=true', queryStr)
        
        response = conn.getresponse()   
        dataResponse = response.read()
        
        dataJson = json.loads(dataResponse)
        
        resultList = []
        
        for event in dataJson['hits']['hits']:
            resultList.append(event['_source'])
    
    conn.close()
    return resultList
    
if __name__ == '__main__':
    search('medical conference')
    