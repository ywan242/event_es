'''
Created on Jun 25, 2013

@author: Yu Wang
'''
import httplib
import json

def getConnection():
    conn = httplib.HTTPConnection("hadoop005.mathcs.emory.edu:9200")
    return conn

def createIndex():
    conn = getConnection()
    if conn:

        conn.request("DELETE", "/event_index/event")
        
        response = conn.getresponse()
        print(response.status)
        print(response.reason)
    
    
if __name__ == "__main__":
    createIndex()