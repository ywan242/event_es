'''
Created on Jul 17, 2013

@author: Yu Wang
'''
import httplib
import json

def getConnection():
    conn = httplib.HTTPConnection("hadoop005.mathcs.emory.edu:9200")
    return conn

def createMapping():
    conn = getConnection()
    if conn:
        mapping = {"event": {"properties": {"id": {"index": "not_analyzed", "store": "yes", "type": "long"}, "start": {"type": "date", "format" : "YYYY-MM-dd HH:mm:ss"}, "end": {"type": "date", "format" : "YYYY-MM-dd HH:mm:ss"}, "city": {"index": "analyzed","store": "yes","type": "string"}, "address": {"index": "analyzed","store": "yes","type": "string"}, "name": {"index": "analyzed","store": "yes","type": "string"}, "description": {"index": "analyzed","store": "yes","type": "string"}, "hashtags": {"type": "string"}, "url": {"type": "string"}, "created": {"type": "date", "format" : "YYYY-MM-dd HH:mm:ss"}, "version": {"index": "not_analyzed", "store": "yes", "type": "integer"}}}}
        
        mapping_str = json.dumps(mapping)
        conn.request("PUT", "/dump_index/event/_mapping", mapping_str)
        
        response = conn.getresponse()
        print(response.status)
        print(response.reason)
    
    
if __name__ == "__main__":
    createMapping()
    