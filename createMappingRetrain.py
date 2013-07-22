'''
Created on Jul 16, 2013

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
        mapping = {"event": {"properties": {"id": {"index": "not_analyzed", "store": "yes", "type": "long"}, "raw": {"index": "not_analyzed","store": "yes","type": "string"}}}}
        
        mapping_str = json.dumps(mapping)
        conn.request("PUT", "/retrain_index/event/_mapping", mapping_str)
        
        response = conn.getresponse()
        print(response.status)
        print(response.reason)
    
    
if __name__ == "__main__":
    createMapping()
    