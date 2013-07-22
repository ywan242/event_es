'''
Created on Jul 18, 2013

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
        mapping = {"settings": {"index": {"number_of_shards" : 9, "number_of_replicas" : 0}}}
        
        mapping_str = json.dumps(mapping)
        conn.request("PUT", "/dump_index/", mapping_str)
        
        response = conn.getresponse()
        print(response.status)
        print(response.reason)
    
    
if __name__ == "__main__":
    createIndex()