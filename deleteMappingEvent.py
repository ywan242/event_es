'''
Created on Jun 25, 2013

@author: Yu Wang
'''
import httplib

def getConnection():
    conn = httplib.HTTPConnection("hadoop005.mathcs.emory.edu:9200")
    return conn

def deleteMapping():
    conn = getConnection()
    if conn:
        #pp = pprint.PrettyPrinter(indent=1)

        conn.request("DELETE", "/event_index/event/")
        
        response = conn.getresponse()
        print(response.status)
        print(response.reason)
        
        conn.close()
    
    
if __name__ == "__main__":
    deleteMapping()
