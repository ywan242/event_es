'''
Created on Jul 17, 2013

@author: Yu Wang
'''
import MySQLdb as mdb
import httplib
import json

def getConnection():
    conn = httplib.HTTPConnection("hadoop005.mathcs.emory.edu:9200")
    return conn

def collectLabel():
    con = mdb.connect('tritanium.mathcs.emory.edu', 'eventgenie', 'eventgeniepass', 'eventgenie')
    conn = getConnection()
    
    if con and conn:
        cur = con.cursor(mdb.cursors.DictCursor)
        cur.execute('SELECT id, name, city, start, version, views FROM search_event WHERE e_type = 0 AND trained = 0 AND (version > 0 OR views > 5)')
        data = cur.fetchall()
        
        for event in data:
            eid = event['id']
            conn.request('GET', '/retrain_index/event/' + str(eid))
            response = conn.getresponse()
            dataResponse = response.read()
            dataJson = json.loads(dataResponse)
            result = event
            if 'exists' in dataJson and dataJson['exists']:
                result['raw'] = dataJson['_source']['raw']
                print(json.dump(result))
                
        con.close()
        conn.close()
                