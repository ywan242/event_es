'''
Created on Jul 16, 2013

@author: Yu Wang
'''
import MySQLdb as mdb

def test():
    
    con = mdb.connect('tritanium.mathcs.emory.edu', 'eventgenie', 'eventgeniepass', 'eventgenie')
    
    if con:
        print('hello')
        
if __name__ == '__main__':
    test()
    