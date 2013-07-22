'''
Created on Jul 17, 2013

@author: Yu Wang
'''
import datetime
import json
import re

s = 'abc def_k; ?0(**(&'

a = filter(None, re.split('\W', s))
for tag in a:
    if tag[0].isdigit():
        a.remove(tag)
print(json.dumps(a))