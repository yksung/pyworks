'''
Created on 2017. 7. 7.

@author: Holly
'''
import re

def urlEncodeNonAscii(b):
    return re.sub('[\x80-\xFF]', lambda c: '%%%02x' % ord(c.group(0)), b)