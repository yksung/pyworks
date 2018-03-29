'''
Created on 2017. 6. 9.

@author: Holly
'''
import os

def search_create_directory(dirname):
    if not os.path.exists(dirname):
        os.makedirs(dirname)
        
    return dirname