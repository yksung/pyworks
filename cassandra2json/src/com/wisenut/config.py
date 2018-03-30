'''
Created on 2017. 7. 27.

@author: Holly
'''
import yaml
import os

'''
    각종 디렉토리를 초기화하는 클래스
'''
class Config:
    report_home=''
    
    def __init__(self):
        with open(os.path.join(os.path.dirname(__file__),'config.yml'), 'r') as stream:
            try:
                global_config = yaml.load(stream.read())
                
                self.report_home = global_config['reporthome']
            except yaml.YAMLError as exc:
                print(exc)
    
    def get_logconfig_path(self):
        return os.path.join(os.path.dirname(__file__), 'logging.ini')
    
    def get_report_home(self):
        return self.report_home
        