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
    es_ip=''
    es_port=0
    
    mariadb_ip=""
    mariadb_port=0
    mariadb_user=""
    mariadb_password=""
    mariadb_db=""
    mariadb_charset=""
    
    def __init__(self):
        with open(os.path.join(os.path.dirname(__file__),'config.yml'), 'r') as stream:
            try:
                global_config = yaml.load(stream.read())
                
                self.report_home = global_config['reporthome']
                self.es_ip = global_config['es_ip']
                self.es_port = global_config['es_port']
                
                self.mariadb_ip = global_config['mariadb_ip']
                self.mariadb_port = global_config['mariadb_port']
                self.mariadb_user = global_config['mariadb_user']
                self.mariadb_password = global_config['mariadb_password']
                self.mariadb_db = global_config['mariadb_db']
                self.mariadb_charset = global_config['mariadb_charset']
                
            except yaml.YAMLError as exc:
                print(exc)
    
    def get_report_home(self):
        return self.report_home
    
    def get_es_ip(self):
        return self.es_ip
    
    def get_es_port(self):
        return self.es_port
    
    def get_mariadb_ip(self):
        return self.mariadb_ip
    
    def get_mariadb_port(self):
        return self.mariadb_port
    
    def get_mariadb_user(self):
        return self.mariadb_user
    
    def get_mariadb_password(self):
        return self.mariadb_password
    
    def get_mariadb_db(self):
        return self.mariadb_db
    
    def get_mariadb_charset(self):
        return self.mariadb_charset
    
