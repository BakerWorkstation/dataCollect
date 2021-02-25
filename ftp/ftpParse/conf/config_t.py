import os,sys
import configparser

def conf_info():
    cfgpath = os.path.join(os.path.split(os.path.realpath(__file__))[0], "system_conf.ini")
    conf = configparser.ConfigParser()
    conf.read(cfgpath)

    return conf

handle_conf = conf_info()






