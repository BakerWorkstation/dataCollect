#!/usr/bin/env python3.6
# -*- coding:UTF-8 -*-

import os,re
import json
import configparser
import datetime
import random
from confluent_kafka import Consumer, KafkaError, Producer, KafkaException, TopicPartition, Message, admin
import happybase
import psycopg2,time
from multiprocessing import Pool
import urllib.parse
import urllib

#import ieplog
#import Config; Config._init()
#config_env = Config.get_value()
#from kafkaapi import Kafkaapi





class Datademo(object):
    def __init__(self):
        pass
    #eventlog信息结构
        self.all_eventinfo={
        "hbase_rowkey": "",
        "Used_by_ES_src": {
            "source_endpoint_ip": "",
            "source_endpoint_domain": "",
            "source_endpoint_url": "",
            "source_endpoint_port": "",
            "source_endpoint_ip_cp": "",
            "source_load_md5": "",
            "source_load_filename": "",
            "source_load_filepath": "",
            "source_load_filebytes": "",
            "source_load_filetype": "",
            "source_load_vulne_id": [],
            "source_load_attack_action": [],
            "source_load_attack_v_name": "",
            "source_load_attack_v_type": "",
            "source_load_attack_v_family": "",        
            "direction": ""
        },
        "Used_by_ES_dst": {
            "purpose_endpoint_ip": "",
            "purpose_endpoint_domain": "",
            "purpose_endpoint_url": "",
            "purpose_endpoint_port": "",
            "purpose_endpoint_ip_cp": "",
            "purpose_load_md5": "",
            "purpose_load_filename": "",
            "purpose_load_filepath": "",
            "purpose_load_filebytes": "",
            "purpose_load_filetype": "",
            "purpose_load_vulne_id": [],
            "purpose_load_attack_action": [],
            "purpose_load_attack_v_name": "",
            "purpose_load_attack_v_type": "",
            "purpose_load_attack_v_family": "",    
            "direction": ""
        },
        "Used_by_ES_ExtendInfo": {
            "alert": "",
            "is_malicious": "",
            "detect_pro_id": "",
            "rule_id": ""
        },
        "threat_info": {
            "eventlog_id": "",
            "sponsor_time": "",
            "event_type": {
                "first_type": "",
                "second_type": "",
                "third_type": "",
                "fourth_type": ""
            },
            "source_endpoint": {
                "ip_info": {
                    "ip": "",
                    "port": "",
                    "mac": "",
                    "ipv6": "",
                    "ip_country": "",
                    "ip_city": "",
                    "ip_region": "",
                    "ip_cp": "",
                    "isp": "",
                    "ip_longitude": "",
                    "ip_latitude": ""
                },
                "url": "",
                "domain_info": {
                    "domain": "",
                    "create_time": "",
                    "dead_time": "",
                    "update_time": "",
                    "lastupdate_time": "",
                    "registry": {
                        "reg_people": "",
                        "reg_company": "",
                        "reg_street": "",
                        "reg_city": "",
                        "reg_country": "",
                        "reg_telephone": "",
                        "reg_mailbox": "",
                        "reg_fax": "",
                        "reg_note": ""
                    },
                    "manage": {
                        "man_name": "",
                        "man_organization": "",
                        "man_street": "",
                        "man_city": "",
                        "man_country": "",
                        "man_mailbox": "",
                        "man_telephone": "",
                        "man_fax": "",
                        "man_note": ""
                    }
                },
                "label": [],
                "detect": {
                    "detect_time": "",
                    "detect_pro_id": "",
                    "detect_pro": "",
                    "detect_rule": {
                        "detect_base": "",
                        "detect_id": ""
                    }
                },
                "asset_detail": {
                    "asset_id": "",
                    "os": "",
                    "ip_group": [{
                        "ip": "",
                        "mac": ""
                    }],
                    "unit": "",
                    "network_struct": [],
                    "type": "",
                    "ports": [],
                    "asset_level": "",
                    "people": {
                        "user": "",
                        "manager": ""
                    },
                    "applications": []
                },
                "status": "",
                "status_des": ""
            },
            "carrier": {
                "carrier_type": "",
                "value": "",
                "protocol": "",
                "info": {
                    "data_info": {
                        "dns_info": {
                            "message_type": "",
                            "record": {
                                "record_type": "",
                                "pro_info": "",
                                "alias": "",
                                "return_data": ""
                            }
                        },
                        "email_info": {
                            "from_ip": "",
                            "from": "",
                            "to": "",
                            "sender": "",
                            "sub": "",
                            "content": {
                                "text": "",
                                "text_url": []
                            },
                            "file": [{
                                "filehash": {
                                    "md5": "",
                                    "sha1": "",
                                    "sha256": ""
                                },
                                "file_name": "",
                                "file_format": ""
                            }]
                        },
                        "ftp_info": {
                            "upload": {
                                "bytes": "",
                                "direct": "",
                                "file_name": "",
                                "file_type": "",
                                "file_hash": {
                                    "md5": "",
                                    "sha1": "",
                                    "sha256": ""
                                }
                            },
                            "download": {
                                "bytes": "",
                                "direct": "",
                                "file_name": "",
                                "file_type": "",
                                "file_hash": {
                                    "md5": "",
                                    "sha1": "",
                                    "sha256": ""
                                }
                            }
                        },
                        "http_info": {
                            "request": {
                                "header": {
                                    "method": "",
                                    "URL": "",
                                    "version": "",
                                    "Accept": "",
                                    "Accept-Encoding": "",
                                    "Accept-Language": "",
                                    "Connection": "",
                                    "Contern-Type": "",
                                    "Cookie": "",
                                    "Host": "",
                                    "Referer": "",
                                    "User-Agent": ""
                                },
                                "body": {
                                    "bytes": "",
                                    "direction": "",
                                    "file_name": "",
                                    "file_type": "",
                                    "file_hash": {
                                        "md5": "",
                                        "sha1": "",
                                        "sha256": ""
                                    }
                                }
                            },
                            "response": {
                                "header": {
                                    "method": "",
                                    "URL": "",
                                    "version": "",
                                    "Accept": "",
                                    "Accept-Encoding": "",
                                    "Accept-Language": "",
                                    "Connection": "",
                                    "Contern-Type": "",
                                    "Cookie": "",
                                    "Host": "",
                                    "Referer": "",
                                    "User-Agent": ""
                                },
                                "body": {
                                    "bytes": "",
                                    "direction": "",
                                    "file_name": "",
                                    "file_type": "",
                                    "filehash": {
                                        "md5": "",
                                        "sha1": "",
                                        "sha256": ""
                                    }
                                }
                            }
                        },
                        "smb_info": {
                            "upload": {
                                "bytes": "",
                                "direct": "",
                                "file_name": "",
                                "file_type": "",
                                "file_hash": {
                                    "md5": "",
                                    "sha1": "",
                                    "sha256": ""
                                }
                            },
                            "download": {
                                "bytes": "",
                                "direct": "",
                                "file_name": "",
                                "file_type": "",
                                "file_hash": {
                                    "md5": "",
                                    "sha1": "",
                                    "sha256": ""
                                }
                            }
                        },
                        "smtp_info": {}
                    },
                    "pcap": ""
                }
            },
            "source_load": [{
                "fprocess_pid": "",
                "fprocess_hash": {
                    "md5": "",
                    "sha1": "",
                    "sha256": ""
                },
                "fprocess_path": "",
                "o_filename": "",
                "file_format": "",
                "size": "",
                "v_name": "",
                "v_type": "",
                "v_family": "",
                "platform": "",
                "attack_action": [],
                "vulne": [{
                    "vulne_id": "",
                    "vulne_name": "",
                    "open_time": "",
                    "vulne_level": "",
                    "vulne_type": "",
                    "for_products": []
                }],
                "level": "",
                "dispose": "",
                "download_time": "",
                "run_time": "",
                "loader_label": [],
                "loader_behavior_label":[],
                "defense_state":"",
                "detect": {
                    "detect_time": "",
                    "detect_pro_id": "",
                    "detect_pro": "",
                    "detect_rule": {
                        "detect_base": "",
                        "detect_id": ""
                    }
                }
            }],
            "sponsor_behavior": {
                "attack_info": {
                    "first_type": "",
                    "second_type": "",
                    "third_type": "",
                    "fourth_type": ""
                },
                "exploit_vulnerability": {
                    "vulne_id": "",
                    "vulne_name": "",
                    "open_time": "",
                    "vulne_level": "",
                    "vulne_type": "",
                    "for_products": []
                },
                "detect": {
                    "detect_time": "",
                    "detect_pro_id": "",
                    "detect_pro": "",
                    "detect_rule": {
                        "detect_base": "",
                        "detect_id": ""
                    }
                },
                "dispose": "",
                "attack_label": {
                    "attack": [{
                        "tactic": "",
                        "techniques": "",
                        "tactic_number": ""
                    }],
                    "nsacss": [{
                        "stage": "",
                        "objective": "",
                        "action": "",
                        "stage_number": ""
                    }]
                }
            },
            "purpose_endpoint": {
                "ip_info": {
                    "ip": "",
                    "port": "",
                    "mac": "",
                    "ipv6": "",
                    "ip_country": "",
                    "ip_city": "",
                    "ip_region": "",
                    "ip_cp": "",
                    "isp": "",
                    "ip_longitude": "",
                    "ip_latitude": ""
                },
                "url": "",
                "domain_info": {
                    "domain": "",
                    "create_time": "",
                    "dead_time": "",
                    "update_time": "",
                    "lastupdate_time": "",
                    "registry": {
                        "reg_people": "",
                        "reg_company": "",
                        "reg_street": "",
                        "reg_city": "",
                        "reg_country": "",
                        "reg_telephone": "",
                        "reg_mailbox": "",
                        "reg_fax": "",
                        "reg_note": ""
                    },
                    "manage": {
                        "man_name": "",
                        "man_organization": "",
                        "man_street": "",
                        "man_city": "",
                        "man_country": "",
                        "man_mailbox": "",
                        "man_telephone": "",
                        "man_fax": "",
                        "man_note": ""
                    }
                },
                "label": [],
                "detect": {
                    "detect_time": "",
                    "detect_pro_id": "",
                    "detect_pro": "",
                    "detect_rule": {
                        "detect_base": "",
                        "detect_id": ""
                    }
                },
                "asset_detail": {
                    "asset_id": "",
                    "os": "",
                    "ip_group": [{
                        "ip": "",
                        "mac": ""
                    }],
                    "unit": "",
                    "network_struct": [],
                    "type": "",
                    "ports": [],
                    "asset_level": "",
                    "people": {
                        "user": "",
                        "manager": ""
                    },
                    "applications": []
                },
                "status": "",
                "status_des": ""
            },
            "purpose_load": {
                "sprocess_pid": "",
                "p_load_cmd": "",
                "p_load_extend_info_from_cmd": [],
                "p_load_behavior_info_additional": "",
                "behavior_initiator": "",
                "sfile_hash": {
                    "md5": "",
                    "sha1": "",
                    "sha256": ""
                },
                "sprocess_path": "",
                "o_filename": "",
                "file_format": "",
                "size": "",
                "v_name": "",
                "v_type": "",
                "v_family": "",            
                "platform": "",
                "attack_action": [],
                "vulne": [{
                    "vulne_id": "",
                    "vulne_name": "",
                    "open_time": "",
                    "vulne_level": "",
                    "vulne_type": "",
                    "for_products": []
                }],
                "level": "",
                "dispose": "",
                "download_time": "",
                "run_time": "",
                "loader_label": [],
                "loader_behavior_label":[],
                "defense_state":"",
                "detect": {
                    "detect_time": "",
                    "detect_pro_id": "",
                    "detect_pro": "",
                    "detect_rule": {
                        "detect_base": "",
                        "detect_id": ""
                    }
                }
            },
            "extend_details": {
                "attack_info": {
                    "from_ip": {
                        "ip": "",
                        "country": "",
                        "city": "",
                        "region": "",
                        "units": "",
                        "operators": "",
                        "ASN": "",
                        "longitude": "",
                        "latitude": ""
                    },
                    "url": "",
                    "hit_beacon": "",
                    "domain": "",
                    "intelligence_info": {
                        "attacker_id": "",
                        "type": "",
                        "attacker": "",
                        "country": "",
                        "behavior_info": {
                            "act_name": "",
                            "act_time": "",
                            "act_description": ""
                        },
                        "loader_Carrier_hit": "",
                        "loader_Carrier_his": [{
                            "type": "",
                            "value": "",
                            "protocol": "",
                            "loa_other": {
                                "domain_info": {
                                    "domain": "",
                                    "create_time": "",
                                    "dead_time": "",
                                    "update_time": "",
                                    "lastupdate_time": "",
                                    "registry": {
                                        "reg_people": "",
                                        "reg_company": "",
                                        "reg_street": "",
                                        "reg_city": "",
                                        "reg_country": "",
                                        "reg_telephone": "",
                                        "reg_mailbox": "",
                                        "reg_fax": "",
                                        "reg_note": ""
                                    },
                                    "manage": {
                                        "man_name": "",
                                        "man_organization": "",
                                        "man_street": "",
                                        "man_city": "",
                                        "man_country": "",
                                        "man_mailbox": "",
                                        "man_telephone": "",
                                        "man_fax": "",
                                        "man_note": ""
                                    }
                                },
                                "ip_position": {
                                    "ip": "",
                                    "country": "",
                                    "city": "",
                                    "region": "",
                                    "units": "",
                                    "operators": "",
                                    "ASN": "",
                                    "longitude": "",
                                    "latitude": ""
                                }
                            }
                        }],
                        "other_attacker_label": []
                    },
                    "attacker_label": []
                },
                "loader_info": [{
                    "process_pid": "",
                    "file_hash": {
                        "md5": "",
                        "sha1": "",
                        "sha256": ""
                    },
                    "process_path": "",
                    "o_filename": "",
                    "size": "",
                    "file_format": "",
                    "v_name": "",
                    "v_type": "",
                    "v_family": "",
                    "platform": "",
                    "level": "",
                    "download_time": "",
                    "run_time": "",
                    "label": [],
                    "attack_action": [],
                    "vulne": [{
                        "vulne_id": "",
                        "vulne_name": "",
                        "open_time": "",
                        "vulne_level": "",
                        "vulne_type": "",
                        "for_products": []
                    }],
                    "relation": {
                        "fprocess_pid": "",
                        "o_filename": "",
                        "fprocess_path": "",
                        "ffile_hash": {
                            "md5": "",
                            "sha1": "",
                            "sha256": ""
                        },
                        "sprocess_pid": "",
                        "p_filename": "",
                        "sprocess_path": "",
                        "sfile_hash": {
                            "md5": "",
                            "sha1": "",
                            "sha256": ""
                        }
                    },
                    "version_info": {
                        "author": "",
                        "update_time": "",
                        "company": "",
                        "create_time": "",
                        "file_version": "",
                        "last_updater": "",
                        "last_visit_time": ""
                    },
                    "certificate_info": {
                        "file_signature": "",
                        "file_signature_time": ""
                    },
                    "dynamic_result": ""
                }],
                "vulne_info": [{
                    "vulne_id": "",
                    "vulne_name": "",
                    "open_time": "",
                    "vulne_level": "",
                    "vulne_type": "",
                    "for_products": [],
                    "poc_info": "",
                    "patch_info": [{
                        "patch_id": "",
                        "patch_name": "",
                        "open_time": "",
                        "patch_level": "",
                        "patch_label": [],
                        "cvelist": [],
                        "size": "",
                        "patch_md5": "",
                        "patch_rules": "",
                        "download_url": ""
                    }]
                }],
                "detector_info": [{
                    "detect_time": "",
                    "detect_pro_id": "",
                    "detect_pro": "",
                    "detect_rule": {
                        "detect_base": "",
                        "detect_id": ""
                    },
                    "d_proinfo": {
                        "unit": "",
                        "safety_region": "",
                        "network_region": "",
                        "location": "",
                        "people": ""
                    },
                    "detect_obj": ""
                }]
            },
            "custom_log": {}
        },

        "Vulnerability_info": {
            "id": "",
            "time": {
                "attack_time": "",
                "end_time": ""
            },
            "discoverer": {
                "ip": "",
                "product": "",
                "productinfo": {
                    "unit": "",
                    "domain": "",
                    "safety_zone": "",
                    "phy_location": "",
                    "contacts": ""
                }
            },
            "discover_way": {
                "name": "",
                "describe": "",
                "rule_info": {
                    "id": "",
                    "detect_base": ""
                }
            },
            "fragile_node": {
                "type": "",
                "name": "",
                "version": "",
                "for_asset": {
                    "ip_info": {
                        "ip": "",
                        "country": "",
                        "city": "",
                        "region": "",
                        "units": "",
                        "operators": "",
                        "ASN": "",
                        "longitude": "",
                        "latitude": ""
                    },
                    "asset_detail": {
                        "asset_id": "",
                        "os": "",
                        "ip_group": [{
                            "ip": "",
                            "mac": ""
                        }],
                        "unit": "",
                        "network_struct": [],
                        "type": "",
                        "ports": [],
                        "label": "",
                        "asset_level": "",
                        "people": {
                            "user": "",
                            "manager": ""
                        },
                        "applications": [],
                        "uptime": ""
                    }
                },
                "describe": ""
            },
            "vulnerable_point": [{
                "vulnerable_point_list": [],
                "vulnerability_type": {
                    "first_type": "",
                    "second_type": "",
                    "third_type": ""
                },
                "level": "",
                "vulnerability_name": "",
                "vulnerability_describe": ""
            }],
            "vulnerability_status": {
                "vulne_repair": "",
                "vulne_discover": "",
                "vulne_confirm": "",
                "vulne_ease": ""
            },
            "extend_details": {
                "detector_info": {
                    "detect_time": "",
                    "detect_pro_id": "",
                    "detect_pro": "",
                    "detect_rule": {
                        "detect_base": "",
                        "detect_id": ""
                    },
                    "d_proinfo": {
                        "unit": "",
                        "safety_region": "",
                        "network_region": "",
                        "location": "",
                        "people": ""
                    }
                },
                "vulne_info": [{
                    "vulne_id": "",
                    "vulne_name": "",
                    "open_time": "",
                    "vulne_level": "",
                    "vulne_type": "",
                    "vulne_description": "",
                    "for_products": [],
                    "poc_info": "",
                    "patch_info": [{
                        "patch_id": "",
                        "patch_name": "",
                        "open_time": "",
                        "patch_level": "",
                        "patch_label": [],
                        "cvelist": [],
                        "size": "",
                        "patch_md5": "",
                        "patch_rules": "",
                        "download_url": ""
                    }]
                }],
                "vulnerability_info": {
                    "dispose_time": "",
                    "disposer": "",
                    "vulnerability_status": ""
                }
            }
        },
        "abnormal": {},
        "malfunction_and_performance": {},
        "NetworkBehavior-netflow": {},
        "Violation": {}
    }
    ##丰富漏洞函数
    def vulne_f(self,datas):
        vulne_1,vulne_2,vulne_3=[],[],[]
        for data in datas:
            patch_info=[]
            x={
            "vulne_id": data["1"],
            "vulne_name": data["2"],
            "open_time": str(data["3"]),
            "vulne_level": str(data["4"]),
            "vulne_type": data["5"],
            "for_products": data["6"]
            }
            vulne_1.append(x)
            vulne_3.append(data["1"])
            x["poc_info"]=data["7"]
            x["patch_info"]=[]
            for value in data["8"]:
                y={
                "patch_id": value["1"],
                "patch_name": value["2"],
                "open_time": value["3"],
                "patch_level": str(value["4"]),
                "patch_label": value["5"],
                "cvelist": value["6"],
                "size": value["7"],
                "patch_md5": value["8"],
                "patch_rules": value["9"],
                "download_url":value["a"]
                }
                x["patch_info"].append(y)
            vulne_2.append(x)
        return vulne_1,vulne_2,vulne_3



        
    #丰富旧日志函数
    def tranOldData(self,data,sql_data,uuidlist):
        eventtype=["first_type","second_type","third_type","fourth_type"]
        ##获取漏洞信息

        ##  丰富源端点信息
        self.all_eventinfo["threat_info"]["eventlog_id"]=str(data["ts"])
        self.all_eventinfo["threat_info"]["sponsor_time"]=str(data["ts"])
        row = sql_data[data["client"]["uuid"]] if data["client"]["uuid"] in uuidlist else []
        #self.all_eventinfo["threat_info"]["source_endpoint"]["ip_info"]["ip"]=data["data"]["d"]["1"][0]["1"] if data["data"]["d"]["1"]!=None else ""
        #self.all_eventinfo["threat_info"]["source_endpoint"]["ip_info"]["mac"]=data["data"]["d"]["1"][0]["2"] if data["data"]["d"]["1"]!=None else ""
        if row == []:
            self.all_eventinfo["threat_info"]["source_endpoint"]["asset_detail"]={
                "asset_id": data["client"]["uuid"],
                "os": "",
                "ip_group":[{"ip":data["client"]["ip"],"mac":""}],
                "unit": "",
                "network_struct":[],
                "type": "",
                "ports":[],
                "asset_level": "",
                "people": {
                    "user": "",
                    "manager": ""
                },
                "applications": []
            }
        else:
            self.all_eventinfo["threat_info"]["source_endpoint"]["asset_detail"]={
                "asset_id": data["client"]["uuid"],
                "os": row[0][0] if row[0][0] !=None else "",
                "ip_group":[{"ip":x[2],"mac":x[3]} for x in row],
                "unit": row[0][7] if row[0][7] !=None else "",
                "network_struct": [row[0][9]] if row[0][9] !=None else [],
                "type": row[0][8] if row[0][8] !=None else "",
                "ports": [],
                "asset_level": str(row[0][4]) if row[0][4] !=None else "",
                "people": {
                    "user": row[0][5] if row[0][5] !=None else "",
                    "manager": row[0][6] if row[0][6] !=None else ""
                },
                "applications": row[0][1].split(',') if row[0][1]!=None else []
            }

        #self.all_eventinfo["threat_info"]["source_endpoint"]["ip_info"]["ip"]=self.all_eventinfo["threat_info"]["source_endpoint"]["asset_detail"]["ip_group"][0]["ip"]
        #self.all_eventinfo["threat_info"]["source_endpoint"]["ip_info"]["mac"]=self.all_eventinfo["threat_info"]["source_endpoint"]["asset_detail"]["ip_group"][0]["mac"]
        self.all_eventinfo["threat_info"]["source_endpoint"]["ip_info"]["ip"]=data["data"]["d"]["1"][0]["1"] if data["data"]["d"]["1"]!=None else ""
        self.all_eventinfo["threat_info"]["source_endpoint"]["ip_info"]["mac"]=data["data"]["d"]["1"][0]["2"] if data["data"]["d"]["1"]!=None else ""
        if self.all_eventinfo["threat_info"]["source_endpoint"]["ip_info"]["ip"] == "":
            self.all_eventinfo["threat_info"]["source_endpoint"]["ip_info"]["ip"] = data["client"]["ip"]
        
        #  丰富行为信息

        detect= {
            "detect_time": str(data["ts"]),
            "detect_pro_id": data["client"]["uuid"],
            "detect_pro": "IEP",
            "detect_rule": {
                "detect_base": "",
                "detect_id": ""
            }
        }
        
        ##TODO  丰富源载荷信息  
        if data["type"] in ["active_protect","proc_protect"]:
            self.all_eventinfo["threat_info"]["source_load"][0]["fprocess_pid"]=data.get("parent_process_id","")
            self.all_eventinfo["threat_info"]["source_load"][0]["o_filename"]=data["parent_process_path"].split("\\")[-1]
            self.all_eventinfo["threat_info"]["source_load"][0]["fprocess_path"]=data["parent_process_path"]
            self.all_eventinfo["threat_info"]["source_load"][0]["fprocess_hash"]["md5"]=data["parent_process_md5"]
            self.all_eventinfo["threat_info"]["source_load"][0]["detect"]=detect
            #TODO   目标载荷填充  漏洞信息
            self.all_eventinfo["threat_info"]["purpose_load"]["p_load_cmd"] = data["parameters"]
            self.all_eventinfo["threat_info"]["purpose_load"]["sprocess_path"] = data["child_process_path"]
            self.all_eventinfo["threat_info"]["purpose_load"]["o_filename"] = data["child_process_path"].split("\\")[-1]
            self.all_eventinfo["threat_info"]["purpose_load"]["sfile_hash"]["md5"] = data["child_process_md5"]
            self.all_eventinfo["threat_info"]["purpose_load"]["v_name"] = data["malname"] if "malname" in data.keys() else ""
            self.all_eventinfo["threat_info"]["purpose_load"]["detect"] = detect
            self.all_eventinfo["threat_info"]["extend_details"]["loader_info"][0]["process_path"] = data["child_process_path"]
            self.all_eventinfo["threat_info"]["extend_details"]["loader_info"][0]["o_filename"] = data["child_process_path"].split("\\")[-1]
            self.all_eventinfo["threat_info"]["extend_details"]["loader_info"][0]["v_name"] = data["malname"] if "malname" in data.keys() else ""
            self.all_eventinfo["threat_info"]["extend_details"]["loader_info"][0]["file_hash"]["md5"] = data["child_process_md5"]
            self.all_eventinfo["threat_info"]["extend_details"]["loader_info"][0]["relation"]["fprocess_pid"] = data.get("parent_process_id","")
            self.all_eventinfo["threat_info"]["extend_details"]["loader_info"][0]["relation"]["o_filename"] = data["parent_process_path"].split("\\")[-1]
            self.all_eventinfo["threat_info"]["extend_details"]["loader_info"][0]["relation"]["fprocess_path"] = data["parent_process_path"]
            #self.all_eventinfo["threat_info"]["extend_details"]["loader_info"][0]["relation"]["fprocess_path"]
            self.all_eventinfo["threat_info"]["extend_details"]["loader_info"][0]["relation"]["ffile_hash"]["md5"] =data["parent_process_md5"] 

            self.all_eventinfo["threat_info"]["extend_details"]["purpose_carrier"]={
                "reg":{
                    "reg_path": data.get("registry_path",""),
                    "reg_key":data.get("registry_key",""),
                    "reg_value":data.get("registry_value","")
                    },
                "task":{
                    "task_name": "",
                    "task_value": "",
                    "task_path": "",
                    "task_param": ""
                    },
                "runtime":str(data["ts"]),
                "level":""
            }
            #提取命令行载荷
            if data["parameters"] != "":
                if "WIN" in self.all_eventinfo["threat_info"]["source_endpoint"]["asset_detail"]["os"].upper():
                    p_load_extend_info_from_cmd=re.findall(r"(([abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ]:){1}(\\[^\\/<>\|\*\?]+)+)+",data["data"]["i"]["1"][0]["5"])
                else:
                    p_load_extend_info_from_cmd=re.findall(r"((^[\s]*[/|\./|\.\.]{1}([^\s]+)){1}((/){1}([^\s]+))+)",data["data"]["i"]["1"][0]["5"])
                if p_load_extend_info_from_cmd != []:
                    self.all_eventinfo["threat_info"]["purpose_load"]["p_load_extend_info_from_cmd"]=[cmd[0] for cmd in p_load_extend_info_from_cmd]    

             
        ##   此处判断被检测对象是否是源载荷  如果是 ：： 
        else:
            self.all_eventinfo["threat_info"]["extend_details"]["loader_info"]=[]
            if data["type"] == "virus_detect":
                self.all_eventinfo["threat_info"]["source_load"]=[]
                for fileinfo in data["fileinfo"]:
                    file_data = {
                        "fprocess_pid": "",
                        "fprocess_hash": {
                            "md5": fileinfo["md5"],
                            "sha1": "",
                            "sha256": ""
                        },
                        "fprocess_path": fileinfo["path"],
                        "o_filename": fileinfo["path"].split("\\")[-1],
                        "file_format": "",
                        "size": fileinfo["size"],
                        "v_name": fileinfo["malname"],
                        "v_type": "",
                        "v_family": "",
                        "platform": "",
                        "attack_action": [],
                        "vulne": [{
                            "vulne_id": "",
                            "vulne_name": "",
                            "open_time": "",
                            "vulne_level": "",
                            "vulne_type": "",
                            "for_products": []
                        }],
                        "level": "",
                        "dispose": fileinfo.get("option",""),
                        "download_time": "",
                        "run_time": "",
                        "loader_label": [],
                        "detect": detect
                        }         
                    self.all_eventinfo["threat_info"]["source_load"].append(file_data)
            else:
                self.all_eventinfo["threat_info"]["purpose_load"]={
                    "s  process_pid": "",
                    "p_load_cmd": "",
                    "p_load_extend_info_from_cmd": [],
                    "p_load_behavior_info_additional": "",
                    "behavior_initiator": "",
                    "sfile_hash": {
                        "md5": data["fileinfo"][0]["md5"],
                        "sha1": "",
                        "sha256": ""
                    },
                    "sprocess_path": data["fileinfo"][0]["path"],
                    "o_filename": data["fileinfo"][0]["path"].split("\\")[-1],
                    "file_format": "",
                    "size": "",
                    "v_name": "",
                    "v_type": "",
                    "v_family": "",            
                    "platform": "",
                    "attack_action": [],
                    "vulne": [{
                        "vulne_id": "",
                        "vulne_name": "",
                        "open_time": "",
                        "vulne_level": "",
                        "vulne_type": "",
                        "for_products": []
                    }],
                    "level": "",
                    "dispose": "",
                    "download_time": "",
                    "run_time": str(data["ts"]),
                    "loader_label": [],
                    "detect": detect
                }
            for fileinfo in data["fileinfo"]:
                filedata={
                    "process_pid":"",
                    "file_hash": {
                    "md5": fileinfo["md5"],
                    "sha1":"",
                    "sha256":""
                    },
                    "process_path":fileinfo["path"],
                    "o_filename":fileinfo["path"].split("\\")[-1],
                    "size": fileinfo["size"],
                    "file_format":"",
                    "v_name": fileinfo["malname"],
                    "platform":"",
                    "level": "",
                    "download_time": "",
                    "run_time": str(data["ts"]),
                    "label": [],
                    "attack_action": [],
                    "vulne": [],
                    "relation": {
                        "fprocess_pid":"",
                        "o_filename": "",
                        "fprocess_path": "",
                        "ffile_hash": {
                            "md5": "",
                            "sha1": "",
                            "sha256":""
                        },
                        "sprocess_pid": "",
                        "p_filename": "",
                        "sprocess_path": "",
                        "sfile_hash": {
                            "md5": "",
                            "sha1": "",
                            "sha256": ""
                        }
                    },
                    "version_info": {
                        "author": "",
                        "update_time": fileinfo["mt"],
                        "company": "",
                        "create_time": fileinfo["ct"],
                        "file_version": fileinfo["ver"],
                        "last_updater": "",
                        "last_visit_time": ""
                    },
                    "certificate_info": {
                        "file_signature": fileinfo["digsig"],
                        "file_signature_time": ""
                    },
                    "dynamic_result": ""
                }
                self.all_eventinfo["threat_info"]["extend_details"]["loader_info"].append(filedata)
        self.all_eventinfo["threat_info"]["source_endpoint"]["detect"]= detect
        self.all_eventinfo["threat_info"]["purpose_endpoint"]=self.all_eventinfo["threat_info"]["source_endpoint"]
        detect_obj = {
            "proc_protect":"目的载荷",
            "active_protect":"目的载体",
            "virus_detect":"源载荷",
            "virus_defend":"目的载荷"
        }
        self.all_eventinfo["threat_info"]["extend_details"]["detector_info"][0]={
                "detect_time": str(data["ts"]),
                "detect_pro_id": data["client"]["uuid"],
                "detect_pro": "IEP",
                "detect_rule": {
                    "detect_base": "",
                    "detect_id": ""
                },
                "d_proinfo": {
                    "unit": "",
                    "safety_region": "",
                    "network_region": "",
                    "location": "",
                    "people": ""
                },
                "detect_obj" : detect_obj[data["type"]]
            }

        self.all_eventinfo["Used_by_ES_src"] = {
            "source_endpoint_ip": self.all_eventinfo["threat_info"]["source_endpoint"]["ip_info"]["ip"],
            "source_endpoint_domain": "",
            "source_endpoint_url": "",
            "source_endpoint_port":"",
            "source_endpoint_ip_cp":"",
            "source_load_md5":[i["fprocess_hash"]["md5"] for i in self.all_eventinfo["threat_info"]["source_load"]],
            "source_load_filename":[i["fprocess_path"] for i in self.all_eventinfo["threat_info"]["source_load"]] ,
            "source_load_filepath":[i["o_filename"] for i in self.all_eventinfo["threat_info"]["source_load"]] ,
            "source_load_filebytes": "",
            "source_load_filetype": "",
            "source_load_vulne_id":[i["vulne_id"] for i in self.all_eventinfo["threat_info"]["source_load"][0]["vulne"]] if self.all_eventinfo["threat_info"]["source_load"][0]["vulne"] != [] else [],
            "source_load_attack_action": self.all_eventinfo["threat_info"]["source_load"][0]["attack_action"],
            "sorce_load_attack_v_name": self.all_eventinfo["threat_info"]["source_load"][0]["v_name"],
            "direction": ""}
        self.all_eventinfo["Used_by_ES_dst"] = {
            "purpose_endpoint_ip": self.all_eventinfo["threat_info"]["source_endpoint"]["ip_info"]["ip"],
            "purpose_endpoint_domain": "",
            "purpose_endpoint_url": "",
            "purpose_endpoint_port":"",
            "purpose_endpoint_ip_cp":"",
            "purpose_load_md5": self.all_eventinfo["threat_info"]["purpose_load"]["sfile_hash"]["md5"],
            "purpose_load_filename": self.all_eventinfo["threat_info"]["purpose_load"]["sprocess_path"],
            "purpose_load_filepath": self.all_eventinfo["threat_info"]["purpose_load"]["o_filename"],
            "purpose_load_filebytes": self.all_eventinfo["threat_info"]["purpose_load"]["size"],
            "purpose_load_filetype": self.all_eventinfo["threat_info"]["purpose_load"]["file_format"],
            "purpose_load_attack_v_name": self.all_eventinfo["threat_info"]["purpose_load"]["v_name"],
            "purpose_load_vulne_id": [i["vulne_id"] for i in self.all_eventinfo["threat_info"]["purpose_load"]["vulne"]] if self.all_eventinfo["threat_info"]["purpose_load"]["vulne"] != [] else [],
            "purpose_load_attack_action": self.all_eventinfo["threat_info"]["purpose_load"]["attack_action"],
            "direction": ""}
        if data["type"] == "active_protect":
            flag = 0  ##  白
        else:
            flag = 1  ##  黑
        self.all_eventinfo["Used_by_ES_ExtendInfo"]["is_malicious"] = True if flag == 1 else False
        return flag,self.all_eventinfo


















        
    def tranData(self,data,sql_data,uuidlist,attck_dist):
        eventtype=["first_type","second_type","third_type","fourth_type"]
        ##把文件名%E6%B5%8B%E8%AF%95abc变成中文 
        data["data"]["i"]["1"][0]["3"] = urllib.parse.unquote(data["data"]["i"]["1"][0]["3"])
        data["data"]["i"]["1"][0]["4"] = urllib.parse.unquote(data["data"]["i"]["1"][0]["4"])
        data["data"]["i"]["1"][0]["g"]["2"] = urllib.parse.unquote(data["data"]["i"]["1"][0]["g"]["2"])
        data["data"]["i"]["1"][0]["g"]["3"] = urllib.parse.unquote(data["data"]["i"]["1"][0]["g"]["3"])
        ##获取漏洞信息
        
        if data["data"]["i"]["2"] !=[] and data["data"]["i"]["2"] != None:
            vulne_1,vulne_2,vulne_3 = self.vulne_f(data["data"]["i"]["2"])
        
        self.all_eventinfo["threat_info"]["eventlog_id"]=data["data"]["a"]
        self.all_eventinfo["threat_info"]["sponsor_time"]=str(data["data"]["b"])
        for i in range(len(eventtype)):
            self.all_eventinfo["threat_info"]["event_type"][eventtype[i]]=data["data"]["c"][str(i+1)]
            
        ##  丰富源端点信息
        
        row = sql_data[data["client"]["uuid"]] 
        data["data"]["i"]["3"][0]["2"] = row[0][10]

        self.all_eventinfo["threat_info"]["source_endpoint"]["ip_info"]["ip"]=data["data"]["d"]["1"][0]["1"] if data["data"]["d"]["1"]!=None else ""
        self.all_eventinfo["threat_info"]["source_endpoint"]["ip_info"]["mac"]=data["data"]["d"]["1"][0]["2"] if data["data"]["d"]["1"]!=None else ""
        if self.all_eventinfo["threat_info"]["source_endpoint"]["ip_info"]["ip"] == "":
            self.all_eventinfo["threat_info"]["source_endpoint"]["ip_info"]["ip"] = data["client"]["ip"]
        if data["client"]["uuid"] not in uuidlist:
            self.all_eventinfo["threat_info"]["source_endpoint"]["asset_detail"]={
                "asset_id": row[0][10],
                "os": "",
                "ip_group":[{"ip":x["1"],"mac":x["2"]} for x in data["data"]["d"]["1"]],
                "unit": data["data"]["i"]["3"][0]["5"]["1"],
                "network_struct":[],
                "type": "",
                "ports":[],
                "asset_level": "",
                "people": {
                    "user": "",
                    "manager": ""
                },
                "applications": []
            }
        else:
            self.all_eventinfo["threat_info"]["source_endpoint"]["asset_detail"]={
                "asset_id": row[0][10],
                "os": row[0][0] if row[0][0] !=None else "",
                "ip_group":[{"ip":x[2],"mac":x[3]} for x in row],
                "unit": row[0][7] if row[0][7] !=None else "",
                "network_struct": [row[0][9]] if row[0][9] !=None else [],
                "type": row[0][8] if row[0][8] !=None else "",
                "ports": [],
                "asset_level": str(row[0][4]) if row[0][4] !=None else "",
                "people": {
                    "user": row[0][5] if row[0][5] !=None else "",
                    "manager": row[0][6] if row[0][6] !=None else ""
                },
                "applications": row[0][1].split(',') if row[0][1]!=None else []
            }
        #  丰富行为信息
        for i in range(len(eventtype)):
            self.all_eventinfo["threat_info"]["sponsor_behavior"]["attack_info"][eventtype[i]]=data["data"]["f"]["1"][str(i+1)]
        if "2" in data["data"]["f"]:
            for value in vulne_1:
                if value["vulne_id"] == data["data"]["f"]["2"]["1"]:
                    self.all_eventinfo["threat_info"]["sponsor_behavior"]["exploit_vulnerability"]=value
        self.all_eventinfo["threat_info"]["sponsor_behavior"]["level"]=data["data"]["f"]["3"]
        self.all_eventinfo["threat_info"]["sponsor_behavior"]["dispose"]=data["data"]["f"]["4"]
        attck=data["data"]["f"]["5"].split(".") if data["data"]["f"]["5"]!= "" else ['','','','']
        self.all_eventinfo["threat_info"]["sponsor_behavior"]["xw_name"]=[data["data"]["f"]["1"]["3"]]
        self.all_eventinfo["threat_info"]["sponsor_behavior"]["xw_label"]=[data["data"]["f"]["1"]["3"]]
        attack=data["data"]["f"]["7"].split(".") if "7" in data["data"]["f"] and data["data"]["f"]["7"]!= "" else ['','']
        stage_number={
            "准备":"2",
            "交互":"3",
            "存在":"4",
            "影响":"5",
            "持续":"6",
            "管理":"1"
        }
        attack_label={
            "初始访问":"1",
            "执行":"2",
            "持久化":"3",
            "提权":"4",
            "防御规避":"5",
            "凭证访问":"6",
            "发现":"7",
            "横向运动":"8",
            "收集":"9",
            "命令控制":"10",
            "渗透":"11",
            "影响":"12"
        }
        
        self.all_eventinfo["threat_info"]["sponsor_behavior"]["attack_label"]={
                                                                                "attack": [{
                                                                                    "tactic": attack[0],
                                                                                    "techniques": attack[1] ,
                                                                                    "tactic_number": attack_label[attack[0]]  if attack[0] in attack_label else "",
                                                                                    "techniques_number": attck_dist[attack[1]] if attack[0] in attack_label else ""
                                                                                }],
                                                                                "nsacss": [{
                                                                                    "stage": attck[0],
                                                                                    "objective": attck[1],
                                                                                    "action": attck[2],
                                                                                    "stage_number":stage_number[attck[0]] if attck[0] in stage_number else ""
                                                                                }]
                                                                            }
        self.all_eventinfo["threat_info"]["sponsor_behavior"]["detect"]={
                "detect_time": str(data["data"]["i"]["3"][0]["1"]),
                "detect_pro_id": data["data"]["i"]["3"][0]["2"],
                "detect_pro": data["data"]["i"]["3"][0]["3"],
                "detect_rule": {
                    "detect_base": data["data"]["i"]["3"][0]["4"]["1"],
                    "detect_id": data["data"]["i"]["3"][0]["4"]["2"]
                }
            }
        #   此处判断被检测对象是否是源载荷  如果不是 ：：
        ##TODO  丰富源载荷信息  
        if data["data"]["i"]["3"][0]["6"] not in ["源载荷"]:
            self.all_eventinfo["threat_info"]["source_load"][0]["fprocess_pid"]=str(data["data"]["i"]["1"][0]["g"]["1"])
            self.all_eventinfo["threat_info"]["source_load"][0]["o_filename"]=data["data"]["i"]["1"][0]["g"]["2"]
            self.all_eventinfo["threat_info"]["source_load"][0]["fprocess_path"]=data["data"]["i"]["1"][0]["g"]["3"]
            self.all_eventinfo["threat_info"]["source_load"][0]["fprocess_hash"]["md5"]=data["data"]["i"]["1"][0]["g"]["4"]["1"]
            self.all_eventinfo["threat_info"]["source_load"][0]["fprocess_hash"]["sha1"]=data["data"]["i"]["1"][0]["g"]["4"]["2"]
            self.all_eventinfo["threat_info"]["source_load"][0]["fprocess_hash"]["sha256"]=data["data"]["i"]["1"][0]["g"]["4"]["3"]
            self.all_eventinfo["threat_info"]["source_load"][0]["defense_state"]=str(data["data"]["e"][0].get("p",""))
            
            #TODO   目标载荷填充  漏洞信息
            self.all_eventinfo["threat_info"]["purpose_load"]={
                "sprocess_pid":str(data["data"]["i"]["1"][0]["1"]),
                "p_load_cmd": data["data"]["i"]["1"][0].get("5"),
                "p_load_extend_info_from_cmd": [],
                "p_load_behavior_info_additional": data["data"]["i"]["1"][0].get("l"),
                "behavior_initiator": data["data"]["i"]["1"][0].get("k"),
                "sfile_hash": {
                    "md5": data["data"]["i"]["1"][0]["2"]["1"],
                    "sha1":data["data"]["i"]["1"][0]["2"]["2"],
                    "sha256":data["data"]["i"]["1"][0]["2"]["3"]
                },
                "sprocess_path":data["data"]["i"]["1"][0]["3"],
                "o_filename": data["data"]["i"]["1"][0]["4"],
                "file_format":data["data"]["i"]["1"][0]["7"],
                "size": data["data"]["i"]["1"][0]["6"],
                "v_name": data["data"]["i"]["1"][0]["8"],
                "v_type": "",
                "v_family": "",
                "platform":data["data"]["i"]["1"][0]["9"],
                "attack_action": [data["data"]["i"]["1"][0]["e"]],
                "vulne":[],
                "level": data["data"]["i"]["1"][0]["a"],
                "dispose": data["data"]["f"]["4"],
                "download_time": str(data["data"]["i"]["1"][0]["b"]),
                "run_time": str(data["data"]["i"]["1"][0]["c"]),
                "loader_label": vulue_label(data["data"]["i"]["1"][0]["8"],0) ,
                "loader_behavior_label": vulue_label(data["data"]["i"]["1"][0]["8"],1) ,
                "defense_state":str(data["data"]["g"][0].get("p","")),
                "detect": {
                    "detect_time": str(data["data"]["i"]["3"][0]["1"]),
                    "detect_pro_id": data["data"]["i"]["3"][0]["2"],
                    "detect_pro": data["data"]["i"]["3"][0]["3"],
                    "detect_rule": {
                        "detect_base": data["data"]["i"]["3"][0]["4"]["1"],
                        "detect_id": data["data"]["i"]["3"][0]["4"]["2"]
                    }
                }
            }
            #提取命令行载荷
            if "5" in data["data"]["i"]["1"][0]:
                if data["data"]["i"]["1"][0]["9"]=="Windows":
                    p_load_extend_info_from_cmd=re.findall(r"(([abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ]:){1}(\\[^\\/<>\|\*\?]+)+)+",data["data"]["i"]["1"][0]["5"])
                else:
                    p_load_extend_info_from_cmd=re.findall(r"((^[\s]*[/|\./|\.\.]{1}([^\s]+)){1}((/){1}([^\s]+))+)",data["data"]["i"]["1"][0]["5"])
                if p_load_extend_info_from_cmd != []:
                    self.all_eventinfo["threat_info"]["purpose_load"]["p_load_extend_info_from_cmd"]=[cmd[0] for cmd in p_load_extend_info_from_cmd]    

                
            if "3" in data["data"]["g"][0]: 
                for value in data["data"]["g"][0]["3"]:
                    if value["1"] in vulne_3:
                        self.all_eventinfo["threat_info"]["purpose_load"]["vulne"].append(vulne_1[vulne_3.index(value["1"])]) 
        ##   此处判断被检测对象是否是源载荷  如果是 ：： 
        else:
            self.all_eventinfo["threat_info"]["source_load"][0]={
                "fprocess_pid":str(data["data"]["i"]["1"][0]["1"]),
                "fprocess_hash": {
                    "md5": data["data"]["i"]["1"][0]["2"]["1"],
                    "sha1":data["data"]["i"]["1"][0]["2"]["2"],
                    "sha256":data["data"]["i"]["1"][0]["2"]["3"]
                },
                "fprocess_path":data["data"]["i"]["1"][0]["3"],
                "o_filename": data["data"]["i"]["1"][0]["4"],
                "file_format":data["data"]["i"]["1"][0]["7"],
                "size": data["data"]["i"]["1"][0]["6"],
                "v_name": data["data"]["i"]["1"][0]["8"],
                "v_type": "",
                "v_family": "",
                "platform":data["data"]["i"]["1"][0]["9"],
                "attack_action": [data["data"]["i"]["1"][0]["e"]],
                "vulne": [],
                "level": data["data"]["i"]["1"][0]["a"],
                "dispose": data["data"]["f"]["4"],
                "download_time": data["data"]["i"]["1"][0]["b"],
                "run_time": data["data"]["i"]["1"][0]["c"],
                "loader_label": vulue_label(data["data"]["i"]["1"][0]["8"],0) ,
                "loader_behavior_label": vulue_label(data["data"]["i"]["1"][0]["8"],1) ,
                "defense_state":str(data["data"]["e"][0].get("p","")),
                "detect": {
                    "detect_time": data["data"]["i"]["3"][0]["1"],
                    "detect_pro_id": data["data"]["i"]["3"][0]["2"],
                    "detect_pro": data["data"]["i"]["3"][0]["3"],
                    "detect_rule": {
                        "detect_base": data["data"]["i"]["3"][0]["4"]["1"],
                        "detect_id": data["data"]["i"]["3"][0]["4"]["2"]
                    }
                }
            }
            if "3" in data["data"]["e"][0]: 
                for value in data["data"]["e"][0]["3"]:
                    if value["1"] in vulne_3:
                        self.all_eventinfo["threat_info"]["source_load"][0]["vulne"].append(vulne_1[vulne_3.index(value["1"])])
                        
        #   目标载体填充
        self.all_eventinfo["threat_info"]["source_endpoint"]["detect"]={
                                                            "detect_time": data["data"]["i"]["3"][0]["1"],
                                                            "detect_pro_id": data["data"]["i"]["3"][0]["2"],
                                                            "detect_pro": data["data"]["i"]["3"][0]["3"],
                                                            "detect_rule": {
                                                                "detect_base": data["data"]["i"]["3"][0]["4"]["1"],
                                                                "detect_id": data["data"]["i"]["3"][0]["4"]["2"]
                                                            }
                                                        }
        self.all_eventinfo["threat_info"]["purpose_endpoint"]=self.all_eventinfo["threat_info"]["source_endpoint"]
        # 扩展信息填充
        self.all_eventinfo["threat_info"]["extend_details"]["loader_info"][0]={
            "process_pid":data["data"]["i"]["1"][0]["1"],
            "file_hash": {
            "md5": data["data"]["i"]["1"][0]["2"]["1"],
            "sha1":data["data"]["i"]["1"][0]["2"]["2"],
            "sha256":data["data"]["i"]["1"][0]["2"]["3"]
            },
            "process_path":data["data"]["i"]["1"][0]["3"],
            "o_filename":data["data"]["i"]["1"][0]["4"],
            "size": data["data"]["i"]["1"][0]["6"],
            "file_format":data["data"]["i"]["1"][0]["7"],
            "v_name": data["data"]["i"]["1"][0]["8"],
            "platform":data["data"]["i"]["1"][0]["9"],
            "level": data["data"]["i"]["1"][0]["a"],
            "download_time": str(data["data"]["i"]["1"][0]["b"]),
            "run_time": str(data["data"]["i"]["1"][0]["c"]),
            "label": [i for i in vulue(data["data"]["i"]["1"][0]["8"]) if i != None],
            "attack_action": [data["data"]["i"]["1"][0]["e"]],
            "vulne": vulne_1 if data["data"]["i"]["2"] !=[] and data["data"]["i"]["2"] != None else [],
            "relation": {
                "fprocess_pid":data["data"]["i"]["1"][0]["g"]["1"],
                "o_filename": data["data"]["i"]["1"][0]["g"]["2"],
                "fprocess_path": data["data"]["i"]["1"][0]["g"]["3"],
                "ffile_hash": {
                    "md5": data["data"]["i"]["1"][0]["g"]["4"]["1"],
                    "sha1": data["data"]["i"]["1"][0]["g"]["4"]["2"],
                    "sha256":data["data"]["i"]["1"][0]["g"]["4"]["3"]
                },
                "sprocess_pid": "",
                "p_filename": "",
                "sprocess_path": "",
                "sfile_hash": {
                    "md5": "",
                    "sha1": "",
                    "sha256": ""
                }
            },
            "version_info": {
                "author": data["data"]["i"]["1"][0]["h"]["1"],
                "update_time": str(data["data"]["i"]["1"][0]["h"]["2"]),
                "company": data["data"]["i"]["1"][0]["h"]["3"],
                "create_time": str(data["data"]["i"]["1"][0]["h"]["4"]),
                "file_version": data["data"]["i"]["1"][0]["h"]["5"],
                "last_updater": "",
                "last_visit_time": str(data["data"]["i"]["1"][0]["h"]["6"])
            },
            "certificate_info": {
                "file_signature": "",
                "file_signature_time": ""
            },
            "dynamic_result": data["data"]["i"]["1"][0].get("j")
        }
        if "i" in data["data"]["i"]["1"][0]:
            self.all_eventinfo["threat_info"]["extend_details"]["loader_info"][0]["certificate_info"]["file_signature"]=data["data"]["i"]["1"][0]["i"]["7"]
            self.all_eventinfo["threat_info"]["extend_details"]["loader_info"][0]["certificate_info"]["file_signature_time"]=data["data"]["i"]["1"][0]["i"]["8"]
        if "j" in data["data"]["i"]["1"][0]:
            self.all_eventinfo["threat_info"]["extend_details"]["loader_info"][0]["dynamic_result"]=data["data"]["i"]["1"][0]["j"]
        self.all_eventinfo["threat_info"]["extend_details"]["vulne_info"] = vulne_2 if data["data"]["i"]["2"] !=[] and data["data"]["i"]["2"] != None else []
        self.all_eventinfo["threat_info"]["extend_details"]["detector_info"][0]={
                "detect_time": str(data["data"]["i"]["3"][0]["1"]),
                "detect_pro_id": data["data"]["i"]["3"][0]["2"],
                "detect_pro": data["data"]["i"]["3"][0]["3"],
                "detect_rule": {
                    "detect_base": data["data"]["i"]["3"][0]["4"]["1"],
                    "detect_id": data["data"]["i"]["3"][0]["4"]["2"]
                },
                "d_proinfo": {
                    "unit": data["data"]["i"]["3"][0]["5"]["1"] if "5" in data["data"]["i"]["3"][0] else "",
                    "safety_region": "",
                    "network_region": "",
                    "location": "",
                    "people": ""
                },
                "detect_obj" : data["data"]["i"]["3"][0]["6"]
            }
        if "h" in data["data"]:
            self.all_eventinfo["threat_info"]["extend_details"]["purpose_carrier"]={
                "reg":{
                    "reg_path": data["data"]["h"]["1"].get("1") if "1" in data["data"]["h"] else "",
                    "reg_key":data["data"]["h"]["1"].get("2") if "1" in data["data"]["h"] else "",
                    "reg_value":data["data"]["h"]["1"].get("3") if "1" in data["data"]["h"] else ""
                    },
                "task":{
                    "task_name":data["data"]["h"]["2"].get("1") if "2" in data["data"]["h"] else "",
                    "task_value":data["data"]["h"]["2"].get("2") if "2" in data["data"]["h"] else "",
                    "task_path":data["data"]["h"]["2"].get("3") if "2" in data["data"]["h"] else "",
                    "task_param":data["data"]["h"]["2"].get("4") if "2" in data["data"]["h"] else ""
                    },
                "runtime":str(data["data"]["h"].get("3")),
                "level":data["data"]["h"].get("4")
            }
        self.all_eventinfo["Used_by_ES_src"] = {
            "source_endpoint_ip": self.all_eventinfo["threat_info"]["source_endpoint"]["ip_info"]["ip"],
            "source_endpoint_domain": "",
            "source_endpoint_url": "",
            "source_endpoint_port":"",
            "source_endpoint_ip_cp":"",
            "source_load_md5": self.all_eventinfo["threat_info"]["source_load"][0]["fprocess_hash"]["md5"],
            "source_load_filename": self.all_eventinfo["threat_info"]["source_load"][0]["fprocess_path"],
            "source_load_filepath": self.all_eventinfo["threat_info"]["source_load"][0]["o_filename"],
            "source_load_filebytes": "",
            "source_load_filetype": "",
            "source_load_vulne_id":[i["vulne_id"] for i in self.all_eventinfo["threat_info"]["source_load"][0]["vulne"]] if self.all_eventinfo["threat_info"]["source_load"][0]["vulne"] != [] else [],
            "source_load_attack_action": self.all_eventinfo["threat_info"]["source_load"][0]["attack_action"],
            "source_load_attack_v_name": self.all_eventinfo["threat_info"]["source_load"][0]["v_name"],
            "source_load_attack_v_type": "",
            "source_load_attack_v_family": "",   
            "direction": ""}

        self.all_eventinfo["Used_by_ES_dst"] = {
            "purpose_endpoint_ip": self.all_eventinfo["threat_info"]["source_endpoint"]["ip_info"]["ip"],
            "purpose_endpoint_domain": "",
            "purpose_endpoint_url": "",
            "purpose_endpoint_port":"",
            "purpose_endpoint_ip_cp":"",
            "purpose_load_md5": self.all_eventinfo["threat_info"]["purpose_load"]["sfile_hash"]["md5"],
            "purpose_load_filename": self.all_eventinfo["threat_info"]["purpose_load"]["sprocess_path"],
            "purpose_load_filepath": self.all_eventinfo["threat_info"]["purpose_load"]["o_filename"],
            "purpose_load_filebytes": self.all_eventinfo["threat_info"]["purpose_load"]["size"],
            "purpose_load_filetype": self.all_eventinfo["threat_info"]["purpose_load"]["file_format"],
            "purpose_load_attack_v_name": self.all_eventinfo["threat_info"]["purpose_load"]["v_name"],
            "purpose_load_vulne_id": [i["vulne_id"] for i in self.all_eventinfo["threat_info"]["purpose_load"]["vulne"]] if self.all_eventinfo["threat_info"]["purpose_load"]["vulne"] != [] else [],
            "purpose_load_attack_action": self.all_eventinfo["threat_info"]["purpose_load"]["attack_action"],
            "purpose_load_attack_v_type": "",
            "purpose_load_attack_v_family": "", 
            "direction": ""}
        if data["data"]["i"]["1"][0]["8"] == "":
            
            flag = 0  ##  白
        else:
            flag = 1  ##  黑
        self.all_eventinfo["Used_by_ES_ExtendInfo"]["is_malicious"] = True if flag == 1 else False
        return flag,self.all_eventinfo

def vulue(data):
    behivior_dict = {
    "adtool": "广告工具",
    "adware": "广告软件",
    "arcbomb": "包裹炸弹",
    "avtool": "反病毒工具",
    "backdoor": "后门",
    "badjoke": "恶意玩笑程序",
    "banker": "银行木马",
    "clicker": "点击器（刷数据）",
    "client-irc": "IRC客户端",
    "client-p2p": "P2P客户端",
    "client-smtp": "SMTP客户端",
    "constructor": "病毒生成器",
    "cracktool": "破解工具",
    "ddos": "分布式拒绝服务攻击",
    "dialer": "拨号程序",
    "dos": "拒绝服务攻击",
    "downloader": "下载者",
    "dropper": "捆绑者",
    "email": "通过Email传播",
    "exploit": "溢出代码",
    "fakeav": "伪杀毒软件",
    "flooder": "洪水攻击",
    "fraudtool": "恶意欺骗工具",
    "gamethief": "盗窃游戏账号",
    "garbage": "垃圾文件",
    "hacktool": "黑客工具",
    "hoax": "恶作剧程序",
    "im": "通过即时通讯传播",
    "irc": "通过IRC传播",
    "joke": "玩笑程序",
    "mailfinder": "搜集邮件",
    "monitor": "监控程序",
    "multipacked": "多重加壳",
    "net": "通过网络传播",
    "nettool": "网络工具",
    "notifier": "通知程序",
    "p2p": "通过P2P传播",
    "packed": "包裹（壳）",
    "porn-dialer": "色情软件",
    "porn-downloader": "色情下载程序",
    "porn-tool": "色情工具",
    "proxy": "代理程序",
    "psw": "窃取密码",
    "pswtool": "窃取密码工具",
    "ransom": "勒索软件",
    "remoteadmin": "远程管理软件",
    "risktool": "风险工具",
    "rootkit": "隐匿程式",
    "server-ftp": "FTP服务端",
    "server-proxy": "代理服务端",
    "server-telnet": "Telnet服务端",
    "server-web": "Web服务端",
    "sms": "短信木马",
    "spamtool": "垃圾邮件工具",
    "spoof": "欺骗工具",
    "spy": "窃取行为",
    "suspiciouspacker": "可疑包（壳）",
    "tool": "攻击工具",
    "virtool": "病毒工具",
    "webtoolbar": "浏览器工具条",
    "modifier": "浏览器修改",
    "autorun": "自动运行",
    "injector": "注射式攻击",
    "phishing": "网络仿冒",
    "toolbar": "篡改工具栏",
    "keylogger": "键盘记录器",
    "filecoder": "文件加密",
    "addisplay": "插播广告",
    "lockscreen": "锁定屏幕",
    "spammer": "发送垃圾邮件",
    "hllw": "伴生文件",
    "sniffer": "嗅探器",
    "bundler": "打包器",
    "rogue": "流氓软件",
    "riskware": "风险软件",
    "trojan": "木马程序",
    "testfile": "测试文件",
    "junkfile": "垃圾文件",
    "vcs": "启发式病毒",
    "grayware": "灰色软件",
    "spyware": "风险软件",
    "virus": "感染式病毒",
    "worm": "蠕虫程序",
    "hacktool": "黑客工具",
    "attempted-admin": "尝试获取管理员权限",
    "attempted-user": "尝试获取用户权限",
    "default-login-attempt": "尝试以默认用户名和密码登录",
    "successful-admin": "成功获取管理员权限",
    "successful-user": "成功获取用户权限",
    "unsuccessful-user": "获取用户权限失败",
    "suspicious-login": "可疑登录",
    "network-scan": "网络扫描",
    "attempted-recon": "尝试侦察探测",
    "successful-recon-largescale": "大规模侦察探测成功",
    "successful-recon-limited": "侦察探测成功",
    "attempted-dos": "尝试造成拒绝服务",
    "denial-of-service": "拒绝服务攻击",
    "successful-dos": "成功造成拒绝服务",
    "web-application-activity": "Web应用活动",
    "web-application-attack": "Web应用攻击",
    "misc-activity": "其他活动",
    "trojan-activity": "C2通信",
    "shellcode-detect": "缓冲区溢出尝试",
    "unusual-client-port-connection": "非常见端口使用",
    "non-standard-protocol": "非标准协议",
    "policy-violation": "疑似违规",
    "protocol-command-decode": "通用协议命令",
    "icmp-event": "通用ICMP事件",
    "tcp-connection": "TCP连接",
    "string-detect": "可疑字符串",
    "suspicious-filename-detect": "可疑文件名",
    "bad-unknown": "潜在恶意流量",
    "inappropriate-content": "不当内容",
    "not-suspicious": "非可疑流量",
    "rpc-portmap-decode": "远程调用解码",
    "system-call-detect": "系统调用"
}
    data = data.split("/")[0]
    data = data.split("[")
    if len(data) == 2:
        return behivior_dict.get(data[0],None),behivior_dict.get(data[1][:-1],None)
    elif len(data)==1:
        return behivior_dict.get(data[0],None),None
    else:
        return None,None


def vulue_label(data,id):
    data = [i for i in vulue(data)]
    return [data[id]] if data[id]!=None else []

if __name__ == "__main__":
    pg_conf=json.loads(config_env["pg_conn"])
    conn = psycopg2.connect(database=pg_conf["database"], user=pg_conf["user"], password=pg_conf["password"], host=pg_conf["host"], port=pg_conf["port"])
    attck_dict = ieplog.get_k_attck(conn,["命令控制.测试22"])
    damo = Datademo()
    data1 ={
    "data": {
        "a": "1579231117036",
        "c": {
            "1": "",
            "3": "",
            "2": "",
            "4": ""
        },
        "b": 1579231373,
        "e": [{
            "1": 560,
            "p": 1,
            "2": {
                "1": "71C85477DF9347FE8E7BC55768473FCA"
            }
        }],
        "d": {
            "1": [{
                "1": "192.168.154.237",
                "2": "00-0C-29-AD-DD-42"
            }]
        },
        "g": [{
            "1": 2244,
            "p": 1,
            "2": {
                "1": "9AD833027AF42AEFCA1FE6CD64F31B22"
            }
        }],
        "f": {
            "1": {
                "1": "\u4e3b\u673a\u653b\u51fb\u884c\u4e3a",
                "3": "\u542f\u52a8\u8fdb\u7a0b",
                "2": "\u8fdb\u7a0b\u884c\u4e3a",
                "4": ""
            },
            "3": "",
            "5": "",
            "4": "\u5df2\u653e\u884c",
            "7": "命令控制.测试22",
            "6": ""
        },
        "i": {
            "1": [{
                "a": "",
                "c": 1579231373,
                "b": 1462079956,
                "e": "",
                "d": "",
                "g": {
                    "1": 560,
                    "3": "C:\\Windows\\System32\\services.exe",
                    "2": "services.exe",
                    "4": {
                        "1": "71C85477DF9347FE8E7BC55768473FCA",
                        "3": "A86D6A6D1F5A0EFCD649792A06F3AE9B37158D48493D2ECA7F52DCC1CB9B6536",
                        "2": "FF658A36899E43FEC3966D608B4AA4472DE7A378"
                    }
                },
                "h": {
                    "1": "Microsoft Corporation",
                    "3": "Microsoft Corporation",
                    "2": 1462079956,
                    "5": "10.0.14275.1000",
                    "4": 1462079956,
                    "6": 1462079956
                },
                "k": "SYSTEM",
                "1": 2244,
                "3": "C:\\Windows\\System32\\CompatTelRunner.exe",
                "2": {
                    "1": "9AD833027AF42AEFCA1FE6CD64F31B22",
                    "3": "FE4AEF90114733559E0BB09C2AA336B124E0D850061000D0CF2F0B5E0D91E1DD",
                    "2": "D6B334C76EC56B985E6F860A527C799E678A523B"
                },
                "5": "",
                "4": "CompatTelRunner.exe",
                "7": "PE",
                "6": 38120,
                "9": "Windows",
                "8": "trojan[suspicious-login]/msil.ageneric(asmalws@20745)"
            }],
            "3": [{
                "1": 1579231373,
                "3": "IEP",
                "2": "58DA42C6C010F7B783661FB201DC024C",
                "5": {
                    "1": "Antiy Labs"
                },
                "4": {
                    "1": "\u654f\u611f\u884c\u4e3a\u89c4\u5219\u547d\u4e2d",
                    "2": "10002"
                },
                "6": "\u76ee\u7684\u8f7d\u8377"
            }],
            "2": []
        }
    },
    "client": {
        "name": "USER-20171218JH",
        "server_uid": "fdd27420-1c6a-42a0-aaa2-cc8e6519b681",
        "ip": "192.168.154.237",
        "company_id": 1,
        "server_ip": "10.255.49.17",
        "group_id": 1,
        "uuid": "58DA42C6C010F7B783661FB201DC024C"
    },
    "type": "eventlog",
    "ts": 1579231305
}
    flag,data=damo.tranData(data1,{"58DA42C6C010F7B783661FB201DC024C":[("","",{"":""},"","3","","","","","",'58DA42C6C010F7B783661FB201DC024C',"58DA42C6C010F7B783661FB201DC024C")]},[],attck_dict)
    print(json.dumps(data,ensure_ascii=False,sort_keys=True, indent=4, separators=(', ', ': ')))
    #print([i for i in vulue("[suspicious-login]/msil.ageneric(asmalws@20745)")])
    #print( vulue_label("[suspicious-login]/msil.ageneric(asmalws@20745)",1))