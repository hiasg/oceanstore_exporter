#!/usr/bin/env python3
"""
Exporter for Huawei Ocean Dorado Storage
"""

import argparse
import logging
import sys
import time
from configparser import ConfigParser
from pprint import pprint as pp
import requests
from requests.exceptions import HTTPError
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

__VERSION__ = 0.1

def arguments():
    parser = argparse.ArgumentParser(
        description='Huawei Dorado Storage exporter',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    # expose is not jet implemented
    parser.add_argument("-a", "--listen_addr",type=str,
                       default="127.0.0.1",
                       help="TCP address to expose metrics")
    parser.add_argument("--config", "-c", type=str,
                       help="config file")
    parser.add_argument("--target", "-t", type=str,
                       help="Ocean Storage")
    parser.add_argument("--timeout",type=int,
                       default=10,
                       help="connection timeout (default 10sec")
    parser.add_argument("-p", "--listen_port",type=int,
                       default=8088,
                       help="TCP port to expose metrics")
    parser.add_argument('--verbose', '-v',
                       required=False,
                       action='count',
                       default=1,
                       help='Verbose output')
    parser.add_argument('--pipe',type=str,
                       default='stderr',
                       help='Default logoutput to stderr. Nedd to be visible on expexp log')
    parser
    return parser.parse_args()

def configargs(args):
    conf = {}
    try:
        config = ConfigParser()
        config.read(args.config)
        for section in config.sections():
            conf[section] = {}
            for k,v in config.items(section):
                conf[section][k] = v
    except Exception as e:
        logger.error(f"{e}")
    if severity(args.verbose) == 'DEBUG':
        logger.debug(f"Read from config file:\n{conf}")
    return conf

def severity(level):
    if level > 5:
        level = 5
    log_levels = {
        1: 'CRITICAL',
        2: 'ERROR',
        3: 'WARNING',
        4: 'INFO',
        5: 'DEBUG',
    }
    return log_levels[level]

class OceanStor(object):
    def __init__(self, host, port, username, password, timeout) -> None:
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.timeout = timeout
        self.url = f"https://{self.host}:{self.port}/deviceManager/rest"
        self.session = requests.Session()
        self.session.verify = False

    def login(self):
        logger.debug(f"start login")
        try:
            response = self.session.post(self.url + '/xxxxx/sessions',json={'scope': 0,'username': self.username,'password': self.password})
        except HTTPError as HttpErr:
            logger.error(f"login {HttpErr}")
        except Exception as err:
            logger.critical(f"login {err}")
        resp = response.json()
        logger.debug(f"{resp}")
        if resp['error']['code'] != 0:
            logger.error(f"login {resp['error']['description']} {resp['error']['suggestion']}")
            sys.exit(3)
        elif not 'deviceid' in resp['data']:
            logger.critical(f"login no deviceID found -> exit")
            sys.exit(3)
        else:
            self.deviceID = resp['data']['deviceid']
            self.session.headers.update({'iBaseToken': resp['data']['iBaseToken'], 'Content-Type': 'application/json', 'Accept': 'application/json'})
        return True

    def get_data(self,endpoint):
        try:
            response = self.session.get(self.url + '/' + self.deviceID + '/' + endpoint )
        except HTTPError as HttpErr:
            logger.error(f"login {HttpErr}")
        except Exception as err:
            logger.critical(f"login {err}")
        data = response.json()
        return data

    def get_perf_data(self,stats_uid,data_ids):
        params = {"CMO_STATISTIC_UUID": stats_uid, "CMO_STATISTIC_DATA_ID_LIST": data_ids}
        try:
            response = self.session.get(self.url + '/' + self.deviceID + '/performace_statistic/cur_statistic_data', params=params )
        except HTTPError as HttpErr:
            logger.error(f"login {HttpErr}")
        except Exception as err:
            logger.critical(f"login {err}")
        data = response.json()
        return data
    
    def logout(self):
        logger.debug(f"start logout")
        try:
            resp = self.session.delete(self.url + '/' + self.deviceID + '/sessions')
        except HTTPError as HttpErr:
            logger.error(f"logout {HttpErr}")
        except Exception as err:
            logger.error(f"logout {err}")
        logger.debug(f"logout {resp.json()}")
        return True

def valuemap(typ,id):
    vmap = {
        "health_status": {
            "0": "unknown",
            "1": "normal",
            "2": "faulty",
            "3": "about_to_fail",
            "5": "degraded",
            "9": "inconsistent",
            "11": "no input",
            "12": "low_battery"
            },
        "running_status": {
            "0": "unknown",
            "1": "normal",
            "2": "running",
            "3": "not_running",
            "5": "sleep_in_high_temperature",
            "8": "spin_down",
            "10": "link_up",
            "11": "link_down",
            "12": "powering_on",
            "13": "powering_off",
            "14": "pre-copy",
            "16": "reconstruction",
            "27": "online",
            "28": "offline",
            "32": "balancing",
            "48": "charging",
            "49": "charging_completed",
            "50": "discharging",
            "32": "balancing",
            "53": "initializing",
            "103": "power_on_failed",
            "106": "deleting",
            }, 
        "data_ids": {
            "read_iops": "22",
            "read_mbytes": "23",
            "write_iops": "28",
            "write_mbytes": "26",
            "max_read_latency": "382",
            "max_write_latency": "383",
            "avg_read_latency": "384",
            "avg_write_latency": "385",
            "max_latency": "371",
            "failed_reads": "532",
            "failed_writes": "533",
            "usage": "18",
            "queue_length": "19",
            "avg_cpu_usage": "68",
            "avg_cache_usage": "69",
            "read_cache_hits": "93",
            "write_cache_hits": "95",
            "read_cache_usage": "110",
            "write_cache_usage": "120",
            "cache_page_usage": "1055",
            "cache_chunk_usage": "1056",
            "max_read_kbytes": "802",
            "max_write_kbytes": "803",
            },    
        "eth_port_types": {
            "0": "host_port/service_port",
            "1": "expansion_port",
            "2": "management_port",
            "3": "internal_port",
            "4": "maintenance_port",
            "5": "management/service_port",
            "6": "maintenance/service_port",
            "11": "cluster_port"
            },
        }
    return vmap[typ][id]

def get_power_data(connection):
    data = connection.get_data("power")
    metrics = []
    for entry in data["data"]:
        labels = [
                    ("type", "PSU"),
                    ("serial", entry["SERIALNUMBER"]),
                    ("id", entry["ID"]),
                    ("model", entry["MODEL"]),
                    ("name", entry["NAME"]),
                    ("location", entry["LOCATION"]),
                    ]
        metric_dict = {
                            "key": "huawei_storage_component_health_status",
                            "value": entry["HEALTHSTATUS"],
                            "customlabels": [("status_text", valuemap("health_status",entry["HEALTHSTATUS"]))],
                            "labels": labels
                            }
        metrics.append(metric_dict)
        metric_dict = {
                            "key": "huawei_storage_component_running_status",
                            "value": entry["RUNNINGSTATUS"],
                            "customlabels": [("status_text", valuemap("running_status",entry["RUNNINGSTATUS"]))],
                            "labels": labels
                            }
        metrics.append(metric_dict)

    return metrics

def get_bbu_data(connection):
    data = connection.get_data("backup_power")
    metrics = []
    for entry in data["data"]:
        labels = [
                    ("type", "bbu"),
                    ("id", entry["ID"]),
                    ("name", entry["NAME"]),
                    ("location", entry["LOCATION"]),
                    ]
        metric_dict = {
                            "key": "huawei_storage_component_health_status",
                            "value": entry["HEALTHSTATUS"],
                            "customlabels": [("status_text", valuemap("health_status",entry["HEALTHSTATUS"]))],
                            "labels": labels
                            }
        metrics.append(metric_dict)
        metric_dict = {
                            "key": "huawei_storage_component_running_status",
                            "value": entry["RUNNINGSTATUS"],
                            "customlabels": [("status_text", valuemap("running_status",entry["RUNNINGSTATUS"]))],
                            "labels": labels
                            }
        metrics.append(metric_dict)
        metric_dict = {
                            "key": "huawei_storage_remainlife",
                            "value": entry["REMAINLIFEDAYS"],
                            "customlabels": [],
                            "labels": labels
                            }
        metrics.append(metric_dict)
    return metrics

def get_enclosure_data(connection):
    data = connection.get_data("enclosure")
    metrics = []
    for entry in data["data"]:
        labels = [
                    ("type", "enclosure"),
                    ("serial", entry["SERIALNUM"]),
                    ("id", entry["ID"]),
                    ("name", entry["NAME"]),
                    ("model", entry["MODEL"]),
                    ]
        metric_dict = {
                            "key": "huawei_storage_component_health_status",
                            "value": entry["HEALTHSTATUS"],
                            "customlabels": [("status_text", valuemap("health_status",entry["HEALTHSTATUS"]))],
                            "labels": labels
                            }
        metrics.append(metric_dict)
        metric_dict = {
                            "key": "huawei_storage_component_running_status",
                            "value": entry["RUNNINGSTATUS"],
                            "customlabels": [("status_text", valuemap("running_status",entry["RUNNINGSTATUS"]))],
                            "labels": labels
                            }
        metrics.append(metric_dict)
        metric_dict = {
                            "key": "huawei_storage_component_temperature",
                            "value": entry["TEMPERATURE"],
                            "customlabels": [],
                            "labels": labels
                            }
        metrics.append(metric_dict)
    return metrics

def get_intf_module_data(connection):
    data = connection.get_data("intf_module")
    metrics = []
    for entry in data["data"]:
        labels = [
                    ("type", "intf_module"),
                    ("id", entry["ID"]),
                    ("name", entry["NAME"]),
                    ("model", entry["MODEL"]),
                    ("location", entry["LOCATION"])
                    ]
        metric_dict = {
                            "key": "huawei_storage_component_health_status",
                            "value": entry["HEALTHSTATUS"],
                            "customlabels": [("status_text", valuemap("health_status",entry["HEALTHSTATUS"]))],
                            "labels": labels
                            }
        metrics.append(metric_dict)
        metric_dict = {
                            "key": "huawei_storage_component_running_status",
                            "value": entry["RUNNINGSTATUS"],
                            "customlabels": [("status_text", valuemap("running_status",entry["RUNNINGSTATUS"]))],
                            "labels": labels
                            }
        metrics.append(metric_dict)
    return metrics

def get_fan_data(connection):
    data = connection.get_data("fan")
    metrics = []
    for entry in data["data"]:
        labels = [
                    ("type", "fan"),
                    ("id", entry["ID"]),
                    ("name", entry["NAME"]),
                    ("location", entry["LOCATION"])
                    ]
        metric_dict = {
                            "key": "huawei_storage_component_health_status",
                            "value": entry["HEALTHSTATUS"],
                            "customlabels": [("status_text", valuemap("health_status",entry["HEALTHSTATUS"]))],
                            "labels": labels
                            }
        metrics.append(metric_dict)
        metric_dict = {
                            "key": "huawei_storage_component_running_status",
                            "value": entry["RUNNINGSTATUS"],
                            "customlabels": [("status_text", valuemap("running_status",entry["RUNNINGSTATUS"]))],
                            "labels": labels
                            }
        metrics.append(metric_dict)
    return metrics

def get_disk_data(connection):
    data = connection.get_data("disk")
    metrics = []
    for entry in data["data"]:
        labels = [
                    ("type", "disk"),
                    ("serial", entry["SERIALNUMBER"]),
                    ("barcode", entry["barcode"]),
                    ("model", entry["MODEL"]),
                    ("location", entry["LOCATION"]),
                    ("id", entry["ID"])
                    ]
        metric_dict = {
                            "key": "huawei_storage_component_health_status",
                            "value": entry["HEALTHSTATUS"],
                            "customlabels": [("status_text", valuemap("health_status",entry["HEALTHSTATUS"]))],
                            "labels": labels
                            }
        metrics.append(metric_dict)
        metric_dict = {
                            "key": "huawei_storage_component_running_status",
                            "value": entry["RUNNINGSTATUS"],
                            "customlabels": [("status_text", valuemap("running_status",entry["RUNNINGSTATUS"]))],
                            "labels": labels
                            }
        metrics.append(metric_dict)
        metric_dict = {
                            "key": "huawei_storage_component_temperature",
                            "value": entry["TEMPERATURE"],
                            "customlabels": [],
                            "labels": labels
                            }
        metrics.append(metric_dict)
        metric_dict = {
                            "key": "huawei_storage_remainlife",
                            "value": entry["REMAINLIFE"],
                            "customlabels": [],
                            "labels": labels
                            }
        metrics.append(metric_dict)
        metric_dict = {
                            "key": "huawei_storage_usage",
                            "value": entry["CAPACITYUSAGE"],
                            "customlabels": [],
                            "labels": labels
                            }
        metrics.append(metric_dict)
        stats_uid = "{0}:{1}".format(entry["TYPE"], entry["ID"])
        perfmetrics_list = [
                            "read_iops",
                            "read_mbytes",
                            "write_iops",
                            "write_mbytes",
                            "avg_read_latency",
                            "avg_write_latency",
                            "queue_length"
                            ]
        data_ids = []
        for metric in perfmetrics_list:
            data_ids.append(valuemap("data_ids",metric))
        data = (connection.get_perf_data(stats_uid,",".join(data_ids)))
        perf_data = data['data'][0]['CMO_STATISTIC_DATA_LIST'].split(',')
        for metric in perfmetrics_list:
            metric_dict = {
                "key": f"huawei_storage_metrics_{metric}",
                "value": f"{perf_data.pop(0)}",
                "customlabels": [],
                "labels": labels
            }
            metrics.append(metric_dict)
    return metrics

def get_eth_port_data(connection):
    data = connection.get_data("eth_port")
    metrics = []
    for entry in data["data"]:
        labels = [
                    ("type", "eth_port"),
                    ("id", entry["ID"]),
                    ("name", entry["NAME"]),
                    ("mac", entry["MACADDRESS"]),
                    ("ipv4", entry["IPV4ADDR"]),
                    ("v4mask", entry["IPV4MASK"]),
                    ("location", entry["LOCATION"]),
                    ("port_type_id", entry["LOGICTYPE"]),
                    ("port_type_text", valuemap("eth_port_types",entry["LOGICTYPE"])),
                    ]
        metric_dict = {
                            "key": "huawei_storage_component_health_status",
                            "value": entry["HEALTHSTATUS"],
                            "customlabels": [("status_text", valuemap("health_status",entry["HEALTHSTATUS"]))],
                            "labels": labels
                            }
        metrics.append(metric_dict)
        metric_dict = {
                            "key": "huawei_storage_component_running_status",
                            "value": entry["RUNNINGSTATUS"],
                            "customlabels": [("status_text", valuemap("running_status",entry["RUNNINGSTATUS"]))],
                            "labels": labels
                            }
        metrics.append(metric_dict)
        metric_dict = {
                            "key": "huawei_storage_port_errors",
                            "value": entry["crcErrors"],
                            "customlabels": [("error_type", "crc"), ("port_type", "eth")],
                            "labels": labels
                            }
        metrics.append(metric_dict)
        metric_dict = {
                            "key": "huawei_storage_port_errors",
                            "value": entry["frameErrors"],
                            "customlabels": [("error_type", "frame"), ("port_type", "eth")],
                            "labels": labels
                            }
        metrics.append(metric_dict)
        metric_dict = {
                            "key": "huawei_storage_port_errors",
                            "value": entry["frameLengthErrors"],
                            "customlabels": [("error_type", "frame_length"), ("port_type", "eth")],
                            "labels": labels
                            }
        metrics.append(metric_dict)
        if entry["LOGICTYPE"] == "0":
            # Here we have management and host ports. Management ports don't have metrics, so we don't query them for not "0" port type
            stats_uid = "{0}:{1}".format(entry["TYPE"], entry["ID"])
            perfmetrics_list = [
                                "usage",
                                "queue_length",
                                "read_iops",
                                "read_mbytes",
                                "write_iops",
                                "write_mbytes",
                                "max_latency",
                                "avg_read_latency",
                                "avg_write_latency",
                                # "avg_cpu_usage",
                                # "avg_cache_usage",
                                # "read_cache_hits",
                                # "write_cache_hits",
                                # "read_cache_usage",
                                # "write_cache_usage",
                                # "cache_page_usage",
                                # "cache_chunk_usage",
                                # "max_read_kbytes",
                                # "max_write_kbytes",
                                # "failed_reads",
                                # "failed_writes",

                                ]
             
            data_ids = []
            for metric in perfmetrics_list:
                data_ids.append(valuemap("data_ids",metric))
            data = (connection.get_perf_data(stats_uid,",".join(data_ids)))
            perf_data = data['data'][0]['CMO_STATISTIC_DATA_LIST'].split(',')
            for metric in perfmetrics_list:
                metric_dict = {
                    "key": f"huawei_storage_metrics_{metric}",
                    "value": f"{perf_data.pop(0)}",
                    "customlabels": [],
                    "labels": labels
                }
                metrics.append(metric_dict)
    return metrics

def get_sas_port_data(connection):
    # I don't have them connected, so this function may contain bugs. Check out
    data = connection.get_data("sas_port")
    metrics = []
    for entry in data["data"]:
        labels = [
                    ("type", "sas_port"),
                    ("id", entry["ID"]),
                    ("name", entry["NAME"]),
                    ("location", entry["LOCATION"]),
                    ]
        metric_dict = {
                            "key": "huawei_storage_component_health_status",
                            "value": entry["HEALTHSTATUS"],
                            "customlabels": [("status_text", valuemap("health_status",entry["HEALTHSTATUS"]))],
                            "labels": labels
                            }
        metrics.append(metric_dict)
        metric_dict = {
                            "key": "huawei_storage_component_running_status",
                            "value": entry["RUNNINGSTATUS"],
                            "customlabels": [("status_text", valuemap("running_status",entry["RUNNINGSTATUS"]))],
                            "labels": labels
                            }
        metrics.append(metric_dict)
        metric_dict = {
                            "key": "huawei_storage_port_errors",
                            "value": entry["DISPARITYERROR"],
                            "customlabels": [("error_type", "disparity"), ("port_type", "sas")],
                            "labels": labels
                            }
        metrics.append(metric_dict)
        metric_dict = {
                            "key": "huawei_storage_port_errors",
                            "value": entry["PHYRESETERRORS"],
                            "customlabels": [("error_type", "phy_reset"), ("port_type", "sas")],
                            "labels": labels
                            }
        metrics.append(metric_dict)
        stats_uid = "{0}:{1}".format(entry["TYPE"], entry["ID"])
        perfmetrics_list = [
                            # "usage",
                            # "queue_length",
                            "read_iops",
                            "read_mbytes",
                            "write_iops",
                            "write_mbytes",
                            "max_latency",
                            "max_read_latency",
                            "max_write_latency",
                            "avg_read_latency",
                            "avg_write_latency",
                            # "avg_cpu_usage",
                            # "avg_cache_usage",
                            # "read_cache_hits",
                            # "write_cache_hits",
                            # "read_cache_usage",
                            # "write_cache_usage",
                            # "cache_page_usage",
                            # "cache_chunk_usage",
                            # "max_read_kbytes",
                            # "max_write_kbytes",
                            # "failed_reads",
                            # "failed_writes",

                            ]
        data_ids = []
        for metric in perfmetrics_list:
            data_ids.append(valuemap("data_ids",metric))
        data = (connection.get_perf_data(stats_uid,",".join(data_ids)))
        perf_data = data['data'][0]['CMO_STATISTIC_DATA_LIST'].split(',')
        for metric in perfmetrics_list:
            metric_dict = {
                "key": f"huawei_storage_metrics_{metric}",
                "value": f"{perf_data.pop(0)}",
                "customlabels": [],
                "labels": labels
            }
            metrics.append(metric_dict)
    return metrics

def get_lun_data(connection):
    data = connection.get_data("lun")
    metrics = []
    for entry in data["data"]:
        labels = [
                    ("type", "lun"),
                    ("id", entry["ID"]),
                    ("name", entry["NAME"]),
                    ("wwn", entry["WWN"])
                    ]
        metric_dict = {
                            "key": "huawei_storage_component_health_status",
                            "value": entry["HEALTHSTATUS"],
                            "customlabels": [("status_text", valuemap("health_status",entry["HEALTHSTATUS"]))],
                            "labels": labels
                            }
        metrics.append(metric_dict)
        metric_dict = {
                            "key": "huawei_storage_component_running_status",
                            "value": entry["RUNNINGSTATUS"],
                            "customlabels": [("status_text", valuemap("running_status",entry["RUNNINGSTATUS"]))],
                            "labels": labels
                            }
        metrics.append(metric_dict)
        metric_dict = {
                            "key": "huawei_storage_capacity_total",
                            "value": entry["CAPACITY"],
                            "customlabels": [],
                            "labels": labels
                            }
        metrics.append(metric_dict)
        metric_dict = {
                            "key": "huawei_storage_capacity_allocated",
                            "value": entry["ALLOCCAPACITY"],
                            "customlabels": [],
                            "labels": labels
                            }
        metrics.append(metric_dict)

        stats_uid = "{0}:{1}".format(entry["TYPE"], entry["ID"])
        perfmetrics_list = [
                            # "usage",
                            "queue_length",
                            "read_iops",
                            "read_mbytes",
                            "write_iops",
                            "write_mbytes",
                            "max_latency",
                            # "max_read_latency",
                            # "max_write_latency",
                            "avg_read_latency",
                            "avg_write_latency",
                            # "avg_cpu_usage",
                            # "avg_cache_usage",
                            "read_cache_hits",
                            "write_cache_hits",
                            # "read_cache_usage",
                            # "write_cache_usage",
                            # "cache_page_usage",
                            # "cache_chunk_usage",
                            # "max_read_kbytes",
                            # "max_write_kbytes",
                            # "failed_reads",
                            # "failed_writes",

                            ]
        data_ids = []
        for metric in perfmetrics_list:
            data_ids.append(valuemap("data_ids",metric))
        data = (connection.get_perf_data(stats_uid,",".join(data_ids)))
        perf_data = data['data'][0]['CMO_STATISTIC_DATA_LIST'].split(',')
        for metric in perfmetrics_list:
            metric_dict = {
                "key": f"huawei_storage_metrics_{metric}",
                "value": f"{perf_data.pop(0)}",
                "customlabels": [],
                "labels": labels
            }
            metrics.append(metric_dict)
    return metrics

def get_disk_pool_data(connection):
    data = connection.get_data("diskpool")
    metrics = []
    for entry in data["data"]:
        labels = [
                    ("type", "disk_pool"),
                    ("id", entry["ID"]),
                    ("name", entry["NAME"]),
                    ]
        metric_dict = {
                            "key": "huawei_storage_component_health_status",
                            "value": entry["HEALTHSTATUS"],
                            "customlabels": [("status_text", valuemap("health_status",entry["HEALTHSTATUS"]))],
                            "labels": labels
                            }
        metrics.append(metric_dict)
        metric_dict = {
                            "key": "huawei_storage_component_running_status",
                            "value": entry["RUNNINGSTATUS"],
                            "customlabels": [("status_text", valuemap("running_status",entry["RUNNINGSTATUS"]))],
                            "labels": labels
                            }
        metrics.append(metric_dict)
        metric_dict = {
                            "key": "huawei_storage_capacity_total",
                            "value": entry["TOTALCAPACITY"],
                            "customlabels": [],
                            "labels": labels
                            }
        metrics.append(metric_dict)
        metric_dict = {
                            "key": "huawei_storage_capacity_allocated",
                            "value": entry["USEDCAPACITY"],
                            "customlabels": [],
                            "labels": labels
                            }
        metrics.append(metric_dict)
        metric_dict = {
                            "key": "huawei_storage_remainlife",
                            "value": entry["remainLife"],
                            "customlabels": [],
                            "labels": labels
                            }
        metrics.append(metric_dict)

        stats_uid = "{0}:{1}".format(entry["TYPE"], entry["ID"])
        perfmetrics_list = [
                            # "usage",
                            "queue_length",
                            "read_iops",
                            "read_mbytes",
                            "write_iops",
                            "write_mbytes",
                            "max_latency",
                            # "max_read_latency",
                            # "max_write_latency",
                            "avg_read_latency",
                            "avg_write_latency",
                            # "avg_cpu_usage",
                            # "avg_cache_usage",
                            # "read_cache_hits",
                            # "write_cache_hits",
                            # "read_cache_usage",
                            # "write_cache_usage",
                            # "cache_page_usage",
                            # "cache_chunk_usage",
                            # "max_read_kbytes",
                            # "max_write_kbytes",
                            # "failed_reads",
                            # "failed_writes",

                            ]
        data_ids = []
        for metric in perfmetrics_list:
            data_ids.append(valuemap("data_ids",metric))
        data = (connection.get_perf_data(stats_uid,",".join(data_ids)))
        perf_data = data['data'][0]['CMO_STATISTIC_DATA_LIST'].split(',')
        for metric in perfmetrics_list:
            metric_dict = {
                "key": f"huawei_storage_metrics_{metric}",
                "value": f"{perf_data.pop(0)}",
                "customlabels": [],
                "labels": labels
            }
            metrics.append(metric_dict)
    return metrics

def get_storage_pool_data(connection):
    data = connection.get_data("storagepool")
    metrics = []
    for entry in data["data"]:
        labels = [
                    ("type", "storage_pool"),
                    ("id", entry["ID"]),
                    ("name", entry["NAME"]),
                    ]
        metric_dict = {
                            "key": "huawei_storage_component_health_status",
                            "value": entry["HEALTHSTATUS"],
                            "customlabels": [("status_text", valuemap("health_status",entry["HEALTHSTATUS"]))],
                            "labels": labels
                            }
        metrics.append(metric_dict)
        metric_dict = {
                            "key": "huawei_storage_component_running_status",
                            "value": entry["RUNNINGSTATUS"],
                            "customlabels": [("status_text", valuemap("running_status",entry["RUNNINGSTATUS"]))],
                            "labels": labels
                            }
        metrics.append(metric_dict)
        metric_dict = {
                            "key": "huawei_storage_capacity_total",
                            "value": entry["USERTOTALCAPACITY"],
                            "customlabels": [],
                            "labels": labels
                            }
        metrics.append(metric_dict)
        metric_dict = {
                            "key": "huawei_storage_capacity_allocated",
                            "value": entry["USERWRITEALLOCCAPACITY"],
                            "customlabels": [],
                            "labels": labels
                            }
        metrics.append(metric_dict)
        stats_uid = "{0}:{1}".format(entry["TYPE"], entry["ID"])
        perfmetrics_list = [
                            # "usage",
                            "queue_length",
                            "read_iops",
                            "read_mbytes",
                            "write_iops",
                            "write_mbytes",
                            "max_latency",
                            # "max_read_latency",
                            # "max_write_latency",
                            "avg_read_latency",
                            "avg_write_latency",
                            # "avg_cpu_usage",
                            # "avg_cache_usage",
                            # "read_cache_hits",
                            # "write_cache_hits",
                            # "read_cache_usage",
                            # "write_cache_usage",
                            # "cache_page_usage",
                            # "cache_chunk_usage",
                            # "max_read_kbytes",
                            # "max_write_kbytes",
                            # "failed_reads",
                            # "failed_writes",

                            ]
        data_ids = []
        for metric in perfmetrics_list:
            data_ids.append(valuemap("data_ids",metric))
        data = (connection.get_perf_data(stats_uid,",".join(data_ids)))
        perf_data = data['data'][0]['CMO_STATISTIC_DATA_LIST'].split(',')
        for metric in perfmetrics_list:
            metric_dict = {
                "key": f"huawei_storage_metrics_{metric}",
                "value": f"{perf_data.pop(0)}",
                "customlabels": [],
                "labels": labels
            }
            metrics.append(metric_dict)
    return metrics

def get_controller_data(connection):
    data = connection.get_data("controller")
    metrics = []
    for entry in data["data"]:
        labels = [
                    ("type", "controller"),
                    ("id", entry["ID"]),
                    ("name", entry["NAME"]),
                    ("location", entry["LOCATION"]),
                    ]
        metric_dict = {
                            "key": "huawei_storage_component_health_status",
                            "value": entry["HEALTHSTATUS"],
                            "customlabels": [("status_text", valuemap("health_status",entry["HEALTHSTATUS"]))],
                            "labels": labels
                            }
        metrics.append(metric_dict)
        metric_dict = {
                            "key": "huawei_storage_component_running_status",
                            "value": entry["RUNNINGSTATUS"],
                            "customlabels": [("status_text", valuemap("running_status",entry["RUNNINGSTATUS"]))],
                            "labels": labels
                            }
        metrics.append(metric_dict)
        metric_dict = {
                            "key": "huawei_storage_controller_cpuusage",
                            "value": entry["CPUUSAGE"],
                            "customlabels": [],
                            "labels": labels
                            }
        metrics.append(metric_dict)
        metric_dict = {
                            "key": "huawei_storage_controller_memorysize",
                            "value": entry["MEMORYSIZE"],
                            "customlabels": [],
                            "labels": labels
                            }
        metrics.append(metric_dict)
        metric_dict = {
                            "key": "huawei_storage_controller_memoryusage",
                            "value": entry["MEMORYUSAGE"],
                            "customlabels": [],
                            "labels": labels
                            }
        metrics.append(metric_dict)
        stats_uid = "{0}:{1}".format(entry["TYPE"], entry["ID"])
        perfmetrics_list = [
                            "queue_length",
                            "read_iops",
                            "read_mbytes",
                            "write_iops",
                            "write_mbytes",
                            "max_latency",
                            "avg_read_latency",
                            "avg_write_latency",
                            "avg_cpu_usage",
                            "avg_cache_usage",
                            "read_cache_hits",
                            "write_cache_hits",
                            "read_cache_usage",
                            "write_cache_usage",
                            "cache_page_usage",
                            "cache_chunk_usage",
                            "max_read_kbytes",
                            "max_write_kbytes",
                            # "failed_reads",
                            # "failed_writes",
                            # "usage"
                            ]
        data_ids = []
        for metric in perfmetrics_list:
            data_ids.append(valuemap("data_ids",metric))
        data = (connection.get_perf_data(stats_uid,",".join(data_ids)))
        perf_data = data['data'][0]['CMO_STATISTIC_DATA_LIST'].split(',')
        for metric in perfmetrics_list:
            metric_dict = {
                "key": f"huawei_storage_metrics_{metric}",
                "value": f"{perf_data.pop(0)}",
                "customlabels": [],
                "labels": labels
            }
            metrics.append(metric_dict)
    return metrics

#
# --- MAIN ---
#
def main(args):
    if args.config:
        conf = configargs(args)
    if not conf or not args.target in conf:
        logger.critical(f"No username / password found for target {args.target}")
        sys.exit(3)
    else:
        user = conf[args.target]['user']
        password = conf[args.target]['password']
        port = conf[args.target]['port']
        modules = conf[args.target]['modules'].split(',')
    stime = int(time.time() * 1000)
    Storage = OceanStor(args.target,port,user,password, 10)
    Storage.login()

    for module in  modules:
        logger.debug(f"fetch metrics for {module}")
        try:
            metrics = globals()[module](Storage)
        except:
            logger.error(f"mode {module} not found")
            Storage.logout()
            sys.exit(3)
        text_out = "" 
        for metric in metrics:
            text_out += metric["key"] + '{'
            for label in metric["labels"]:
                text_out += label[0] + '="' + label[1] + '",'
            for label in metric["customlabels"]:
                text_out += label[0] + '="' + label[1] + '",'
            text_out = text_out[:-1] + '} ' + metric["value"] + '\n'
        print(text_out)
    rtime = int(time.time() * 1000) - stime
    print(f'huawei_storage_exporter_duration{{version="{__VERSION__}"}} {rtime}')    
    logger.info(f"fine")

    Storage.logout()

if __name__ == "__main__":
    args = arguments()

    logger = logging.getLogger(__name__)
    logger.setLevel(severity(args.verbose))

    logformat = logging.Formatter(f"%(asctime)s %(levelname)s\t{args.target}\t%(message)s")

    streamHandler = logging.StreamHandler(getattr(sys, args.pipe ))
    streamHandler.setFormatter(logformat)

    requests_log = logging.getLogger('requests')
    if severity(args.verbose) != "DEBUG":
        requests_log.setLevel('WARNING')
    else:
        requests_log.setLevel(severity(args.verbose))

    logger.addHandler(streamHandler) 
    requests_log.addHandler(streamHandler)

    main(args)

