node_num = 1
lat_list = []
long_list = []
process_capacity_list = []
address_list = []
role_list = []
node_id_list = []
bandwidth_list = []
position_upload_time = 0
queue_time_upload = 0
prediction_time = 0
import logging
from logging import handlers
import numpy as np

class Logger(object):
    level_relations = {
        'debug':logging.DEBUG,
        'info':logging.INFO,
        'warning':logging.WARNING,
        'error':logging.ERROR,
        'crit':logging.CRITICAL
    }
    def __init__(self,filename,level='info',when='D',backCount=3,fmt='%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s'):
        self.logger = logging.getLogger(filename)
        format_str = logging.Formatter(fmt)
        self.logger.setLevel(self.level_relations.get(level))
        sh = logging.StreamHandler()
        sh.setFormatter(format_str) 
        th = handlers.RotatingFileHandler(filename=filename,mode='w',backupCount=backCount,encoding='utf-8')
        th.setFormatter(format_str)
        self.logger.addHandler(sh) 
        self.logger.addHandler(th)

class Task:
    def __init__(self,start_time,object_t,deadline,task_id,vehicle_src_id,data_size,result_size,workload,chunk_num,chunk_index=None,local_server_id=None):
        self.start_time = start_time
        self.object = object_t
        self.deadline = deadline 
        self.task_id = task_id
        self.type = None
        self.data_size = data_size
        self.vehicle_src_id = vehicle_src_id
        self.local_server_id = local_server_id
        self.result_size = result_size
        self.workload = workload
        self.chunk_num = chunk_num
        self.chunk_index = chunk_index
        self.end_time = -1
        self.dst_node_id = None
        self.slack_time = -1
        self.result = None
    def SetLocalServetrId(self,local_server_id):
        self.local_server_id = local_server_id
    def SetStartTime(self,start_time):
        self.start_time = start_time
    def SetDestination(self,dst_node_id):
        self.dst_node_id = dst_node_id
    def SetSlackTime(self,slack_time):
        self.slack_time = slack_time
    def SetResult(self,result):
        self.result = result
    def SetEndTime(self,end_time):
        self.end_time = end_time
    def ReturnObject(self):
        return self.object
    def SetWorkload(self,workload):
        self.workload = workload

class Node:
    def __init__(self,lat_pos,long_pos,process_capacity,node_id,address,role,bandwidth):
        self.lat = lat_pos
        self.long = long_pos
        self.process_capacity = process_capacity
        self.node_id = node_id
        self.address = address
        self.role = role
        self.bandwidth = bandwidth
        self.queue_time = -1
    def UpdateQueueTime(self,queue_time):
        self.queue_time = queue_time
    def ChangePosition(self,new_lat,new_long):
        self.lat = new_lat
        self.long = new_long

class Object:
    def __init__(self,data,object_id):
        self.data = data
        self.object_id = object_id
        self.chunk_list = list()
        self.pointer = 0 
    def CutDataIntoChunk(self,chunk_num):
        newarr = np.array_split(self.data,chunk_num)
        for i in range(len(newarr)):
            self.chunk_list.append(newarr[i])
    def InsertChunkIntoData(self,chunk):
        self.chunk_list.append(chunk) 
        self.data = np.concatenate(self.chunk_list,axis=0)
    def AddChunkPointer(self):
        self.pointer = self.pointer + 1