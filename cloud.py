# -*-coding:utf-8 -*-

from concurrent import futures
import logging
from logging import handlers

import grpc
import helloworld_pb2
import helloworld_pb2_grpc
import cluster_config as config
import time,threading
import numpy as np
import math
from io import BytesIO
import uuid
import queue,os
import copy,csv
import getopt,sys
from cluster_config import Task,Node,Object


nodes_ = dict()
local_node_id_ = 0
local_role_ = "Cloud"
average_server_queue_time = 0 
server_queue_time = dict()
current_lat = dict()
current_long = dict()
sojourn_time = dict()   
current_server_id = dict()
next_server_id = dict()
trace_index = dict()
large_task_dict = dict()
    
class NodeManager:
    def InitCluster(self,node_num,lat_list,long_list,process_capacity_list,address_list,role_list,node_id_list,bandwidth_list):
        for i in range(node_num):
            node_id = node_id_list[i]
            global local_node_id_
            if role_list[i] == local_role_:
                local_node_id_ = node_id
                server_queue_time[local_node_id_] = 0
            node = Node(lat_list[i],long_list[i],process_capacity_list[i],node_id,address_list[i],role_list[i],bandwidth_list[i])
            nodes_[node_id] = node

class VehicleManager:
    def __init__(self,dir_trace):
        self.dir_trace = dir_trace
        self.vehicle_info = [list() for i in range(6)]
    def ReadTrace(self):
        with open(self.dir_trace, 'r') as f:
            reader = csv.reader(f, delimiter=',')
            for row in reader:
                for i in range(len(self.vehicle_info)):
                    self.vehicle_info[i].append(row[i])
    def PredictTrajectory(self):
        while True:
            for id,index in list(trace_index.items()):
                global sojourn_time,next_server_id,current_server_id
                sojourn_time[id] = round(float(self.vehicle_info[4][index]),1)
                for node in nodes_.values():
                    if(node.role == self.vehicle_info[3][index]):
                        current_server_id[id] = node.node_id
                    if(node.role == self.vehicle_info[5][index]):
                        next_server_id[id] = node.node_id

class Greeter(helloworld_pb2_grpc.GreeterServicer):
    def VehicleReport(self,request,context):
        global current_lat,current_long,trace_index
        current_lat[request.node_id] = request.current_lat
        current_long[request.node_id] = request.current_long
        trace_index[request.node_id] = request.trace_index
        return helloworld_pb2.VehicleReportReply(average_queue_time=average_server_queue_time,sojourn_time=sojourn_time[request.node_id])
    def ServerReport(self,request,context):
        global server_queue_time,average_server_queue_time
        server_queue_time[request.node_id] = request.local_queue_time
        if len(server_queue_time) > 0:
            average_server_queue_time = sum(list(server_queue_time.values()))/(len(server_queue_time))
        def dict2bytes(input_dict):
            input_list = list(input_dict.items()) 
            input_array = np.array(input_list)
            byte_data = BytesIO()
            np.save(byte_data,input_array , allow_pickle=True)
            byte_data = byte_data.getvalue()
            return byte_data
        return helloworld_pb2.ServerReportReply(queue_time_list=dict2bytes(server_queue_time),current_server_id=dict2bytes(current_server_id),sojourn_time=dict2bytes(sojourn_time),next_server_id=dict2bytes(next_server_id),vehicle_lat=dict2bytes(current_lat),vehicle_long=dict2bytes(current_long))
    def RealTimeInfo(self,request,context):
        return helloworld_pb2.InfoReply(local_queue_time=float(server_queue_time[local_node_id_]))
    def UploadAndRequestInfo(self,request,context):
        if request.type == "upload":
            node_id = request.node_id
            large_task_dict[request.task_id] = node_id
        else:  
            if request.task_id in large_task_dict.keys():
                node_id = large_task_dict[request.task_id]
            else:
                node_id = "empty"
        return helloworld_pb2.TaskReply(node_id=node_id)
    def GetFutureServer(self,request,context):
        vehicle_trace = trace_index[request.vehicle_id]
        diff_trace = int(request.remain_time * 10)
        future_server = vehicle_manager_.vehicle_info[5][vehicle_trace+diff_trace]
        return helloworld_pb2.FutureServerReply(future_server=future_server)


def StartServer():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    helloworld_pb2_grpc.add_GreeterServicer_to_server(Greeter(), server)
    local_address = nodes_[local_node_id_].address
    server.add_insecure_port(local_address)
    server.start()
    server.wait_for_termination()


if __name__ == '__main__':
    log = config.Logger('cloud.log',level='debug')
    node_manager_ = NodeManager()
    vehicle_manager_ = VehicleManager("vehicle_info_trace.csv")
    vehicle_manager_.ReadTrace()
    node_manager_.InitCluster(config.node_num,config.lat_list,config.long_list,config.process_capacity_list,config.address_list,config.role_list,config.node_id_list,config.bandwidth_list)
    receive_info = threading.Thread(target=StartServer,args=(),name="ReceiveTaskAndResult")
    vehicle_predictor_t = threading.Thread(target=vehicle_manager_.PredictTrajectory,args=(),name="VehiclePredictor")

    receive_info.start()
    vehicle_predictor_t.start()

    receive_info.join()
    vehicle_predictor_t.join()