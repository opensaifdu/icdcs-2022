# -*-coding:utf-8 -*-

from concurrent import futures
from concurrent.futures import process
import logging
from logging import handlers
from google.protobuf import message

import grpc
import helloworld_pb2
import helloworld_pb2_grpc
import cluster_config as config
import time,threading
import numpy as np
import math
from io import BytesIO
import uuid
import queue
import copy
import getopt,sys,random
from cluster_config import Task,Node,Object


all_task_dict = dict()  
task_queue = queue.Queue()  
local_execution_queue = dict()
result_sender_queue = queue.Queue()
nodes_ = dict()
local_node_id_ = 0
local_role_ = "RSU-1"
local_queue_time_ = 0  
vehicle_lat = dict()
vehicle_long = dict()
sojourn_time = dict()   
current_server_id = dict()
next_server_id = dict()

def CalDataTransferTime(node,data_size):
    if "Vehicle" in node.role:
        wireless_rate = 10.0
        delay = data_size/wireless_rate
    else:
        transfer_rate = node.bandwidth
        delay = data_size/transfer_rate
    return delay
    
class NodeManager:
    def InitCluster(self,node_num,lat_list,long_list,process_time_list,address_list,role_list,node_id_list,bandwidth_list):
        for i in range(node_num):
            node_id = node_id_list[i]
            global local_node_id_
            if role_list[i] == local_role_:
                local_node_id_ = node_id
            node = Node(lat_list[i],long_list[i],process_time_list[i],node_id,address_list[i],role_list[i],bandwidth_list[i])
            nodes_[node_id] = node
            if(local_node_id_ != 0):
                nodes_[local_node_id_].UpdateQueueTime(0)

class ObjectManager:
    def UploadQueueTimeAndGetVehicleInfo(self): 
        while True:
            start_time = time.time()
            while time.time() - start_time < config.queue_time_upload:
                continue
            for node in nodes_.values():
                if node.role == "Cloud":
                    cloud_address = node.address
                    break
            try:
                with grpc.insecure_channel(cloud_address) as channel:
                    stub = helloworld_pb2_grpc.GreeterStub(channel)
                    PushRequest_t = helloworld_pb2.ServerReportRequest(node_id=local_node_id_,local_queue_time=local_queue_time_)
                    response = stub.ServerReport(PushRequest_t)
                    def bytes2array(input_bytes):
                        byte_list = BytesIO(input_bytes)
                        input_array = np.load(byte_list,allow_pickle=True)
                        return input_array
                    global sojourn_time,vehicle_lat,vehicle_long,current_server_id,next_server_id
                    array_queue_time = bytes2array(response.queue_time_list)
                    for i in range(len(array_queue_time)):
                        nodes_[array_queue_time[i][0]].UpdateQueueTime(float(array_queue_time[i][1]))
                    array_sojourn = bytes2array(response.sojourn_time)
                    array_lat = bytes2array(response.vehicle_lat)
                    array_long = bytes2array(response.vehicle_long)
                    array_current = bytes2array(response.current_server_id)
                    array_next = bytes2array(response.next_server_id)
                    for i in range(len(array_long)):
                        sojourn_time[array_sojourn[i][0]] = float(array_sojourn[i][1])
                        vehicle_lat[array_lat[i][0]] = float(array_lat[i][1])
                        vehicle_long[array_long[i][0]] = float(array_long[i][1])
                        current_server_id[array_current[i][0]] = array_current[i][1]
                        next_server_id[array_next[i][0]] = array_next[i][1]
            except:
                time.sleep(0.1)
    def SchedulingSendResult(self,result_sender_queue):
        while True:
            task_send = result_sender_queue.get()
            self.PushResult(task_send)
    def PushResult(self,task):
        if local_node_id_ == current_server_id[task.vehicle_src_id]:
            dst_address = nodes_[task.vehicle_src_id].address
        else:
            result_time = CalDataTransferTime(nodes_[current_server_id[task.vehicle_src_id]],task.result_size) + CalDataTransferTime(nodes_[task.vehicle_src_id],task.result_size)
            if sojourn_time[task.vehicle_src_id] < result_time:
                if local_role_ == nodes_[next_server_id[task.vehicle_src_id]]:
                    result_sender_queue.put(task)
                    return
                else:
                    dst_address = nodes_[next_server_id[task.vehicle_src_id]].address
            else:
                dst_address = nodes_[current_server_id[task.vehicle_src_id]].address
        try:
            with grpc.insecure_channel(dst_address) as channel:
                stub = helloworld_pb2_grpc.GreeterStub(channel)
                byte_result = BytesIO()
                np.save(byte_result,task.result,allow_pickle=True)
                byte_result = byte_result.getvalue()
                PushResultRequest_t = helloworld_pb2.PushResultRequest(task_id=task.task_id,vehicle_src_id=task.vehicle_src_id,result=byte_result,local_server_id=task.local_server_id)
                response = stub.PushResult(PushResultRequest_t)
        except:
            log.logger.error("%s unavailable now,keep waiting!",dst_address)
    def SchedulingSendTasks(self,task_sender_queue):
        while True:
            task_send = task_sender_queue.get()
            current_chunk = task_send.object.pointer
            if current_chunk < len(task_send.object.chunk_list):
                dst_address = nodes_[task_send.dst_node_id].address
                self.PushObject(task_send,current_chunk,dst_address)
                task_send.ReturnObject().AddChunkPointer()
    def PushObject(self,task,chunk_index,dst_address):
        try:
            with grpc.insecure_channel(dst_address) as channel:
                stub = helloworld_pb2_grpc.GreeterStub(channel)
                chunk_num = task.chunk_num
                object_t = task.object
                byte_data = BytesIO()
                np.save(byte_data,object_t.chunk_list[chunk_index] , allow_pickle=True)
                byte_data = byte_data.getvalue()
                PushRequest_t = helloworld_pb2.PushObjRequest(task_id=task.task_id,object_id=object_t.object_id,node_id=local_node_id_,chunk_index=chunk_index,chunk_num=chunk_num,data=byte_data,deadline=task.deadline,start_time=task.start_time,data_size=task.data_size,result_size=task.result_size,workload=task.workload,vehicle_src_id=task.vehicle_src_id) 
                response = stub.PushObject(PushRequest_t)
        except:
            log.logger.error("%s unavailable now,keep waiting!",dst_address)

class TaskManager:
    def __init__(self):
        self.local_execution_queue = local_execution_queue
    def DispatchTasks(self,task_sender_queue):
        while True:
            task = task_queue.get()
            best_node_id = self.GetBestNode(task)
            task.SetDestination(best_node_id)
            if best_node_id == local_node_id_:
                self.local_execution_queue[task.task_id] = task
                global local_queue_time_
                local_queue_time_ += task.workload/nodes_[local_node_id_].process_capacity
            else:
                for _ in range(len(task.object.chunk_list)):
                    task_sender_queue.put(task)
    def ScheduleLocalTasks(self,result_sender_queue):
        while True:
            if len(self.local_execution_queue) == 0:
                continue
            ddl_task_dict = dict()
            exe_task_dict = dict() 
            slack_time_dict = dict()
            for task in list(self.local_execution_queue.values()):
                if task.chunk_num > len(task.object.chunk_list) or task.end_time != -1:
                    continue
                ddl_task_dict[task.task_id] = task.deadline + task.start_time 
                process_time = task.workload/nodes_[local_node_id_].process_capacity
                exe_task_dict[task.task_id] = process_time
            sort_list = sorted(ddl_task_dict.items(), key = lambda kv:(kv[1], kv[0]))
            sum_process_time = 0
            for i in range(len(sort_list)):
                task_id = sort_list[i][0]
                task = self.local_execution_queue[task_id]
                process_time = task.workload/nodes_[local_node_id_].process_capacity
                sum_process_time += process_time
                completion_time = sum_process_time + CalDataTransferTime(nodes_[current_server_id[task.vehicle_src_id]],task.result_size) + CalDataTransferTime(nodes_[task.vehicle_src_id],task.result_size)
                slack_time_dict[task.task_id] = task.deadline - completion_time - (time.time()-task.start_time)
            if len(ddl_task_dict.keys()) > 0:
                min_ddl_id = min(ddl_task_dict,key=ddl_task_dict.get)
                min_exe_id = min(exe_task_dict,key=exe_task_dict.get)
                min_exe_time = exe_task_dict[min_exe_id]
                flag_slack = True
                for slack in slack_time_dict.values():
                    if slack <= min_exe_time:
                        flag_slack = False
                        break
                if flag_slack:
                    task_execute = self.local_execution_queue[min_exe_id]
                else:
                    task_execute = self.local_execution_queue[min_ddl_id]
                result = self.ExecuteTask(task_execute)
                global local_queue_time_
                local_queue_time_ -= task_execute.workload/nodes_[local_node_id_].process_capacity
                task_execute.SetResult(result)
                task_execute.SetEndTime(time.time())
                self.RecordTaskFinishEvent(task_execute)
                result_sender_queue.put(task_execute)
                a = self.local_execution_queue.pop(task_execute.task_id)
    def ExecuteTask(self,task):
        data_array = np.concatenate(task.object.chunk_list,axis=0)
        for i in range(len(data_array)):
            result = result + data_array[i]
        result = np.array([len(data_array),result])
        time.sleep(task.workload/nodes_[local_node_id_].process_capacity)
        return result
    def RecordTaskFinishEvent(self,task):
        completion_time = task.end_time - task.start_time
        item_list = [task.task_id,task.deadline,completion_time]
        log.logger.error("%s, %s",local_role_,item_list)
    def GetBestNode(self,task):
        if task.chunk_num == 1:
            time_cost_dict = dict()
            queue_time_dict = dict()
            for node in nodes_.values():
                if "Vehicle" in node.role or node.queue_time == -1 or node.role == "Cloud": 
                    continue
                if node.role == local_role_:
                    time_cost = local_queue_time_ + task.workload/node.process_capacity 
                    queue_time_dict[node.node_id] = local_queue_time_
                else:
                    time_cost = task.data_size/nodes_[local_node_id_].bandwidth + task.workload/node.process_capacity + CalDataTransferTime(node,task.result_size) + node.queue_time
                    queue_time_dict[node.node_id] = node.queue_time
                time_cost_dict[node.node_id] = time_cost
            candidate_node = self.select_candinate(time_cost_dict,2)
            new_cost_dict = dict()
            for i in range(len(candidate_node)):
                if candidate_node[i] == local_node_id_:
                    new_cost_dict[local_node_id_] = time_cost_dict[candidate_node[i]] + local_queue_time_ - queue_time_dict[candidate_node[i]]
                else:
                    try:
                        node_address = candidate_node[i].address
                        with grpc.insecure_channel(node_address) as channel:
                            stub = helloworld_pb2_grpc.GreeterStub(channel)
                            TaskRequest_t = helloworld_pb2.InfoRequest(message="request")
                            response = stub.RealTimeInfo(TaskRequest_t)
                            real_queue_time = response.local_queue_time
                    except:
                        time.sleep(0.1)
                    new_cost_dict[candidate_node[i]] = time_cost_dict[candidate_node[i]] + real_queue_time - queue_time_dict[candidate_node[i]]
            sort_list = sorted(new_cost_dict.items(), key = lambda kv:(kv[1], kv[0]))
            return sort_list[0][0]
        elif task.chunk_num > 1 and task.chunk_index == 0:
            remain_send_time = CalDataTransferTime(nodes_[task.vehicle_src_id],task.data_size-3.0)*2
            cloud_address = config.address_list[2]
            try:
                with grpc.insecure_channel(cloud_address) as channel:
                    stub = helloworld_pb2_grpc.GreeterStub(channel)
                    FutureRequest_t = helloworld_pb2.FutureServerRequest(vehicle_id=task.vehicle_src_id,remain_time=float(remain_send_time))
                    response = stub.GetFutureServer(FutureRequest_t)
                    node_id = response.future_server
            except:
                time.sleep(0.1)
            try:
                with grpc.insecure_channel(cloud_address) as channel:
                    stub = helloworld_pb2_grpc.GreeterStub(channel)
                    TaskRequest_t = helloworld_pb2.TaskRequest(type="upload",task_id=task.task_id,node_id=node_id)
                    response = stub.UploadAndRequestInfo(TaskRequest_t)
            except:
                time.sleep(0.1)
            return node_id
        else:
            node_id = "empty"
            try:
                with grpc.insecure_channel(config.address_list[2]) as channel:  #cloud address
                    stub = helloworld_pb2_grpc.GreeterStub(channel)
                    TaskRequest_t = helloworld_pb2.TaskRequest(type="request",task_id=task.task_id,node_id="empty")
                    response = stub.UploadAndRequestInfo(TaskRequest_t)
                    node_id = response.node_id
            except:
                time.sleep(0.1)
            if node_id == "empty":
                return local_node_id_
            else:
                return node_id
    def select_candinate(self,time_cost_dict,num):
        probability_list = dict()
        total_cost = sum(time_cost_dict.values())
        for id,cost in time_cost_dict.items():
            probability_list[id] = float((total_cost-cost)/((len(time_cost_dict)-1)*total_cost))
        candidate_node = np.random.choice(list(probability_list.keys()),size=num,replace=False,p=list(probability_list.values()))
        return candidate_node

class Greeter(helloworld_pb2_grpc.GreeterServicer):
    def PushObject(self,request,context):
        byte_data = BytesIO(request.data)
        np_data = np.load(byte_data, allow_pickle=True)
        if(request.task_id not in all_task_dict.keys()):
            object_t = Object(None,request.object_id)
            object_t.InsertChunkIntoData(np_data)
            task_t = Task(request.start_time,object_t,request.deadline,request.task_id,request.vehicle_src_id,request.data_size,request.result_size,request.workload,request.chunk_num,request.chunk_index,request.node_id)
            all_task_dict[task_t.task_id] = task_t
            if "Vehicle" in nodes_[request.node_id].role:
                task_t.SetLocalServetrId(local_node_id_)
                task_queue.put(task_t)
            else:
                local_execution_queue[task_t.task_id] = task_t
                global local_queue_time_
                local_queue_time_ += task_t.workload/nodes_[local_node_id_].process_capacity
        else:
            task_corre = all_task_dict[request.task_id] 
            task_corre.ReturnObject().InsertChunkIntoData(np_data)
            if task_corre.dst_node_id and task_corre.dst_node_id != local_node_id_:
                task_sender_queue.put(task_corre)
        return helloworld_pb2.PushObjReply(message='1')
    def PushResult(self,request,context):
        byte_result = BytesIO(request.result)
        np_result = np.load(byte_result,allow_pickle=True)
        task_receive = Task(0,0,0,task_id=request.task_id,vehicle_src_id=request.vehicle_src_id,data_size=0,result_size=len(np_result),workload=0,chunk_num=0,local_server_id=request.local_server_id)
        task_receive.SetResult(np_result)
        result_sender_queue.put(task_receive)
        return helloworld_pb2.PushResultReply(message='1')
    def RealTimeInfo(self,request,context):
        return helloworld_pb2.InfoReply(local_queue_time=local_queue_time_)

def StartServer():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    helloworld_pb2_grpc.add_GreeterServicer_to_server(Greeter(), server)
    local_address = nodes_[local_node_id_].address
    server.add_insecure_port(local_address)
    server.start()
    server.wait_for_termination()


if __name__ == '__main__':
    try:
        opts,args = getopt.getopt(sys.argv[1:], 'r:', ['role='])
    except getopt.GetoptError:
        print("python edge_server.py -r role to specify the role of this code")
        sys.exit()
    for opt, arg in opts:
        if opt in ['-r' ,'--role']:
            local_role_ = arg
    log_file = local_role_ + ".log"
    log = config.Logger(log_file,level='debug')
    node_manager_ = NodeManager()
    task_manager_ = TaskManager()
    object_manager_ = ObjectManager()
    node_manager_.InitCluster(config.node_num,config.lat_list,config.long_list,config.process_capacity_list,config.address_list,config.role_list,config.node_id_list,config.bandwidth_list)
    pub_sub_t = threading.Thread(target=object_manager_.UploadQueueTimeAndGetVehicleInfo,args=(),name="GetPubSub")
    pub_sub_t.start()
    time.sleep(4)

    receive_task_result_t = threading.Thread(target=StartServer,args=(),name="ReceiveTaskAndResult")
    task_sender_queue = queue.Queue() 
    dispatch_task_t = threading.Thread(target=task_manager_.DispatchTasks,args=(task_sender_queue,),name="DispatchTasks")
    schedule_local_tasks_t = threading.Thread(target=task_manager_.ScheduleLocalTasks,args=(result_sender_queue,),name="ScheduleLocalTasks") 
    schedule_send_tasks_t = threading.Thread(target=object_manager_.SchedulingSendTasks,args=(task_sender_queue,),name="ScheduleSendTasks") 
    schedule_send_result_t = threading.Thread(target=object_manager_.SchedulingSendResult,args=(result_sender_queue,),name="ScheduleSendResult") 
    
    receive_task_result_t.start()
    dispatch_task_t.start()
    schedule_local_tasks_t.start()
    schedule_send_result_t.start()
    schedule_send_tasks_t.start()
    
    receive_task_result_t.join()
    dispatch_task_t.join()
    schedule_local_tasks_t.join()
    schedule_send_result_t.join()
    schedule_send_tasks_t.join()
    pub_sub_t.join()