# -*-coding:utf-8 -*-
from __future__ import print_function
from concurrent import futures
import logging
from logging import handlers

import grpc
import helloworld_pb2
import helloworld_pb2_grpc
import cluster_config as config
import time,threading
import numpy as np
import math,csv
from io import BytesIO
import uuid
import queue,sys,getopt
from cluster_config import Task,Node,Object

nodes_ = dict()
local_node_id_ = 0
local_role_ = "Vehicle-1"
all_task_dict_ = dict()
local_queue_time = 0
current_edge_server_id_ = 0
sojourn_time = 0
average_queue_time = 0
total_num = 50
finish_num = 0
total_completion_time = 0
ddl_meet_num = 0

class NodeManager:
    def InitCluster(self,node_num,lat_list,long_list,process_capacity_list,address_list,role_list,node_id_list,bandwidth_list):
        for i in range(node_num):
            node_id = node_id_list[i]
            global local_node_id_
            if role_list[i] == local_role_:
                local_node_id_ = node_id
            node = Node(lat_list[i],long_list[i],process_capacity_list[i],node_id,address_list[i],role_list[i],bandwidth_list[i])
            nodes_[node_id] = node

def CalDistanceFromLocal(node):
    local_lat = nodes_[local_node_id_].lat
    local_long = nodes_[local_node_id_].long
    lat_diff = float(node.lat) - float(local_lat)
    long_diff = float(node.long) - float(local_long) 
    return math.sqrt(lat_diff * lat_diff + long_diff * long_diff)

def CalDataTransferTime(node,data_size):
    distance = CalDistanceFromLocal(node)
    wireless_rate = 10.0
    delay = data_size/wireless_rate
    return delay
            
class MobilityManager:
    def __init__(self,dir_trace,pointer):
        self.dir_trace = dir_trace
        self.position_trace = [list() for i in range(2)] 
        self.pointer = pointer
    def ReadTrace(self):
        with open(self.dir_trace, 'r') as f:
            reader = csv.reader(f, delimiter=',')
            for row in reader:
                for i in range(len(self.position_trace)):
                    self.position_trace[i].append(row[i])
    def UpdateReportGetInfo(self):
        while True:
            start_time = time.time()
            while time.time() - start_time < config.position_upload_time:
                continue
            if not (self.pointer < len(self.position_trace[0])): 
                self.pointer = 0
            new_lat = self.position_trace[0][self.pointer]
            new_long = self.position_trace[1][self.pointer]
            local_node = nodes_[local_node_id_]
            local_node.ChangePosition(new_lat,new_long)
            for node in nodes_.values():
                distance = CalDistanceFromLocal(node)
                global current_edge_server_id_
                if distance < 150 and node.node_id != current_edge_server_id_ and node.role != local_role_ and "Vehicle" not in node.role:
                    current_edge_server_id_ = node.node_id
                if node.role == "Cloud":
                    cloud_address = node.address
            try:
                with grpc.insecure_channel(cloud_address) as channel:
                    stub = helloworld_pb2_grpc.GreeterStub(channel)
                    PushRequest_t = helloworld_pb2.VehicleReportRequest(node_id=local_node_id_,current_lat=float(new_lat),current_long=float(new_long),trace_index=int(self.pointer))
                    response = stub.VehicleReport(PushRequest_t)
                    global sojourn_time,average_queue_time
                    sojourn_time = response.sojourn_time
                    average_queue_time = response.average_queue_time
            except:    
                time.sleep(0.1)
            self.pointer += 1
    
class ObjectManager:
    def __init__(self):
        self.queue_threshold = [0,0.4,0.8,1.3,1.8,2.5,3.0,3.5,4.0,4.5,6.0,8.0,15.0]
    def SchedulingSendTasks(self,task_sender_queue):
        while True:
            if len(task_sender_queue) == 0:
                continue
            slacktime_task_dict = dict()
            send_task_id = -1
            for task in list(task_sender_queue.values()):
                if task.object.pointer == task.chunk_num:
                    continue                    
                data_send_time = CalDataTransferTime(nodes_[current_edge_server_id_],task.data_size)
                result_send_time = CalDataTransferTime(nodes_[current_edge_server_id_],task.result_size)
                necessary_time_list = []
                for node in nodes_.values():
                    if "Vehicle" in node.role:
                        continue
                    elif node.node_id == current_edge_server_id_:
                        necessary_time = data_send_time + task.workload/node.process_capacity + result_send_time
                    else:
                        necessary_time = data_send_time + task.data_size/node.bandwidth + task.workload/node.process_capacity + task.result_size/node.bandwidth + result_send_time
                    necessary_time_list.append(necessary_time)
                ave_necessary_time = np.mean(necessary_time_list)
                slack_time = task.deadline - ave_necessary_time - (time.time()-task.start_time)
                slacktime_task_dict[task.task_id] = slack_time
            if len(slacktime_task_dict.keys()) > 0:
                priority_queue = [dict() for i in range(len(self.queue_threshold)-1)]
                send_task_id = self.InsertQueue(slacktime_task_dict,priority_queue)
                task_send = task_sender_queue[send_task_id]
                self.PushObject(task_send) 
                task_send.ReturnObject().AddChunkPointer()    
    def InsertQueue(self,slacktime_task_dict,priority_queue):
        for id,slack in slacktime_task_dict.items():
            for i in range(len(self.queue_threshold)):
                if slack > self.queue_threshold[i] and slack <= self.queue_threshold[i+1]:
                    priority_queue[i][id] = task_sender_queue[id].start_time
        for i in range(len(priority_queue)):
            sort_list = sorted(priority_queue[i].items(), key = lambda kv:(kv[1], kv[0]))
            if len(sort_list) > 0:
                task_id = sort_list[0][0]
                break
        return task_id
    def PushObject(self,task):
        try:
            dst_address = nodes_[current_edge_server_id_].address
            with grpc.insecure_channel(dst_address) as channel:
                stub = helloworld_pb2_grpc.GreeterStub(channel)
                object_t = task.object
                byte_data = BytesIO()
                np.save(byte_data,object_t.chunk_list[task.object.pointer] , allow_pickle=True)
                byte_data = byte_data.getvalue()
                PushRequest_t = helloworld_pb2.PushObjRequest(task_id=task.task_id,object_id=object_t.object_id,node_id=local_node_id_,chunk_index=task.object.pointer,chunk_num=task.chunk_num,data=byte_data,deadline=task.deadline,start_time=task.start_time,data_size=task.data_size,result_size=task.result_size,workload=task.workload,vehicle_src_id=task.vehicle_src_id)
                response = stub.PushObject(PushRequest_t)  
        except:
            time.sleep(0.01)

def RecordTaskFinishEvent(task_id,result):
    task = all_task_dict_[task_id]
    task.SetResult(result)
    task.SetEndTime(time.time())
    global finish_num,ddl_meet_num,total_completion_time
    finish_num += 1
    completion_time = task.end_time - task.start_time
    if completion_time <= task.deadline:
        ddl_meet_num += 1
        total_completion_time += completion_time
    item_list = [task.task_id,task.data_size,task.workload,task.deadline,completion_time]
    error_log.logger.error("%s",item_list)
    if finish_num == total_num:
        result_list = [total_completion_time/finish_num,ddl_meet_num/total_num,finish_num]
        error_log.logger.error("%s",result_list)

class TaskManager:
    def __init__(self):
        self.local_execution_queue = dict()
    def OffloadTasks(self,task_queue,task_sender_queue):
        while True:
            task = task_queue.get()
            best_node_id = self.OffloadPolicy(task)
            task.SetDestination(best_node_id)
            all_task_dict_[task.task_id] = task
            if best_node_id == local_node_id_:
                global local_queue_time
                local_queue_time += task.workload/nodes_[local_node_id_].process_capacity
                self.local_execution_queue[task.task_id] = task
            else:
                task_sender_queue[task.task_id] = task
    def ScheduleLocalTasks(self):
        while True:
            if len(self.local_execution_queue) == 0:
                continue
            ddl_task_dict = dict()
            for task in list(self.local_execution_queue.values()):
                if task.end_time == -1:
                    ddl_task_dict[task.task_id] = task.deadline + task.start_time
            if len(ddl_task_dict.keys()) > 0:
                min_ddl_id = min(ddl_task_dict,key=ddl_task_dict.get)
                task_execute = self.local_execution_queue[min_ddl_id]
                result = self.ExecuteTask(task_execute)
                RecordTaskFinishEvent(task_execute.task_id,result)
                a = self.local_execution_queue.pop(task_execute.task_id)
    def ExecuteTask(self,task):
        data_array = task.object.data
        for i in range(len(data_array)):
            result = result + data_array[i]
        result = np.array([len(data_array),result])
        time.sleep(task.workload/nodes_[local_node_id_].process_capacity)
        return result
    def OffloadPolicy(self,task):
        for node in nodes_.values():
            if node.role == local_role_:
                local_finish_time = local_queue_time + task.workload/node.process_capacity
                break
        if local_finish_time <= task.deadline:
            return local_node_id_
        else:
            return current_edge_server_id_

class User:
    def __init__(self,scheduling_slot,task_num,dir_trace):
        self.scheduling_slot = scheduling_slot
        self.task_num = task_num
        self.task_pointer = 0
        self.dir_trace = dir_trace
        self.task_info = [list() for i in range(6)]
    def ReadTrace(self):
        with open(self.dir_trace, 'r') as f:
            reader = csv.reader(f, delimiter=',')
            for row in reader:
                for i in range(len(self.task_info)):
                    self.task_info[i].append(row[i])
    def UserGenerateTasks(self,task_queue):
        while(self.task_pointer < self.task_num):
            info = self.task_info
            index = self.task_pointer
            week, month, day, time_now, year = time.ctime().split()
            check_time = time_now
            object_id = "Object-" + check_time + str(uuid.uuid4())
            task_id = "Task-" + str(index) + "-" + check_time + "-" + local_role_
            data_size = float(info[2][index])
            data = np.array(range(index,index + int(data_size)))
            def cut(obj, sec):
                return [obj[i:i+sec] for i in range(0,len(obj),sec)]
            cut_data_list = cut(data,3) 
            chunk_num = int(len(cut_data_list))
            object_t = Object(data,object_id)
            object_t.CutDataIntoChunk(chunk_num)
            result_size = float(info[3][index])
            workload = float(info[4][index])
            deadline = float(info[5][index])
            task = Task(time.time(),object_t,deadline,task_id,local_node_id_,data_size,result_size,workload,chunk_num,"empty")
            task_queue.put(task)
            self.task_pointer += 1
            former_time = time.time()
            while time.time() - former_time < float(info[1][index]):
                continue

class Greeter(helloworld_pb2_grpc.GreeterServicer):
    def PushResult(self,request,context):
        byte_result = BytesIO(request.result)
        np_result = np.load(byte_result,allow_pickle=True)
        RecordTaskFinishEvent(request.task_id,np_result)
        return helloworld_pb2.PushResultReply(message='1')

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
    task_log = local_role_ + '-task.log'
    log = config.Logger(log_file,level='debug')
    error_log = config.Logger(task_log, level='error')
    trace_diff = 200
    for i in range(10):
        if str(i) in local_role_:
            trace_start_index = (i-1) * trace_diff
    node_manager_ = NodeManager()
    task_manager_ = TaskManager()
    object_manager_ = ObjectManager()
    dir_trace = "position_trace.csv"
    mobility_manager_ = MobilityManager(dir_trace,trace_start_index)
    mobility_manager_.ReadTrace()
    user_ = User(0.3,total_num,"task_trace.csv")
    user_.ReadTrace()
    node_manager_.InitCluster(config.node_num,config.lat_list,config.long_list,config.process_capacity_list,config.address_list,config.role_list,config.node_id_list,config.bandwidth_list)
    mobility_manager_t = threading.Thread(target=mobility_manager_.UpdateReportGetInfo,args=(),name="MobilityManager")
    mobility_manager_t.start()
    time.sleep(2)
    task_queue = queue.Queue()
    task_sender_queue = dict()
    generate_task_t = threading.Thread(target=user_.UserGenerateTasks,args=(task_queue,),name="UserGenerateTasks")
    offload_task_t = threading.Thread(target=task_manager_.OffloadTasks,args=(task_queue,task_sender_queue,),name="OffloadTasks")
    schedule_local_tasks_t = threading.Thread(target=task_manager_.ScheduleLocalTasks,args=(),name="ScheduleLocalTasks") 
    schedule_send_tasks_t = threading.Thread(target=object_manager_.SchedulingSendTasks,args=(task_sender_queue,),name="ScheduleSendTasks") 
    thread_receive_result = threading.Thread(target=StartServer,args=(),name="ReceiveResult")

    generate_task_t.start()
    offload_task_t.start()
    schedule_local_tasks_t.start()
    schedule_send_tasks_t.start()
    thread_receive_result.start()
    
    mobility_manager_t.join()
    generate_task_t.join()
    offload_task_t.join()
    schedule_local_tasks_t.join()
    schedule_send_tasks_t.join()
    thread_receive_result.join()