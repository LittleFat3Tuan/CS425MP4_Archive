import hashlib
import socket
import sys
import threading
import time
import logging
import re
import os




SIZE = 4096
FORMAT = 'utf-8'
job_id = 0
JOBS = (None, None)

with open('../config.txt') as f:
    line = f.readlines()[0]
    MACHINENUM, SELF_IP = int(line.split(" ")[0].strip()), line.split(" ")[1].strip()

sock_JOB1_A = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
sock_JOB1_A.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
sock_JOB1_A.settimeout(1)
sock_JOB1_A.bind(('127.0.0.1', 8008))
sock_JOB1_F = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
sock_JOB1_F.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
sock_JOB1_F.settimeout(1)
sock_JOB1_F.bind(('127.0.0.1', 8010))

sock_JOB2_A = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
sock_JOB2_A.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
sock_JOB2_A.settimeout(1)
sock_JOB2_A.bind(('127.0.0.1', 8009))
sock_JOB2_F = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
sock_JOB2_F.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
sock_JOB2_F.settimeout(1)
sock_JOB2_F.bind(('127.0.0.1', 8011))

tracker_dict = {}

def store_in_SDFS(local_filename,remote_filename):
    cmd = "./commands.sh put "+local_filename+ ' ' + remote_filename
    os.system(cmd)
    
def get_in_SDFS(local_filename,remote_filename):
    cmd = "./commands.sh get "+local_filename+ ' ' + remote_filename
    os.system(cmd)        

def split_file(filename,batch_size,job_id):
    file = open(filename, 'r')
    lines = file.readlines()
    file.close()
    line_list = []
    for line in lines:
        line_list.append(line)
    count = len(line_list) 
    diff_match_split = [line_list[i:i+batch_size] for i in range(0,len(line_list),batch_size)]
    for j in range(len(diff_match_split)):
         with open(filename+'_'+str(job_id)+'_'+str(j),'w+') as temp:
            for line in diff_match_split[j]:
                temp.write(line)
    
        
    return len(diff_match_split)


class tracker:
    def __init__(self, filename:str, jobtype:str, ID:int, batch_size:int, members:dict, sockA:socket, sockF:socket) -> None:
        '''
            filename: str, SDFS file name to be inferenced
            jobtype: str, languange or vision
            ID: int, Job ID
            batch_size: int, batch_size
            members: dict, entries of mem_list
            machinedict: dict, status of all members, None(available) or queryName(occupied)
            done: list, queries processed and the processor
            todo: list, query to be processed
            numQuery: int, total number of queries
            sock: socket, a UDP socket object to receive message
        '''
        self.data = filename
        self.ID = ID
        self.type = jobtype
        self.batch_size = batch_size
        self.machinedict = {vm:None for vm in members.keys()}
        #self.count_size = length
        self.members = members
        self.done = []
        self.todo = []
        get_in_SDFS(filename,filename)
        self.numQuery = split_file(filename,batch_size,ID)
        self.sockA = sockA
        self.sockF = sockF
        self._stopEvent = threading.Event()
        for i in range(self.numQuery):
            #Task name distributed for each machine
            task_filename = filename+'_'+str(self.IF)+'_'+str(i)
            store_in_SDFS(task_filename,task_filename)
            self.todo.append(task_filename)


    def machine_name(self,num,status):
        self.machinedict[num] = status
        
    def change_status(self,changed_Status):
        self.status = changed_Status
    
    def inference(self):
        for vm in self.machinedict.keys():
            task = self.todo.pop(0)
            self.sockA.sendto(('IT '+ task).encode(FORMAT), (self.members[vm][0], 8001))
            ack, _ = self.sockA.recvfrom(SIZE)
            if ack.startswith(b'IACK'):
                self.machine_name[vm] = task
        while True:
            if self._stopEvent.isSet():
                print('Job terminating!')
                break
            try:
                msg, addr = self.sockF.recvfrom(SIZE)
            except:
                continue
            if msg.startswith(b'TF'):
                taskDone = msg.decode(FORMAT).split(' ')[-1][:-4]
                self.done.append(taskDone)
                for vm in self.machinedict.keys():
                    if self.machinedict[vm] == taskDone:
                        task = self.todo.pop(0)
                        self.sockA.sendto(('IT '+ task).encode(FORMAT), (self.members[vm][0], 8001))
                        ack, _ = self.sockA.recvfrom(SIZE)
                        if ack.startswith(b'IACK'):
                            self.machine_name[vm] = task

            
                        
    
    def add_member(self):
        pass
    
    def remove_member(self):
        pass

    def train(self):
        SJmsg = "SJ"+' '+str(self.ID)+" "+self.type+ " "+str(self.batch_size)
        for vm in self.members.keys():
            self.sockA.sendto(SJmsg.encode(FORMAT), (self.members[vm][0], 8001))
        ack_count = 0
        while True:
            ack, _ = self.sockA.recvfrom(SIZE)
            if ack.startswith(b'SJACK'):
                ack_count += 1
            if ack_count == len(self.members):
                break
        print(f'Job {self.ID} Train finished!')
        
    def stop(self):
        print('Stopping worker...')
        self._stopEvent.set()

    def run_inference(self):
        threading.Thread(target=self.inference).start()


'''Route messages to corresponding job tracker'''
def messageRouter():
    sockAck = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
    sockAck.bind(('0.0.0.0',8006))
    sockFini = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
    sockFini.bind(('0.0.0.0',8012))


def start_job(filename,jobtype,mem_list,batch_size):
    global job_id
    msg_inf_list= []
    msg_job_list = []
    job_id_local = job_id
    job_id += 1 
    #Split the file, get how many files the file was spliited into.
    get_in_SDFS(filename,filename)
    file_count = split_file(filename,batch_size,job_id_local)
    tracker_dict[job_id_local] = tracker(filename)

    for i in range(file_count):
        job_type_msg = "SJ"+' '+str(job_id_local)+" "+jobtype+ " "+str(batch_size)
        #Task name distributed for each machine
        task_filename = filename+'_'+str(job_id_local)+'_'+str(i)
        store_in_SDFS(task_filename,task_filename)
        #The msg_inf_list filename of info.
        msg_inf_list.append('IT '+ task_filename)
        msg_job_list.append(job_type_msg)


    sendML_UDP = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
    sendML_UDP.bind(('0.0.0.0',8012))
    sendML_UDP.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
    count = 0
    
    #Distribute mission to machines 
    joined = list(mem_list.keys())
    machine_count = len(joined)
    # META_dist = {}
    # for i in range(file_count):
    #     index = str(joined[i%machine_count])
    #     META_dist[index] = META_dist[index]+','+msg_inf_list[i].split(' ')[1]
    #Send all the files to the Worker.
    ACK = None
    broken = []
    while count < file_count:
        index = str(joined[count%machine_count])
        if index in broken:
            index = str(int(index)+1) 
        deadline = time.time() + 3 
        sendML_UDP.sendto(msg_job_list[count].encode('utf-8'),(mem_list[index][0],8001))
        while time.time() < deadline:
            ACK,_ = sendML_UDP.recvfrom(1024)
            print('ACK Received')
            ACK = ACK.decode('utf-8')
            if ACK:
                sendML_UDP.sendto(msg_inf_list[count].encode('utf-8'),(mem_list[index][0],8001))
                tracker_dict[job_id_local].machine_name(msg_inf_list[count].split(' ')[1],(mem_list[index][0],'In Progress'))
                break
        if not ACK:
            broken.append(index)
            continue
        else:
            count += 1 
            continue


def job_tracker():
    global tracker_dict
    jobstatus_UDP = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
    jobstatus_UDP.bind(('0.0.0.0',8006))
    while True:
        info,address = jobstatus_UDP.recvfrom(SIZE)
        info =info.decode('utf-8')
        print('Info Received'+info)
        if info.startswith('TF'):
            job_name = info.split(' ')[1].split("_")[1]
            job_name = int(job_name)
            tracker_dict[job_name].machine_name(info.split(' ')[1][:-4],(address[0],'Finished'))
            count_notfinished = 0
            for vm in tracker_dict[job_name].machinedict.values():
                if address[0] == vm[0]:
                    if 'Finished' != vm[1]:
                        count_notfinished += 1
                        
            if count_notfinished == 0:
                jobstatus_UDP.sendto(('EJ'+" "+str(job_name)).encode('utf-8'),(address[0],8001))
            if 'In Progress' not in [item[1] for item in tracker_dict[job_name].machinedict.values()]:
                print(tracker_dict[job_name].machinedict)
                tracker_dict[job_name].change_status('Completed')
                all_filenames = tracker_dict[job_name].machinedict.keys()
                for i in all_filenames:
                    get_in_SDFS(i,i)
                for j in all_filenames:
                    data_got = open(j,encoding = 'utf-8')
                    data_got = data_got.read()
                    with open(str(job_name)+'_translated','w+') as temp:
                        temp.write(data_got)







def main_SERVER():
    global JOBS
    #get file from SDFS
    #split file into smaller files 
    #store the file into SDFS
    #based on the smaller files name, each file start a job
    #In the meantime the job_tracker watches over, to see if finished
    #If finished, get them from the SDFS and aggregate
    requestRecv_UDP = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
    requestRecv_UDP.bind(('0.0.0.0',8004))
    getmem_UDP = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
    getmem_UDP.bind(('0.0.0.0',8002))
    while True:
        request,addr = requestRecv_UDP.recvfrom(1024)
        if None not in JOBS:
            requestRecv_UDP.sendto(b'SUBNACK -1', (addr[0], 8005))
            continue
        request = request.decode('utf-8')
        print("Received Request"+" "+request)
        getmem_UDP.sendto(b'GETMEM 8002', ('127.0.0.1', 5004))
        #get mem_list from membership
        mem_list, _ = getmem_UDP.recvfrom(1024)
        mem_list = {i.split(' ')[0]:(i.split(' ')[1],i.split(' ')[2]) for i in mem_list.decode('utf-8').split(',')}
        mem_list.pop(str(MACHINENUM),None)
        file_name = request.split(' ')[1]
        batch_size = int(request.split(' ')[-1])
        get_in_SDFS(file_name,file_name)
        start_job(file_name,'T',mem_list,batch_size)

 
threading.Thread(target=main_SERVER).start() 
threading.Thread(target=job_tracker).start()      
