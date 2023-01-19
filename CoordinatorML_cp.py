import hashlib
import socket
import sys
import threading
import time
import logging
import re
import os




SIZE = 4096
job_id = 0


tracker_dict = {}

class tracker:
    def __init__(self, filename, jobtype, ID, ):
        self.filename = filename
        self.machinedict = {}
        #self.count_size = length
        self.status = "Uncompleted"

    def machine_name(self,num,status):
        self.machinedict[num] = status
        
    def change_status(self,changed_Status):
        self.status = changed_Status
    
    def task_assign(self):
        pass
    
    def progress_tracking(self):
        pass
    
    def add_member(self):
        pass
    
    def remove_member(self):
        pass

    def run(self):
        time.sleep(5)
        while True:
            if self._stopEvent.isSet():
                print('Job terminating!')
                break
            if None in JOBS:
                break
            if JOBS[0].status == 4 and JOBS[1].status == 4:
                print('Both Jobs ended!')
            if JOBS[0].status == 4 or JOBS[1].status == 4:
                print('One of Jobs ended!')
                if JOBS[0].status == 4 and JOBS[1].status <= 4:
                    print('Job 0 ended!')
                    for vm in JOBS[0].members.keys():
                        mem = (vm, JOBS[0].members[vm])
                        if mem == 'Denied!':
                            break
                        JOBS[1].add_member(mem)
                        print('Machine '+mem[0]+' added to Job '+str(JOBS[1].ID))
                    JOBS[0].members = {}
                else:
                    print('Job 1 ended!')
                    for vm in JOBS[1].members.keys():
                        mem = (vm, JOBS[1].members[vm])
                        if mem == 'Denied!':
                            break
                        JOBS[0].add_member(mem)
                        print('Machine '+mem[0]+' added to Job '+str(JOBS[0].ID))
                    JOBS[1].members = {}
                break
            # qrate1 = JOBS[0].queryrate_est()
            # qrate2 = JOBS[1].queryrate_est()
            qqrate1 = JOBS[0].query_rate()
            qqrate2 = JOBS[1].query_rate()
            if qqrate1 == -1 or qqrate2 == -1:
                continue
            qqrate1 = max(qqrate1, 0.01)
            qqrate2 = max(qqrate2, 0.01)
            if 1.2 < qqrate1 / qqrate2 or qqrate1 / qqrate2 < 0.83:
            #if True:
                machine_num1 = len(JOBS[0].members)
                machine_num2 = len(JOBS[1].members)
                getmem_UDP.sendto(b'GETMEM 8002', ('127.0.0.1', 5004))
                #get mem_list from membership
                mem_list, _ = getmem_UDP.recvfrom(1024)
                mem_list = {i.split(' ')[0]:(i.split(' ')[1],i.split(' ')[2]) for i in mem_list.decode('utf-8').split(',')}
                if '0' in mem_list:
                    total_machine = len(mem_list) - 2
                else:
                    total_machine = len(mem_list) - 1
                if qqrate1 < qqrate2:
                    mem = JOBS[1].remove_member()
                    if mem == 'Denied!':
                        continue
                    print('Machine '+mem[0]+' removed from Job '+str(JOBS[1].ID))
                    JOBS[0].add_member(mem)
                    print('Machine '+mem[0]+' added to Job '+str(JOBS[0].ID))
                    time.sleep(3)
                else:
                    mem = JOBS[0].remove_member()
                    if mem == 'Denied!':
                        continue
                    print('Machine '+mem[0]+' removed from Job '+str(JOBS[0].ID))
                    JOBS[1].add_member(mem)
                    print('Machine '+mem[0]+' added to Job '+str(JOBS[1].ID))
                    time.sleep(3)

                #new_machine_num1 = round(qrate2/machine_num2*total_machine/(qrate1/machine_num1 + qrate2/machine_num2))
                #new_machine_num1 = round(qrate1/(qrate1+qrate2)*total_machine)
                # if new_machine_num1 == 0:
                #     new_machine_num1 = 1
                # if new_machine_num1 == total_machine:
                #     new_machine_num1 -= 1
                # if new_machine_num1 == machine_num1:
                #     if new_machine_num1 == 1 or new_machine_num1 == total_machine - 1:
                #         #time.sleep(1)
                #         continue

                #     if qqrate1 < qqrate2:
                #         mem = JOBS[1].remove_member()
                #         if mem == 'Denied!':
                #             continue
                #         print('Machine '+mem[0]+' removed from Job '+str(JOBS[1].ID))
                #         JOBS[0].add_member(mem)
                #         print('Machine '+mem[0]+' added to Job '+str(JOBS[0].ID))
                #     else:
                #         mem = JOBS[0].remove_member()
                #         if mem == 'Denied!':
                #             continue
                #         print('Machine '+mem[0]+' removed from Job '+str(JOBS[0].ID))
                #         JOBS[1].add_member(mem)
                #         print('Machine '+mem[0]+' added to Job '+str(JOBS[1].ID))
                # elif new_machine_num1 > machine_num1:
                #     for i in range(new_machine_num1 - machine_num1):
                #         mem = JOBS[1].remove_member()
                #         if mem == 'Denied!':
                #             break
                #         print('Machine '+mem[0]+' removed from Job '+str(JOBS[1].ID))
                #         JOBS[0].add_member(mem)
                #         print('Machine '+mem[0]+' added to Job '+str(JOBS[0].ID))
                # else:
                #     for i in range(machine_num1 -  new_machine_num1):
                #         mem = JOBS[0].remove_member()
                #         if mem == 'Denied!':
                #             break
                #         print('Machine '+mem[0]+' removed from Job '+str(JOBS[0].ID))
                #         JOBS[1].add_member(mem)
                #         print('Machine '+mem[0]+' added to Job '+str(JOBS[1].ID))
            #time.sleep(1)


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
    sendML_UDP.bind(('0.0.0.0',8003))
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
                tracker_dict[job_id_local].machine_name(msg_inf_list[count].split(' ')[1],mem_list[index][0]+" "+'In Progress')
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
            tracker_dict[job_name].machine_name(info.split(' ')[1][:-4],address[0]+" "+'Finished')
            count_notfinished = 0
            for vm in tracker_dict[job_name].machinedict.values():
                if address[0] in vm:
                    if 'Finished' not in vm:
                        count_notfinished += 1
                        
            if count_notfinished == 0:
                jobstatus_UDP.send(('EJ'+" "+job_name).encode('utf-8'),(address[0],8001))
            if 'In Progress' not in tracker_dict[job_name].machinedict.values():
                print(tracker_dict[job_name].machinedict)
                tracker_dict[job_name].change_status('Completed')
                all_filenames = tracker_dict[job_name].machinedict.keys()
                for i in all_filenames:
                    get_in_SDFS(i,i)
                for j in all_filenames:
                    data_got = open(j,encoding = 'utf-8')
                    data_got = data_got.read()
                    with open(str(job_name)+'_translated','w+') as temp:
                        for line in all_filenames:
                            temp.write(line)







def main_SERVER():
    #get file from SDFS
    #split file into smaller files 
    #store the file into SDFS
    #based on the smaller files name, each file start a job
    #In the meantime the job_tracker watches over, to see if finished
    #If finished, get them from the SDFS and aggregate
    requestRecv_UDP = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
    requestRecv_UDP.bind(('0.0.0.0',8004))
    while True:
        request,_ = requestRecv_UDP.recvfrom(1024)
        request = request.decode('utf-8')
        print("Received Request"+" "+request)
        getmem_UDP = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
        getmem_UDP.bind(('0.0.0.0',8002))
        getmem_UDP.sendto(b'GETMEM 8002', ('127.0.0.1', 5004))
        #get mem_list from membership
        mem_list, _ = getmem_UDP.recvfrom(1024)
        mem_list = {i.split(' ')[0]:(i.split(' ')[1],i.split(' ')[2]) for i in mem_list.decode('utf-8').split(',')}
        file_name = request.split(' ')[1]
        batch_size = int(request.split(' ')[-1])
        get_in_SDFS(file_name,file_name)
        start_job(file_name,'T',mem_list,batch_size)

 
threading.Thread(target=main_SERVER).start() 
threading.Thread(target=job_tracker).start()      
