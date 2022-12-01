import socket
import os
from transformers import MarianMTModel, MarianTokenizer, ViTFeatureExtractor, ViTForImageClassification
import torch
import sys
from collections import deque
import threading
from PIL import Image
import requests
from Client import client as callSDFS
import time

FORMAT = 'utf-8'

'''Receive work instruction from coordinator, and create worker object.'''
def instructionConsumer():
    sock_ML = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
    sock_ML.bind(('0.0.0.0',8001))
    while True:
        data, addr = sock_ML.recvfrom(1024)
        if data.startswith(b'SJ'):
            print(f'Starting Job '+data.decode(FORMAT).split(' ')[1]+'. Job type '+data.decode(FORMAT).split(' ')[2])
            worker = Worker(*(data.decode(FORMAT).split(' ')[1:]))
            sock_ML.sendto(('SJACK '+ data.decode(FORMAT).split(' ')[1]).encode(FORMAT), (addr[0], 8012))
            worker.run()
            while True:
                train_info, addr = sock_ML.recvfrom(1024)
                if train_info.startswith(b'IT'):
                    filename = train_info.decode(FORMAT).split(' ')[1]
                    sock_ML.sendto(('IACK '+ filename).encode(FORMAT), (addr[0], 8012))
                    worker.addTask(filename)
                elif train_info.startswith(b'EJ'):
                    worker.stop()
                    sock_ML.sendto(('EJACK '+ filename).encode(FORMAT), (addr[0], 8012))
                    break


'''Worker object that process all the inference'''
class Worker:
    def __init__(self, ID, type, batch_size=1):
        self.ID = ID
        if type == 'T':
            self.type = 'Language'
        else:
            self.type = 'Vision'
        self.batch_size = batch_size
        self.taskQ = []
        self.workDone = []
        self._stopEvent = threading.Event()
        self.sock = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
        self.sock.bind(('0.0.0.0', 8007))
        if self.type == 'Language':
            self.model = {'tokenizer': MarianTokenizer.from_pretrained("Helsinki-NLP/opus-mt-en-ROMANCE"),
                            'model': MarianMTModel.from_pretrained("Helsinki-NLP/opus-mt-en-ROMANCE")}
        elif self.type == 'Vision':
            self.model = {'featureExtractor': ViTFeatureExtractor.from_pretrained("google/vit-base-patch16-224"),
                            'model': ViTForImageClassification.from_pretrained("google/vit-base-patch16-224")}
        print('Worker for Job '+str(ID)+' created!')
    
    '''Inference function for language task'''
    def inferLang(self, src_text):
        translated = self.model['model'].generate(**self.model['tokenizer'](src_text, return_tensors="pt", padding=True,max_length = 1024))
        tgt_text = [self.model['tokenizer'].decode(t, skip_special_tokens=True) for t in translated]
        return ' '.join(tgt_text)
    
    '''Inference function for vision task'''
    def inferVis(self, src_img):
        img = Image.open(requests.get(src_img, stream=True).raw)
        inputs = self.model['featureExtractor'](img, return_tensors="pt")

        with torch.no_grad():
            logits = self.model['model'](**inputs).logits

        predicted_label = logits.argmax(-1).item()
        return self.model['model'].config.id2label[predicted_label]
    
    def work(self):
        if self.type == 'Language':
            infer = self.inferLang
        elif self.type == 'Vision':
            infer = self.inferVis
        
        print('Worker for Job '+str(self.ID)+' start working!')
        
        while True:
            if self._stopEvent.isSet():
                print('Job terminating!')
                break
            if not self.taskQ:
                continue
            filename = self.taskQ.pop(0)
            # cmd = './commands.sh get'+' ' + filename+ ' ' + filename
            # os.system(cmd)
            while True:
                exitCode = callSDFS('RR', filename, filename)
                if exitCode == 1:
                    break
                else:
                    time.sleep(0.5)

            with open(filename, 'r') as in_f, \
                open(filename+'_inf', 'w') as out_f:
                for line in in_f:
                    data = infer(line.strip())
                    #print(data)
                    out_f.write(data+'\n')
            # cmd = './commands.sh put'+' ' + filename+'_inf'+ ' ' + filename+'_inf'
            # os.system(cmd)
            while True:
                exitCode = callSDFS('WR', filename+'_inf', filename+'_inf')
                if exitCode == 1:
                    break
                else:
                    time.sleep(0.5)
            self.workDone.append(filename)
            self.sock.sendto(b'GETMAS 8007', ('127.0.0.1', 6019))
            masNum, _ = self.sock.recvfrom(1024)
            self.sock.sendto(b'GETMEM 8007', ('127.0.0.1', 5004))
            mem_list, _ = self.sock.recvfrom(1024)
            mem_list = {i.split(' ')[0]:(i.split(' ')[1],i.split(' ')[2]) for i in mem_list.decode('utf-8').split(',')}

            self.sock.sendto(('TF '+filename+'_inf').encode(FORMAT), (mem_list[masNum.decode(FORMAT)][0], 8006))

    
    def addTask(self, task):
        self.taskQ.append(task)

    def getInfo(self):
        return {'ID': self.ID, 'type':self.type, 
                'batch_size':self.batch_size, 'workDone': self.workDone}
            
    def stop(self):
        print('Stopping worker...')
        self._stopEvent.set()

    def run(self):
        threading.Thread(target=self.work).start()
    

threading.Thread(target=instructionConsumer).start()

    

        

                




