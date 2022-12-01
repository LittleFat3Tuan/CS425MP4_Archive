import socket
import sys
import threading

SIZE = 4096
FORMAT = "utf-8"

def requestConsumer():
    sock_UDP = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
    sock_UDP.bind(('0.0.0.0',8003))

    while True:
        data, addr = sock_UDP.recvfrom(1024)
        request, file_name, batch_size=data.decode(FORMAT).split(' ')[0:]

        sock_UDP.sendto(b'GETMAS 8003', ('127.0.0.1', 6019))
        masNum, _ = sock_UDP.recvfrom(1024)
        sock_UDP.sendto(b'GETMEM 8003', ('127.0.0.1', 5004))
        mem_list, _ = sock_UDP.recvfrom(1024)
        mem_list = {i.split(' ')[0]:(i.split(' ')[1],i.split(' ')[2]) for i in mem_list.decode('utf-8').split(',')}

        sock_UDP.sendto((request+' '+file_name+' '+str(batch_size)).encode(FORMAT), 
                            (mem_list[masNum.decode(FORMAT)][0], 8004))

    


def messageConsumer():
    sock_UDP = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
    sock_UDP.bind(('0.0.0.0',8005))

    while True:
        data, addr = sock_UDP.recvfrom(1024)

        state, id = data.decode(FORMAT).split(' ')[0:]

        if state == 'SUBACK':
            print('Job submission successful! Job ID: '+id)
        elif state == 'F':
            print('Job submission successful! Result file name: '+id)
        elif state == 'SUBNACK':
            print('Job submission failed!')
        else:
            print(f'Job {id} failed! Please try again later!')


threading.Thread(target=requestConsumer).start()
threading.Thread(target=messageConsumer).start()