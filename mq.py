# encoding : utf-8

import zmq
from utils import unicode2str_r
from paras import iptable,porttable

class task_server(object):
    
    def task_server(self,place,call_back):
        
        self.place = place
        self.callback = call_back
        self.bind_ip = iptable[place]
        self.bind_port = porttable[place]
        
        ctx = zmq.Context()
        self.socket = zmq.socket(zmq.REP)
        addr = 'tcp://{}:{}'.format(self.bind_ip,self.bind_port)
        try:
            self.socket.bind(addr)
        except:
            print 'bind error, check socket taken'
        
    def run(self):
        
        while True:
            
            request = self.socket.recv_json()
            strjob = unicode2str_r(request)
            
            fname = strjob['fname']
            params = strjob['fparams']
            
            reply = self.callback[fname](**params)
            
            self.socket.send_json(reply)
            

class task_client(object):
    
    def task_client(self):
        self.ctx = zmq.context()
        
    def send_job(self,place,fname,fparams):
        socket = self.ctx.Socket(zmq.REQ)
        send_dict = {'fname':fname,'fparams':fparams}
        reply = socket.send_json(send_dict)
        socket.close()
        return unicode2str_r(reply)
    
            
