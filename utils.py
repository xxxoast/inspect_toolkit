# encoding : utf-8

from multiprocessing import Process
import time

almost_zero = 0.0001
infinite_plus = 1e10
infinite_minus = -1e10

unicode2utf8 = lambda x: x.encode('utf8') if isinstance(x,unicode) else x
unicode2cp936 = lambda x: x.encode('cp936') if isinstance(x,unicode) else x

def run_paralell_tasks(func, iter_args):
    tasks = []
    for iarg in iter_args:
        task = Process(target=func, args=(iarg,))
        tasks.append(task)
    for itask in tasks:
        itask.start()
    for itask in tasks:
        itask.join()

def run_paralell_task_pairs(funcs,args):
    tasks = []
    for func,arg in zip(funcs,args):
        task = Process(target=func, args=(arg,))
        tasks.append(task)
    for itask in tasks:
        itask.start()
    for itask in tasks:
        itask.join()
        
def dict_to_lower(d):
    l = {}
    for k, v in d.iteritems():
        if not isinstance(v, str):
            if isinstance(v, list):
                l[k] = map(lambda x: str.lower(x), v)
            else:
                l[k] = v
        else:
            l[k] = str.lower(v)
    return l


def unicode2str_r(obj):
    if isinstance(obj, dict):
        new_dict = {}
        for k, v in obj.iteritems():
            new_dict[unicode2str_r(k)] = unicode2str_r(v)
        return new_dict
    elif isinstance(obj, list):
        new_list = [unicode2str_r(i) for i in obj]
        return new_list
    elif isinstance(obj, unicode):
        return str(obj)
    else:
        return obj

def unicode2utf8_r(obj):
    if isinstance(obj, dict):
        new_dict = {}
        for k, v in obj.iteritems():
            new_dict[unicode2utf8_r(k)] = unicode2utf8_r(v)
        return new_dict
    elif isinstance(obj, list):
        new_list = [unicode2utf8_r(i) for i in obj]
        return new_list
    elif isinstance(obj, unicode):
        return unicode2utf8(obj)
    else:
        return obj
   
def unicode2cp936_r(obj):
    if isinstance(obj, dict):
        new_dict = {}
        for k, v in obj.iteritems():
            new_dict[unicode2cp936_r(k)] = unicode2cp936_r(v)
        return new_dict
    elif isinstance(obj, list):
        new_list = [unicode2cp936_r(i) for i in obj]
        return new_list
    elif isinstance(obj, unicode):
        return unicode2cp936(obj)
    else:
        return obj   
   
   
def get_today():
    t = time.localtime()
    return t.tm_year * 10000 + t.tm_mon * 100 + t.tm_mday


def get_hourminsec():
    t = time.localtime()
    return t.tm_hour * 10000 + t.tm_min * 100 + t.tm_sec

    