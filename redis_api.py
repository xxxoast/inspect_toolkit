# encoding : utf-8
import json
import redis

from utils import unicode2str_r

class ipc_db_api(object):

    def __init__(self, db_addr='127.0.0.1', db_port=6379, db_name=5):
        self.iredis = redis.Redis(host=db_addr, port=db_port, db=db_name)

    def exists(self, key):
        return self.iredis.exists(key)

    def get_key(self, date, username, loginID, requestID, funcName, pipeline):
        sep = '|'
        if int(pipeline) > 0:
            sfunc = sep.join(funcName)
        else:
            sfunc = funcName
        return ':'.join((str(date), username, loginID, str(requestID), sfunc,
                         str(pipeline)))

    def set_value(self, key, **value):
        json_str = json.dumps(value)
        self.iredis.set(key, json_str)

    def get_value(self, key):
        if self.iredis.exists(key):
            value = self.iredis.get(key)
        else:
            return None
        pydict = json.loads(value)
        return unicode2str_r(pydict)

if __name__ == '__main__':
    pass
