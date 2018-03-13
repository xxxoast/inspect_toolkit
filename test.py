# -*- coding:utf-8 -*-
from __future__ import absolute_import,unicode_literals

from utils import unicode2str_r,unicode2utf8_r,unicode2cp936_r
import json

import sys
if ".." not in sys.path:
    sys.path.append("..")
from ipcs import task
from ipcs.task import non_certify

if __name__ == '__main__':
    print non_certify.name
    result = non_certify.delay('pingan', 'merchant')
    print result.ready()
    re = json.loads(result.get())['result']
    for i in unicode2utf8_r(re):
        for j in i:
            print j,
        print ''
    print result.ready()