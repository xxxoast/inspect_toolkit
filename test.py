# -*- coding:utf-8 -*-
#from __future__ import absolute_import,unicode_literals

from utils import unicode2str_r,unicode2utf8_r,unicode2cp936_r
import json

import os,sys
pkg_path = os.path.sep.join(
    (os.path.abspath(os.curdir).split(os.path.sep)[:-1]))
if pkg_path not in sys.path:
    sys.path.append(pkg_path)

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