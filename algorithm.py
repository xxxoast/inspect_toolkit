# encoding : utf-8
from __future__ import unicode_literals   
from config import inspect_obj_table
from config import error_code
from utils import unicode2utf8_r,unicode2cp936_r
import json

merchant_search_sql = '''
    SELECT 
        COUNT(1) as 'merchant_numbers', 
        t.account_number AS 'account_number',
        t.account_name AS 'account_name'
    FROM
        {0}.hangzhou_swap_code_trade t
    where t.account_number is not null
    GROUP BY t.account_number,t.account_name
    having count(1) > 20
    order by count(1) desc;
    '''
    
def search(corpration,inspect_item):
    if inspect_item in inspect_obj_table.keys():
        try:
            dataAPI = inspect_obj_table[inspect_item](db_name = corpration)
            status,error_code = 0,0
            cursor = dataAPI.execute_sql(merchant_search_sql.format(corpration))
            result = []
            for i in cursor:
                field = []
                for j in i:   
                    field.append(j)
                result.append(field)
            result = unicode2utf8_r(result)
            return json.dumps({'status':status,'error_code':error_code,'result':result})
        except BaseException,arg:
            status,ec,result = 1,1,error_code[1]
            return json.dumps({'status':status,'error_code':ec,'result':result})
    else:
        status,ec,result = 1,2,error_code[2]
        return json.dumps({'status':status,'error_code':ec,'result':result})
        
if __name__ == '__main__':
    result = json.loads(search('pingan','merchant'))
    if result['status'] == 0:
        print type(result['result']),result['result']
    else:
        print result
        
    
    
    