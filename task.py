# encoding : cp936

from db_table import Trade,Merchant

inspect_obj_table = {
    'trade':Trade,
    'merchant':Merchant    
}

class second_settlement(object):
    
    def __init__(self):
        self.merchant_search_sql = '''
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
    
    def search(self,corpration,inspect_item):
        dataAPI = inspect_obj_table[inspect_item](db_name = corpration)
        return dataAPI.execute_sql(self.merchant_search_sql.format(corpration))
        
if __name__ == '__main__':
    ss = second_settlement()
    for i in ss.search('pingan','merchant'):
        for j in i:
            print j,
        print ''
        
    
    
    