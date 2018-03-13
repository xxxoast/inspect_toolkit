# coding : utf-8
from db_base import DB_BASE
from sqlalchemy import Table,Integer,String,Column,BigInteger,Text,Float
from utils import unicode2utf8

class dbAPI(DB_BASE):

    def __init__(self, db_name, table_name = None):
        super(dbAPI, self).__init__(db_name)
        self.table_struct = None
        self.table_name = table_name
        self.col_sizes = []
        
    def create_table(self):
        if self.table_struct is not None:
            self.table_struct = self.quick_map(self.table_struct)

    def check_table_exist(self):
        if self.table_struct is not None:
            return self.table_struct.exists()
        else:
            raise Exception("no table specified")
        
    def get_row_counts(self):
        session = self.get_session()
        n = session.query(self.table_struct).count()
        session.close()
        return n
    
    def get_col_names(self):
        return [ unicode2utf8(i) for i in self.get_column_names(self.table_struct)]
    
    def get_col_length(self):
        return len(self.get_column_names(self.table_struct))
    

class Trade(dbAPI):

    def __init__(self, db_name='sz_fesc', table_name='all_trade'):
        super(Trade, self).__init__(db_name)
        self.table_struct = None
        if table_name is not None:
            self.table_name = table_name
            self.table_struct = Table(
                table_name, self.meta,
                Column('index', BigInteger),
                Column('WRKDATE', BigInteger),
                Column('SYSDTIME', Text),
                Column('SUBDAY', BigInteger),
                Column('TRANSTYPE', BigInteger),
                Column('SUBDATE', BigInteger),
                Column('SUBNODE', BigInteger),
                Column('TRANSNO', BigInteger),
                Column('TRANSCODE', BigInteger),
                Column('ORDERNO', Text),
                Column('CACCID', Text),
                Column('CACCNAME', Text),
                Column('SNDNODE', Text),
                Column('CURTYPE', Text),
                Column('TRNMONEY', Float(asdecimal=True)),
                Column('SUCTRNMONEY', Float(asdecimal=True)),
                Column('ACTTRNMONEY', Float(asdecimal=True)),
                Column('FEEMONEY', Float(asdecimal=True)),
                Column('RETURNEDMONEY', Float(asdecimal=True)),
                Column('RETURNEDFEEMONEY', Float(asdecimal=True)),
                Column('ORGCHLCODE', Text),
                Column('DESTCHLCODE', Text),
                Column('PAYAGENTBANKNO', BigInteger),
                Column('PAYBANKNO', BigInteger),
                Column('PAYACCBANKNO', Float(asdecimal=True)),
                Column('PAYACCNO', Text),
                Column('PAYACCNAME', Text),
                Column('PAYACCADDR', Float(asdecimal=True)),
                Column('ACCTTYPE', BigInteger),
                Column('ACPAGENTBANKNO', BigInteger),
                Column('ACPBANKNO', BigInteger),
                Column('ACPACCBANKNO', Float(asdecimal=True)),
                Column('ACPACCNO', Text),
                Column('ACPACCNAME', Text),
                Column('ACPACCADDR', Float(asdecimal=True)),
                Column('TRANCHANNEL', BigInteger),
                Column('FSTAUTHNO', Float(asdecimal=True)),
                Column('MOBILENO', Float(asdecimal=True)),
                Column('VERCODEID', BigInteger),
                Column('VERCODE', Float(asdecimal=True)),
                Column('MERCHANTNO', Text),
                Column('SUBMERCHANTNO', Text),
                Column('MERCHANTNAME', Text),
                Column('APPLYTYPECODE', BigInteger),
                Column('PURTYPECODE', BigInteger),
                Column('PURNOTE', Text),
                Column('SIGNNO', Text),
                Column('CERTTYPE', Float(asdecimal=True)),
                Column('CERTNO', Text),
                Column('USERIP', Float(asdecimal=True)),
                Column('ATCMEMO', Text),
                Column('RETATCMEMO', Text),
                Column('PSTATUS', BigInteger),
                Column('PDTIME', Text),
                Column('RETCODE', Text),
                Column('RETDESC', Text),
                Column('SETFLAG', BigInteger),
                Column('SETTYPE', BigInteger),
                Column('SETWRKDATE', Float(asdecimal=True)),
                Column('NETNO', BigInteger),
                Column('CHKDATE', BigInteger),
                Column('CHKNETNO', BigInteger),
                Column('INTERESTFLAG', BigInteger),
                Column('BEGDATE', Float(asdecimal=True)),
                Column('ENDDATE', Float(asdecimal=True)),
                Column('CANCELFLAG', BigInteger),
                Column('RETURNNUM', BigInteger),
                Column('TRNREVERSEFLAG', BigInteger),
                Column('TRNBACKFLAG', BigInteger),
                Column('DBTRFUNDFLAG', BigInteger),
                Column('PLYFUNDFLAG', BigInteger),
                Column('OVERAGEFLAG', BigInteger),
                Column('ACCTNAMEFLAG', BigInteger),
                Column('SAMENAMEFLAG', BigInteger),
                Column('VALIDITYFLAG', BigInteger),
                Column('AUTHENTIFLAG', BigInteger),
                Column('CVNFLAG', BigInteger),
                Column('CHECKPASSFLAG', BigInteger),
                Column('FEEFLAG', BigInteger),
                Column('ULMONEY', Float(asdecimal=True)),
                Column('CHLFEE', Float(asdecimal=True)),
                Column('CHLSENDFEE', Float(asdecimal=True)),
                Column('CHLRECVFEE', Float(asdecimal=True)),
                Column('PAYFEE', Float(asdecimal=True)),
                Column('SENDFEE', Float(asdecimal=True)),
                Column('RECVFEE', Float(asdecimal=True)),
                Column('CENTERFEE', Float(asdecimal=True)),
                Column('SENDDIVFEE', Float(asdecimal=True)),
                Column('RECVDIVFEE', Float(asdecimal=True)),
                Column('CENTERDIVFEE', Float(asdecimal=True)),
                Column('CHLSENDFEE_BK', Float(asdecimal=True)),
                Column('CHLRECVFEE_BK', Float(asdecimal=True)),
                Column('SENDFEE_BK', Float(asdecimal=True)),
                Column('RECVFEE_BK', Float(asdecimal=True)),
                Column('CENTERFEE_BK', Float(asdecimal=True)),
                Column('SENDDIVFEE_BK', Float(asdecimal=True)),
                Column('RECVDIVFEE_BK', Float(asdecimal=True)),
                Column('CENTERDIVFEE_BK', Float(asdecimal=True)),
                Column('RETURNEDCHKDATE', Float(asdecimal=True))
            )

class Merchant(dbAPI):

    def __init__(self, db_name='pingan', table_name='hangzhou_swap_code_trade'):
        super(Merchant, self).__init__(db_name)
        self.table_struct = None
        if table_name is not None:
            self.table_name = table_name
            self.table_struct = Table(
                table_name, self.meta,
                Column('index', Integer, primary_key=True),
                Column('mer_code', String(16)),
                Column('turnover', Float),
                Column('trd_count', Integer),
                Column('settle_type', Integer),
                Column('settle_flag', Integer),
                Column('proxy_code', String(16)),
                Column('mer_name', String(128)),
                Column('branch_name', String(128)),
                Column('detail_addr', String(128)),
                Column('operation_id', String(32)),
                Column('open_bank', String(64)),
                Column('account_number', String(32)),
                Column('account_name', String(64)),
                Column('account_type', Integer),
                Column('account_status', Integer),
                Column('operation_number', Integer),
                Column('proxy_number', String(32)),
                Column('status', Integer),
                Column('proxy_level', Integer),
                Column('parent_proxy', String(16)),
                Column('first_level', String(16)),
                Column('second_level', String(16)),
                Column('third_level', String(16)),
                Column('fourth_level', String(16)),
                Column('fifth_level', String(16))
            )

if __name__ == '__main__':
    
    trade = Trade()
    print trade.get_row_counts()
    
    trade2 = Merchant()
    print trade2.get_row_counts()
    