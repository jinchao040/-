# -*- encoding: utf-8 -*-
from os import error
from shkcb_class import SHkcbSpider
from szcyb_class import SZcybSpider
from statistic import statistic_df
import pickle
import pandas as pd

# IPO爬虫汇总
cyb_path = 'F:/GFS intern/spider/创业板/'
rawDataPath = "F:/GFS intern/spider/"
savedDataPath = 'F:/GFS intern/spider/saved_data/'
kcb_path = 'F:/GFS intern/spider/科创板/'

class spider():
    # type:kcb,cyb,sh,sz
    def __init__(self,type_,Filepath,rawDataPath,savedDataPath):
        self.type_ = type_
        if type_ == 'kcb':
            self.spider = SHkcbSpider(Filepath,rawDataPath,savedDataPath)
            self.update = self.spider.shkcb_check_update
        elif type_ == 'cyb':
            self.spider = SZcybSpider(Filepath,rawDataPath,savedDataPath)
            self.update = self.spider.szcyb_check_update
        else:
            error('该类爬虫暂未实现')
    
    # 将科创板和创业板数据汇总，得到DataFrame
    @staticmethod
    def getDataFrame(path=savedDataPath):
        '''
        baseInfo = {'projID':'',            # 项目id
        'cmpName':'',                       # 公司名称
        'cmpAbbrname':'',                   # 公司简称
        'projType':'',                      # 项目类型
        'phase': '',                        # 审核阶段
        'currStatus':'',                    # 审核状态
        'region':'',                        # 所属地区
        'csrcCode':'',                      # 所属行业
        'acptDate':'',                      # 受理日期
        'updtDate':'',                      # 更新日期
        'issueAmount':'',                   # 融资金额
        'sprInst':'',                       # 保荐机构
        'sprInstAbbr':'',                   # 保荐机构简称
        'sprRep':'',                        # 保荐代表人
        'acctFirm':'',                      # 会计师事务所
        'acctSgnt':'',                      # 签字会计师
        'lawFirm':'',                       # 律师事务所
        'lglSgnt':'',                       # 签字律师
        'evalSgnt':'',                      # 签字评估师
        'evalInst':'',                      # 评估机构
        'projMilestone':[projMilestone],    # 项目里程碑
        'auditMarket':''}                   # 所属交易所
        '''
        
        data1_path = path + 'shkcb_stocksInfo.pkl'
        data2_path = path + 'szcyb_stocksInfo.pkl'    
        with open(data1_path,'rb') as f1:
            data1 = pickle.load(f1)
        
        with open(data2_path,'rb') as f2:
            data2 = pickle.load(f2)
            
        rows = []
        for i in data1:
            dd = i['baseInfo']
            rows.append(dd)

        for i in data2:
            dd = i['baseInfo']
            rows.append(dd)
            
        df = pd.DataFrame(rows)
        spider.df = df
        return df
   
    # 获取统计数据
    @staticmethod
    def stats(start_date,end_date):
        df = spider.df
        spider.stats_ = statistic_df(df,start_date,end_date)
        return spider.stats_
 
if __name__ == '__main__':    
    KCBspider = spider('kcb',kcb_path,rawDataPath,savedDataPath)
    KCBspider.update(fileDownload=False)
    
    CYBspider = spider('cyb',kcb_path,rawDataPath,savedDataPath)
    CYBspider.update(fileDownload=False)
    
    # CYBspider.getDataFrame()
    # start_date = '2019-01-01'
    # end_date = '2021-12-31'
    # CYBspider.stats(start_date,end_date)
    # a = CYBspider.stats()
    # a.inst_stat()