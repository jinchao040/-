import pandas as pd
import numpy as np
from utils import load_pickle,save_pickle
import matplotlib.pyplot as plt


# 统计数据以及数据可视化
class statistic_df:
    
    # data 是一个DataFrame  
    def __init__(self,data,start_date,end_date):
        df = data
        df = df[df['updtDate']>=start_date]
        df = df[df['updtDate']<=end_date]  
        self.data = df
        self.start_date = start_date
        self.end_date = end_date
    
    # 板块汇总  板块-审核状态
    def status_stat(self):
        df = self.data
        # 新表字段：审核阶段、审核状态、沪主板、深主板、创业板、科创板
        status  = pd.DataFrame(columns=['phase','status','沪主板','深主板','创业板','科创板']) 
        df['status'] = df['currStatus'].replace('新受理','已受理')
        df['status'] = df['status'].replace('中止（财报更新）','中止')
        df['status'] = df['status'].replace('终止(撤回)','终止')
        df['status'] = df['status'].replace('终止(审核不通过)','终止')
        g = df.groupby(by=['phase','status'])
        status['phase'] = g.apply(lambda x: max(x['phase']))
        status['status'] = g.apply(lambda x: max(x['status']))
        status['科创板'] = g.apply(lambda x: x['auditMarket'].apply(lambda x:1 if x =='科创板' else 0).sum())
        status['创业板'] = g.apply(lambda x: x['auditMarket'].apply(lambda x:1 if x =='创业板' else 0).sum())
        status['沪主板'] = g.apply(lambda x: x['auditMarket'].apply(lambda x:1 if x =='沪主板' else 0).sum())
        status['深主板'] = g.apply(lambda x: x['auditMarket'].apply(lambda x:1 if x =='深主板' else 0).sum())
        status.index = range(len(status))
        status.fillna(0,inplace=True)
        status['total'] = status.apply(lambda x:x['创业板']+x['科创板']+x['沪主板']+x['深主板'],axis=1)
        status['phase'] = status['phase'].astype('category')
        order = ['已受理','已问询','上市委会议','提交注册','注册结果','中止','终止']
        status['phase'].cat.reorder_categories(order, inplace=True)
        
        status.sort_values('phase', inplace=True)
        
        return status     
                                    
    # 地区汇总
    def region_stat(self):
        df = self.data
        # 新表字段：地区、已受理、已问询、上市委会议、提交注册、注册结果、中止、终止
        status  = pd.DataFrame(columns=['region','已受理','已问询','上市委会议','提交注册','注册结果','中止','终止']) 
        g = df.groupby(by=['region'])
        status['region'] = g.apply(lambda x: max(x['region']))
     
        status['已受理'] = g.apply(lambda x: x['phase'].apply(lambda x:1 if x =='已受理' else 0).sum())
        status['已问询'] = g.apply(lambda x: x['phase'].apply(lambda x:1 if x =='已问询' else 0).sum())
        status['上市委会议'] = g.apply(lambda x: x['phase'].apply(lambda x:1 if x =='上市委会议' else 0).sum())
        status['提交注册'] = g.apply(lambda x: x['phase'].apply(lambda x:1 if x =='提交注册' else 0).sum())
        status['注册结果'] = g.apply(lambda x: x['phase'].apply(lambda x:1 if x =='注册结果' else 0).sum())
        status['中止'] = g.apply(lambda x: x['phase'].apply(lambda x:1 if x =='中止' else 0).sum())
        status['终止'] = g.apply(lambda x: x['phase'].apply(lambda x:1 if x =='终止' else 0).sum())
        
        a = g.apply(lambda x: x['region'].count())
        status['total'] = a
        status.fillna(0,inplace=True)
        status.sort_values('total',ascending=False,inplace=True)
        status.index = range(len(status))
        return status   

    # 中介机构汇总 
    # inst_name = ['主保荐机构','律师事务所','会计师事务所','联合保荐机构','保荐机构']
    def inst_stat(self, inst_name = '保荐机构', is_abbr = False):
        df = self.data
        if inst_name in ['主保荐机构','律师事务所','会计师事务所','联合保荐机构','保荐机构']:
            if inst_name == '保荐机构':
                if is_abbr is False:
                    df1 = self.inst_stat('主保荐机构')
                    df2 = self.inst_stat('联合保荐机构')
                else:
                    df1 = self.inst_stat('主保荐机构',is_abbr=True)
                    df2 = self.inst_stat('联合保荐机构',is_abbr=True) 
                    
                df1['key'] = df1['主保荐机构']
                df2['key'] = df2['联合保荐机构']
                columns=['key','创业板','科创板','深主板','沪主板','total']
                df = pd.merge(df1[columns],df2[columns],on='key',how='outer')
                df.fillna(0,inplace=True)
                df['保荐机构'] = df['key']
                df['创业板'] = (df['创业板_x'] + 0.5*df['创业板_y']).astype(float)
                df['科创板'] = (df['科创板_x'] + 0.5*df['科创板_y']).astype(float)
                df['深主板'] = (df['深主板_x'] + 0.5*df['深主板_y']).astype(float)
                df['沪主板'] = (df['沪主板_x'] + 0.5*df['沪主板_y']).astype(float)
                df['total'] = (df['total_x'] + 0.5*df['total_y']).astype(float)
                columns=['保荐机构','创业板','科创板','深主板','沪主板','total']
                df = df[columns]
                df.index = range(len(df))
                return df
        
            elif inst_name == '主保荐机构':
                if is_abbr is False:
                    dictt = 'sprInst'
                else:
                    dictt = 'sprInstAbbr'
            elif inst_name == '律师事务所':
                dictt = 'lawFirm'
            elif inst_name == '会计师事务所':
                dictt = 'acctFirm'
            elif inst_name == '联合保荐机构':
                dictt = 'jsprInst'   

                
            # 新表字段：inst_name、创业板、科创板、深主板、沪主板
            status  = pd.DataFrame(columns=[inst_name,'创业板','科创板','深主板','沪主板']) 
            df = df.replace('',np.nan)
            df = df.dropna(axis=0,subset=[dictt])
            g = df.groupby(by=[dictt])
            
            status[inst_name] = g.apply(lambda x: max(x[dictt]))
            
        
            status['创业板'] = g.apply(lambda x: x['auditMarket'].apply(lambda x:1 if x =='创业板' else 0).sum())
            status['科创板'] = g.apply(lambda x: x['auditMarket'].apply(lambda x:1 if x =='科创板' else 0).sum())
            status['深主板'] = g.apply(lambda x: x['auditMarket'].apply(lambda x:1 if x =='深主板' else 0).sum())
            status['沪主板'] = g.apply(lambda x: x['auditMarket'].apply(lambda x:1 if x =='沪主板' else 0).sum())

            
            a = g.apply(lambda x: x[dictt].count())
            status['total'] = a
            status.index = range(len(status))
            status.fillna(0,inplace=True)
            status.sort_values('total',ascending=False,inplace=True)
            status.index = range(len(status.index))
            return status  
        else:
            return 1 
    
    # 行业分布柱状图
    def industry_dist_bar(self, savepath = ''):
        df= self.data
        start_date = self.start_date
        end_date = self.end_date
        
        g = df.groupby('csrcCode')
        f = pd.DataFrame({})
        f['industries_name'] = g.apply(lambda x: max(x['csrcCode']))
        f['num'] = g.apply(lambda x: (x['projID'].count()))
        f = f.sort_values('num',ascending=False)
        # 若名字过长，则省略
        f['industries_name'] = f['industries_name'].apply(lambda x: x[0:4]+'...' if len(x)>4 else x)
        
        fig = plt.figure(figsize=(20,7))
        ax  = fig.add_subplot(111)
        ax.bar(x=f['industries_name'], height=f['num'], color='orange',width=0.5)
        ax.set_title('行业分布图 '+str(start_date)+' 至 '+str(end_date))
        ax.set_xlabel('行业')
        ax.set_ylabel('公司数量')
        plt.rcParams['font.sans-serif'] = ['SimHei']  # 解决中文显示问题-设置字体为黑体
        #ax.set_yticklabels(ax.get_yticklabels(), rotation=0)  #旋转坐标label
        ax.set_xticklabels(f['industries_name'],rotation=40) 
        # 在每个bar上增加数字
        for a,b in zip(f['industries_name'],f['num']):
            plt.text(a, b+0.001, '%d' % b, ha='center', va= 'bottom',fontsize=9)
        plt.savefig(savepath + '/Industries.png',bbox_inches='tight')
        plt.show()
    
    # 中介机构分布柱状图
    def inst_dist_bar(self, savepath = '',inst_name='保荐机构',num = 20):
        df = self.inst_stat(inst_name,True)
        start_date = self.start_date
        end_date = self.end_date
        
        f = df[[inst_name,'total']]
        f = f[0:num]
        f[inst_name] = f[inst_name].apply(lambda x: x[0:6]+'...' if len(x)>6 else x)
        
        fig = plt.figure(figsize=(20,10))
        ax  = fig.add_subplot(111)
        ax.bar(x=f[inst_name], height=f['total'], color='orange',width=0.5)
        ax.set_title(inst_name + '分布图 '+str(start_date)+' 至 '+str(end_date))
        ax.set_xlabel(inst_name)
        ax.set_ylabel('数量')
        plt.rcParams['font.sans-serif'] = ['SimHei']  # 解决中文显示问题-设置字体为黑体
        #ax.set_yticklabels(ax.get_yticklabels(), rotation=0)  #旋转坐标label
        ax.set_xticklabels(f[inst_name],rotation=40) 
        # 在每个bar上增加数字
        for a,b in zip(f[inst_name],f['total']):
            plt.text(a, b+0.001, '%d' % b, ha='center', va= 'bottom',fontsize=9)
        plt.savefig(savepath + '/'+inst_name+'.png',bbox_inches='tight')
        plt.show()
        
if __name__ == '__main__':
    df = pd.read_csv('aaaa.csv')
    start_date = '2019-01-01'
    end_date = '2021-12-31'
    df = statistic_df(df,start_date,end_date)
    #df.inst_dist_bar(savepath = './',inst_name = '保荐机构',num = 30)  
    print(df.status_stat())
