# -*- encoding: utf-8 -*-

# 上海证券交易所科创板审核项目爬虫

import requests, time, datetime, random, os, pickle, json, re
from urllib.parse import urlencode
from utils import save_pickle, load_pickle
from datetime import datetime
from data_preprocessing import data_process
from html_generate import gen_html
import math

# default path
savedDataPath = 'F:/GFS intern/JinChao_Data/saved_data/'
kcb_p = 'F:/GFS intern/JinChao_Data/科创板/'
rawDataPath = "F:/GFS intern/JinChao_Data/"

class SHkcbSpider():
    
    def __init__(self,kcb_p=kcb_p,rawDataPath=rawDataPath,savedDataPath=savedDataPath):
        '''
        kcb_p: 科创板项目数据及网页保存路径
        rawDataPath: 项目根目录，原始数据将在里面保存，用于数据处理与网页生成的函数
        savedDataPath: 最终总数据的保存路径
        '''
        self.kcb_path = kcb_p
        self.raw_data_save_path = rawDataPath
        self.saved_data_path = savedDataPath
    
    # Parameters
    IPO_sqlId = {
        'info': 'SH_XM_LB',
        'release': 'GP_GPZCZ_SHXXPL',
        'status': 'GP_GPZCZ_XMDTZTTLB',
        'result': 'GP_GPZCZ_SSWHYGGJG',
        'res': 'GP_GPZCZ_XMDTZTYYLB'}
    refinance_sqlId = {
        'info': 'GP_BGCZ_XMLB',
        'release': 'GP_BGCZ_SSWHYGGJG',
        'status': 'GP_BGCZ_XMDTZTTLB'}
    reproperty_sqlId = {
        'status': 'GP_ZRZ_XMDTZTTLB',
        'release': 'GP_ZRZ_GGJG',
        'result': '',
        'res': 'GP_ZRZ_XMZTYYLB',
        'info': 'GP_ZRZ_XMLB'}
    headers = {
        'User-Agent':
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.141 Safari/537.36',
        'referer': 'https://kcb.sse.com.cn/',
        'Accept': '*/*',
        'Accept-Encoding': 'gzip, deflate',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'Connection': 'keep-alive',
        #'Cookie': r'yfx_c_g_u_id_10000042=_ck21061010480311765584653457041; VISITED_MENU=%5B%228350%22%2C%2212178%22%2C%228307%22%5D; JSESSIONID=7F8F9548AA539A4115A746F5919E9BF4; yfx_f_l_v_t_10000042=f_t_1623293283165__r_t_1624934442852__v_t_1624940605101__r_c_6',
        'Host': 'query.sse.com.cn'}
    
    # 生成获取项目信息的url
    def url_parser(self,stockAuditNum, sqltype, projtype='ipo'):
        '''
        解析url
        '''
        base_url = 'https://query.sse.com.cn/'
        if projtype == 'ipo':
            sqlId = self.IPO_sqlId[sqltype]
        elif projtype == 'refinance':
            sqlId = self.refinance_sqlId[sqltype]

        elif projtype == 'reproperty':
            sqlId = self.reproperty_sqlId[sqltype]
        if sqlId == '':
            return ''
        else:
            jsonpCallback = str(math.floor(random.random() * (100000000 + 1)))
            url = base_url + 'commonSoaQuery.do?' + 'jsonCallBack=jsonpCallback' +jsonpCallback+ '&sqlId=' + sqlId + '&stockAuditNum=' + stockAuditNum
            return url
        
    # 解析返回的json文件
    def jsonp_parser(self,jsonp_str):
        '''
        解析返回的js文件
        '''
        try:
            return re.search('^[^(]*?\((.*)\)[^)]*$', jsonp_str).group(1)
        except:
            raise ValueError('Invalid JSONP')    
        
    # 获取单个项目信息
    def data_getter(self, stockAuditNum, projtype='ipo'):
        '''
        获取单个项目详细信息
        '''
        base_path = self.kcb_path
        stock_allInfo = {}
        for sqltype in ['info', 'status', 'release', 'result', 'res']:

            url = self.url_parser(stockAuditNum, sqltype, projtype)
            if url != '':
                rs = requests.get(url, headers = self.headers)
                raw_data = self.jsonp_parser(rs.text)
                data = json.loads(raw_data)['pageHelp']['data']
                stock_allInfo[sqltype] = data
            else:
                stock_allInfo[sqltype] = ''
        directory = base_path  + stock_allInfo['info'][0]['stockIssuer'][0]['s_issueCompanyFullName'].strip()
        
        if not os.path.exists(directory):
            os.makedirs(directory)
        save_pickle(stock_allInfo, directory + '/' + 'shkcb_info.pkl')
        return stock_allInfo
    
    # 将单个项目问询和回复文件下载到同名文件夹中
    def file_getter(self, stockInfo):
        '''
        将项目问询和回复文件下载到同名文件夹中
        fileType == 5 or 6 是问询和回复
        '''
        base_path = self.kcb_path
        directory = base_path + stockInfo['info'][0]['stockAuditName'].strip()
        if not os.path.exists(directory):
            os.makedirs(directory)
        base_url = 'http://static.sse.com.cn/stock'
        response = stockInfo['release']
        resp = [x for x in response if x['fileType'] == 5 or x['fileType'] == 6]
        totalNum = len(resp)
        index = 1
        if totalNum>0:
            print("开始下载！")
            for prj in resp:
                filePath = prj['filePath']
                filename = directory + '/' + prj['fileTitle'] +'.pdf'
                download_url = base_url + filePath
                
                """下载文件并显示过程
                """
                file_to_save = filename

                with open(file_to_save, "wb") as fw:
                    with requests.get(download_url, stream=True,verify=False) as r:
                        chunk_size = 512
                        count = 0
                        ContentLength = float(r.headers['Content-Length'])
                        for chunk in r.iter_content(chunk_size):
                            count += len(chunk)
                            fw.write(chunk)
                            print("\r下载文件: "+str(index)+'/'+str(totalNum), "下载进度:",'{:.2f}'.format(count/1024/1024)+"M/"+'{:.2f}'.format(float(ContentLength)/1024/1024)+"M",flush=True,end='')
                
                index += 1 
                time.sleep(random.random())
            print("\n结束下载")
            
            

        
        return
           
    # 获取所有项目的索引
    def index_getter(self, projtype='ipo',pageSize=2000):
        '''
        获取目录页
        '''
        saved_data_path = self.saved_data_path
        jsonCallBack_random = str(math.floor(random.random() * (100000 + 1)))
        print('开始获取文件索引！')
        if projtype == 'ipo':
            # pageSize 应当要大于项目总数，这样才能一次性获取全部索引
            #index_url = 'https://query.sse.com.cn/statusAction.do?jsonCallBack=jsonpCallback47570&isPagination=true&sqlId=SH_XM_LB&pageHelp.pageSize=2000&offerType=&commitiResult=&registeResult=&csrcCode=&currStatus=&order=updateDate%7Cdesc&keyword=&auditApplyDateBegin=&auditApplyDateEnd=&_=1611139628341'
            #index_url = 'http://query.sse.com.cn/statusAction.do?jsonCallBack=jsonpCallback77861&isPagination=true&sqlId=SH_XM_LB&pageHelp.pageSize=2000&offerType=&commitiResult=&registeResult=&csrcCode=&currStatus=&order=updateDate%7Cdesc&keyword=&auditApplyDateBegin=&auditApplyDateEnd=&_=1624941895605'
            index_url = 'http://query.sse.com.cn/statusAction.do?jsonCallBack=jsonpCallback'+jsonCallBack_random+'&isPagination=true&sqlId=SH_XM_LB&pageHelp.pageSize='+str(pageSize)+'&offerType=&commitiResult=&registeResult=&csrcCode=&currStatus=&order=updateDate%7Cdesc&keyword=&auditApplyDateBegin=&auditApplyDateEnd=&_=1624941895605'
        elif projtype == 'refinance':
            index_url = 'https://query.sse.com.cn/bgczStatusAction.do?jsonCallBack=jsonpCallback52431&isPagination=true&sqlId=GP_BGCZ_XMLB&pageHelp.pageSize=1000&offerType=&commitiResult=&registeResult=&csrcCode=&currStatus=&order=updateDate%7Cdesc&keyword=&auditApplyDateBegin=&auditApplyDateEnd=&_=1611139628341'
        elif projtype == 'reproperty':
            index_url = 'https://query.sse.com.cn/zrzStatusAction.do?jsonCallBack=jsonpCallback89527&isPagination=true&sqlId=GP_ZRZ_XMLB&pageHelp.pageSize=1000&offerType=&commitiResult=&registeResult=&csrcCode=&currStatus=&order=updateDate%7Cdesc&keyword=&auditApplyDateBegin=&auditApplyDateEnd=&_=1611139628341'

        r = requests.get(index_url, headers=self.headers)

        js = self.jsonp_parser(r.text)
        index_list = json.loads(js)['result']
        if not os.path.exists(saved_data_path):
            os.makedirs(saved_data_path)
        save_pickle(index_list, saved_data_path + 'shkcb_index.pkl')
        print('文件索引获取完成！')
        return index_list

    # 主函数，检查更新，若无数据则重新下载
    def shkcb_check_update(self,fileDownload=True):
        '''
        主函数，先判断有无更新，有新增项目则增量更新
        判断逻辑：每日获取最新索引，若有更新日期是今天的，就更新那些数据
        '''
        prjtype = 'ipo'
        raw_data_save_path = self.raw_data_save_path
        saved_data_path = self.saved_data_path
        try:
            stocksInfo = load_pickle(saved_data_path + 'shkcb_stocksInfo.pkl')
            proj_list_new = self.index_getter(prjtype)
            updated_idx = [index for (index, d) in enumerate(proj_list_new) if datetime.strptime(d['updateDate'], '%Y%m%d%H%M%S').date() ==
                        datetime.today().date()]
            if updated_idx == []:
                print("科创板没有可更新的数据。。。")
                return
            else:
                print("一共有 {} 项目存在更新，正在更新。。。".format(len(updated_idx)))
                for idx in updated_idx:
                    raw_data = self.data_getter(proj_list_new[idx]['stockAuditNum'])
                    cleaned_data = data_process(raw_data, raw_data_save_path)
                    print('项目:', cleaned_data['baseInfo']['cmpName'],
                        '已完成更新')
                    gen_html(cleaned_data, raw_data_save_path,'sh')
                    if fileDownload:
                        self.file_getter(raw_data)
                    time.sleep(random.random())
                    #new_idx = next((index for (index, d) in enumerate(stocksInfo) if d["baseInfo"]['cmpName'] == proj_list_new[idx]['cmpName']), None)
                    #stocksInfo[idx] = cleaned_data

                #save_pickle(stocksInfo, saved_data_path + 'shkcb_stocksInfo.pkl')
                self.get_allStockInfo()
                print('更新完成！')
                return
            
        except FileNotFoundError:
            # 若找不到本地股票信息文件，则重新下载
            proj_list = self.index_getter(prjtype)
            #proj_list = load_pickle(saved_data_path + 'shkcb_index.pkl')
            # print('there are total {} stocks in the list'.format(len(proj_list)))
            i = 0
            print('本地无数据，开始重新下载所有上海科创板数据！')
            for proj in proj_list:
                i += 1
                print('fetching project {}, {}'.format(proj['stockAuditName'],i) + '/' + str(len(proj_list))) 
                stockAuditNum = proj['stockAuditNum']
                raw_data = self.data_getter(stockAuditNum)
                cleaned_data = data_process(raw_data, raw_data_save_path)
                print('项目:', cleaned_data['baseInfo']['cmpName'],
                    '已完成更新')
                gen_html(cleaned_data, raw_data_save_path,'sh')
                if fileDownload:
                    self.file_getter(raw_data)
                time.sleep(random.random())
            self.get_allStockInfo()
            print('更新完成！')
            return
    
    # 汇总所有数据
    def get_allStockInfo(self):
        '''
        汇总所有项目信息
        '''
        savepath = self.saved_data_path
        kcb_path = self.kcb_path
        listOfFiles = list()
        for (dirpath, dirnames, filenames) in os.walk(kcb_path):
            listOfFiles += [os.path.join(dirpath, file) for file in filenames]
        stocksInfo = []
        for i in listOfFiles:
            if os.path.basename(i) == 'clean_info.pkl':
                # print('clean up company:', os.path.dirname(i))
                # raw_data = load_pickle(i)
                # cleaned_data = data_process(raw_data)
                clean_data = load_pickle(i)
                stocksInfo.append(clean_data)
        # to_dataframe(allStock_info)
        if not os.path.exists(savepath):
            os.makedirs(savepath)
        saved_path = savepath + 'shkcb_stocksInfo.pkl'
        save_pickle(stocksInfo, saved_path)
        return

if __name__ == '__main__':
    kcb = SHkcbSpider()
    kcb.shkcb_check_update()