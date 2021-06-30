# -*- encoding: utf-8 -*-

# 深圳证券交易所创业板审核项目爬虫

import requests, time, datetime, random, os, pickle, json, re
from urllib.parse import urlencode
from utils import save_pickle, load_pickle
from data_preprocessing import data_process
from html_generate import gen_html

# default path
cyb_p = 'F:/GFS intern/spider/创业板/'
rawDataPath = "F:/GFS intern/spider/"
savedDataPath = 'F:/GFS intern/spider/saved_data/'

# 深圳创业板爬虫
class SZcybSpider():
    
    headers = {
        'User-Agent':
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.141 Safari/537.36',
        }
    
    def __init__(self,cyb_p=cyb_p,rawDataPath=rawDataPath,savedDataPath=savedDataPath):
        '''
        cyb_p: 创业板项目数据及网页保存路径
        rawDataPath: 项目根目录，原始数据将在里面保存，用于数据处理与网页生成的函数
        savedDataPath: 最终总数据的保存路径
        '''
        self.cyb_path = cyb_p
        self.raw_data_save_path = rawDataPath
        self.saved_data_path = savedDataPath

    
    # 获取所有项目的索引
    def index_getter(self, projtype='ipo',pageSize=2000):
        '''
        获取目录页
        pageSize 应当要大于项目总数，这样才能一次性获取全部索引
        '''
        path = self.saved_data_path
        print('开始获取文件索引！')
        if projtype == 'ipo':
            biztype = 1
        elif projtype == 'refinance':
            biztype = 2
        elif projtype == 'reproperty':
            biztype = 3
        else:
            print("Input error! Please choose the correct type of data")
            return

        # pageSize 应当要大于项目总数，这样才能一次性获取全部索引
        params = {
            'bizType': biztype,
            'random': random.random(),
            'pageIndex': 0,
            'pageSize': pageSize
        }
        base_url = 'http://listing.szse.cn/api/ras/projectrends/query?'
        projList_url = base_url + urlencode(params)

        r = requests.get(projList_url, headers=self.headers)
        index_list = json.loads(r.text)
        if not os.path.exists(path):
            os.makedirs(path)
        save_pickle(index_list['data'], path + 'szcyb_index.pkl')
        print('文件索引获取完成！')
        
        self.index_list = index_list['data']
        return index_list['data']
    
    # 获取并保存单个项目详细信息
    def data_getter(self, prjid):
        '''
        获取单个项目详细信息
        '''
        path = self.cyb_path
        base_url = 'http://listing.szse.cn/api/ras/projectrends/details?id='
        stock_url = base_url + str(prjid)
        r = requests.get(stock_url, headers=self.headers)
        stockInfo = json.loads(r.text)['data']

        directory = path + stockInfo['cmpnm'].strip()
        if not os.path.exists(directory):
            os.makedirs(directory)
        save_pickle(stockInfo, directory + '/' + 'szcyb_info.pkl')
        return stockInfo
    
    # 获取问询与回复文件 
    def file_getter(self, stockInfo):
        '''
        获取各个项目所有文件，保存在同名文件夹中
        '''
        base_path = self.cyb_path
        directory = base_path + stockInfo['cmpnm'].strip()
        if not os.path.exists(directory):
            os.makedirs(directory)
        response = stockInfo['enquiryResponseAttachment']
        #disclosure = stockInfo['disclosureMaterials']
        base_url = 'http://reportdocs.static.szse.cn'
        totalNum = len(response)
        index = 1
        if totalNum>0:
            print("开始下载！")
            for prj in response:
                filePath = prj['dfpth']
                filename = directory + '/' + prj['dfnm']
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
                 
    # 汇总所有项目信息
    def get_allStockInfo(self):
        '''
        汇集所有项目信息
        '''
        savepath = self.saved_data_path
        cyb_path = self.cyb_path
        
        listOfFiles = list()
        for (dirpath, dirnames, filenames) in os.walk(cyb_path):
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
        saved_path = savepath + 'szcyb_stocksInfo.pkl'
        save_pickle(stocksInfo, saved_path)
        return stocksInfo    
      
    # 主程序，检查是否有项目更新，增量更新
    def szcyb_check_update(self, fileDownload=True,prjtype = 'ipo'):
        '''
        主程序，检查是否有项目更新，增量更新
        '''
        raw_data_save_path = self.raw_data_save_path
        saved_data_path = self.saved_data_path
        try:
            #proj_list_old = load_pickle(saved_data_path + 'szcyb_index.pkl')
            proj_list_new = self.index_getter(prjtype)
            stocksInfo = load_pickle(saved_data_path + 'szcyb_stocksInfo.pkl')
            updated_idx = [
                index for (index, d) in enumerate(proj_list_new)
                if d["updtdt"] == datetime.date.today().strftime('%Y-%m-%d')
            ]
            if updated_idx == []:
                print("创业板没有可更新的项目。。。")
                return
            else:
                print("一共有 {} 项目存在更新，正在更新。。。".format(len(updated_idx)))
                for idx in updated_idx:
                    raw_data = self.data_getter(proj_list_new[idx]['prjid'])
                    cleaned_data = data_process(raw_data, raw_data_save_path)
                    print('项目:', cleaned_data['baseInfo']['cmpName'],'已完成更新')
                    # 下载文件
                    if fileDownload:
                        self.file_getter(raw_data)
                    gen_html(cleaned_data, raw_data_save_path,'sz')
                    time.sleep(random.random())
                    #new_idx = next((index for (index, d) in enumerate(stocksInfo) if d["baseInfo"]['cmpName'] == proj_list_new[idx]['cmpnm']), None)
                    #stocksInfo[idx] = cleaned_data

                #save_pickle(stocksInfo, saved_data_path + 'szcyb_stocksInfo.pkl')
                self.get_allStockInfo()
                print('更新完成')
                return

        except FileNotFoundError:
            print('本地无数据，开始重新下载所有深圳创业板数据！')
            proj_list = self.index_getter(prjtype)
            print('一共有 {} 个项目'.format(len(proj_list)))
            i = 0
            for proj in proj_list:
                i += 1
                print('fetching project {}, {}'.format(proj['cmpsnm'],i) + '/' + str(len(proj_list))) 
                #print('正在获取第 {} 个项目： {}'.format(i, proj['cmpsnm']))
                stockInfo = self.data_getter(proj['prjid'])
                cleaned_data = data_process(stockInfo, raw_data_save_path)
                gen_html(cleaned_data, raw_data_save_path,'sz')
                if fileDownload:
                    self.file_getter(stockInfo)
                time.sleep(random.random())
            self.get_allStockInfo()
            print('更新完成')
            return
        
if __name__ == '__main__':
    szcyb = SZcybSpider()
    szcyb.szcyb_check_update(fileDownload=False)