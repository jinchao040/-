import pickle
import pandas as pd
import os 

def load_pickle(path_to_file):
    with open(path_to_file, 'rb') as file:
        data = pickle.load(file)
    return data

def save_pickle(obj, path_to_file):
    with open(path_to_file, 'wb') as f:
        pickle.dump(obj, f, pickle.HIGHEST_PROTOCOL)

def to_dataframe(path_to_StocksInfo):
    dd = []
    # directory = os.getcwd()+'/' + 'allStock_info.pkl'
    allStock_info = load_pickle(path_to_StocksInfo)
    idx = 0
    for stock in allStock_info:
        idx += 1
        k = stock['baseInfo']
        temp = [idx,k['auditMarket'],k['cmpName'],k['currStatus'],k['region'],k['csrcCode'],k['sprInst'],k['acctFirm'],\
                k['lawFirm'],k['updtDate'],k['acptDate']]
        dd.append(temp)
    df = pd.DataFrame(dd,columns=['序号','交易所','发行人全称','审核状态','注册地','行业','保荐机构','会计师事务所',\
                '律师事务所','更新日期','受理日期'],)
    df.to_csv(allStock_info[0]['prjType']+'.csv', encoding='utf_8_sig',index=False)

def to_html(path_to_StocksInfo):
    dd = []
    # directory = os.getcwd()+'/' + 'allStock_info.pkl'
    allStock_info = load_pickle(path_to_StocksInfo)
    for stock in allStock_info:
        k = stock['baseInfo']
        temp = [k['auditMarket'],k['cmpName'],k['currStatus'],k['region'],k['csrcCode'],k['sprInst'],k['acctFirm'],\
                k['lawFirm'],k['updtDate'],k['acptDate']]
        dd.append(temp)
    df = pd.DataFrame(dd,columns=['交易所','发行人全称','审核状态','注册地','行业','保荐机构','会计师事务所',\
                '律师事务所','更新日期','受理日期'],)
    df["发行人全称"] = df["发行人全称"].apply( # insert links
            lambda x: "<a href='{}'>{}</a>".format('./'+x+'/'+x+'.html',x))
    pd.set_option('display.max_colwidth',-1)
    df.sort_values('更新日期',ascending=False)
    df.to_html(os.getcwd()+'/'+allStock_info[0]['prjType']+'_index.html',border=1,render_links=True,escape=False,index=True)