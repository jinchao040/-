# -*- encoding: utf-8 -*-

# 清洗数据的脚本，统一两个交易所数据格式
'''
统一数据格式如下： 

    dict ={'baseInfo':[baseInfo],                        # 项目基本信息
        'disclosureMaterial':[fileConfig],            # 信息披露
        'responseAttachment':[fileConfig],            # 问询与回复
        'meetingAttachment':[fileConfig],             # 上市委会议公告与结果
        'terminationAttachment':[fileConfig],         # 终止审核通知
        'registrationAttachment':[fileConfig],        # 注册结果通知
        'others':[others]}                            # 其他


    baseInfo = {'projID':'',                        # 项目id
                'cmpName':'',                       # 公司名称
                'cmpAbbrname':'',                   # 公司简称
                'projType':'',                      # 项目类型
                'phase':'',                         # 审核阶段
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


    projMilestone = {'1':{'status':'已受理',
                        'time':'',
                        'result':''},
                    '2':{'status':'已问询',
                            'result':'',
                        'time':''},
                    '3':{'status':'上市委会议',
                        'result':'',
                        'time':''},            
                    '4':{'status':'提交注册',
                        'result':'',
                        'time':''},            
                    '5':{'status':'注册结果',
                        'result':'',
                        'time':''},
                    '6':{'status':'暂缓审议',
                        'result':'',
                        'time':''},
                    '7':{'status':'中止及财报更新',
                        'result':'',
                        'time':''},
                    '8':{'status':'中止',
                        'result':'',
                        'time':''},
                    '9':{'status':'复审委会议',
                        'result':'',
                        'time':''},
                    '10':{'status':'补充审核',
                        'result':'',
                        'time':''}
                    }          

    fileConfig = {  'fname':'',                 #  文件名
                    'ftype':'',                 #  文件所属类型   
                    'fphynm':'',                #  文件唯一识别名
                    'ftitle':'',                #  文件标题
                    'fstatus':'',               #  文件所属环节
                    'fdate':'',                 #  文件日期
                    'fpath':'',                 #  文件地址
                    'fext':'',                  #  文件后缀
                    'other':''}                 #   (待选)

    others = {  'index':'',
            'reason':'',
            'date':'',
            'timestamp':''}
'''

import json
from utils import save_pickle
import os

# 主函数，用于判断输入数据属于哪个交易所，并调用相应处理函数
def data_process(raw_data, savepath):
    '''
    主函数，用于判断输入数据属于哪个交易所，并调用相应处理函数
    raw_data: 原始数据路径
    savepath: 生成的新数据保存路径
    '''
    if 'prjid' in raw_data:
        # this is from sz_exchange
        cleaned_data = sz_process(raw_data)
        directory = savepath + '创业板/' + cleaned_data['baseInfo']['cmpName'].strip() 
        if not os.path.exists(directory):
            os.makedirs(directory)
        save_pickle(cleaned_data, directory + '/' + 'clean_info.pkl')
    else:
        cleaned_data = sh_process(raw_data)
        directory = savepath + '科创板/' + cleaned_data['baseInfo']['cmpName'].strip()  
        if not os.path.exists(directory):
            os.makedirs(directory)
        save_pickle(cleaned_data, directory + '/' + 'clean_info.pkl')
    return cleaned_data

# 解析深圳审核状态编码
def sz_phase(raw_data):
    '''
    用于解析深圳创业板网页源代码
    判断项目所处阶段的函数
    '''
    status = raw_data['prjst']
    if status == '新受理':
        return '已受理'
    elif status == '已问询':
        return '已问询'
    elif status == '中止':
        return '中止'
    elif status == '提交注册':
        return '提交注册'
    elif status in ['终止(审核不通过)','终止(撤回)']:
        return '终止'
    elif status in ['注册生效','不予注册','补充审核','终止注册']:
        return '注册结果'
    elif status in ['上市委会议通过','上市委会议未通过','暂缓审议','复审委会议通过','复审委会议不通过']:
        return '上市委会议'

# 处理创业板原始数据
def sz_process(raw_data):
    '''
    深圳创业板数据清洗
    raw_data: 原始数据保存路径
    '''
    cleaned_dict = {}
    temp = json.loads(raw_data['pjdot'])
    
    milestone =[]
    for i in temp.keys():
        if i.isdigit() and temp[i]['startTime'] is not None:
            temp_dict= {'status':temp[i]['name'],'date':temp[i]['startTime'].split(' ')[0],'result':''}
            milestone.append(temp_dict)
    if '-1' in temp.keys():
        milestone[-1]['result'] = temp['-1']['name']

    # clean up info 
    cleaned_dict['baseInfo'] = {'projID':raw_data['prjid'],          # 项目id
            'cmpName':raw_data['cmpnm'],                             # 公司名称
            'cmpAbbrname':raw_data['cmpsnm'],                        # 公司简称
            'projType':raw_data['biztyp'],                           # 项目类型
            'phase': sz_phase(raw_data),                             # 审核阶段 
            'currStatus':raw_data['prjst'],                          # 审核状态
            'region':raw_data['regloc'],                             # 所属地区
            'csrcCode':raw_data['csrcind'],                          # 所属行业
            'acptDate':raw_data['acptdt'],                           # 受理日期
            'updtDate':raw_data['updtdt'],                           # 更新日期
            'issueAmount':raw_data['maramt'],                        # 融资金额
            'sprInst':raw_data['sprinst'],                           # 保荐机构                   
            'sprInstAbbr':raw_data['sprinsts'],                      # 保荐机构简称
            'jsprInst':raw_data['jsprinst'],                         # 联合保荐机构（若有）
            'jsprInstAbbr':raw_data['jsprinsts'],                    # 联合保荐机构简称
            'sprRep':raw_data['sprrep'],                             # 保荐代表人
            'jsprRep':raw_data['jsprrep'],                           # 联合保荐机构代表人
            'acctFirm':raw_data['acctfm'],                           # 会计师事务所
            'acctSgnt':raw_data['acctsgnt'],                         # 签字会计师
            'lawFirm':raw_data['lawfm'],                             # 律师事务所
            'lglSgnt':raw_data['lglsgnt'],                           # 签字律师
            'evalInst':raw_data['evalinst'],                         # 评估机构
            'evalSgnt':raw_data['evalsgnt'],                         # 签字评估师
            'projMilestone':milestone,                               # 项目里程碑
            'auditMarket':'创业板'}                                  # 所属交易所

    # clean up file
    indicator = {'disclosureMaterials':'disclosureMaterial',
                'enquiryResponseAttachment':'responseAttachment',
                'meetingConclusionAttachment':'meetingAttachment',
                'terminationNoticeAttachment':'terminationAttachment',
                'registrationResultAttachment':'registrationAttachment'}

    for key,value in indicator.items():
        material =[]
        for file in raw_data[key]:
            temp_dict =  { 'fname':file['dfnm'],           #  文件名
                    'ftype':file['matnm'],                 #  文件所属类型   
                    'fphynm':file['dfphynm'],              #  文件唯一识别名
                    'ftitle':file['dftitle'],              #  文件标题
                    'fstatus':file['type'],                #  文件所属环节
                    'fdate':file['ddt'],                   #  文件日期
                    'fpath':file['dfpth'],                 #  文件地址
                    'fext':file['dfext'],                  #  文件后缀
                    'other':''} 
            material.append(temp_dict)
        cleaned_dict[value] = material

    # clean up others 
    idx = 1
    temp2 = []
    if raw_data['others'] is not None:
        for item in raw_data['others']:

            others = {  'order':idx,
                'reason':list(item.values())[0].split()[0],
                'date':list(item.keys())[0].split()[0],
                'timestamp':list(item.keys())[0].split()[1]}
            idx+=1
            temp2.append(others)
        cleaned_dict['others'] = temp2
    else:
        cleaned_dict['others'] = ''
    return cleaned_dict

# 解析上海审核状态编码
def sh_phase(st, status_name):
        #if st['suspendStatus'] != '':
            #return '',''
        if int(st[status_name]) == 1:
            result = '已受理'
            status = '已受理'
            
        elif int(st[status_name]) == 2:
            result = '已问询'  
            status = '已问询'
              
        elif int(st[status_name]) == 3:
            status = '上市委会议'
            if st['commitiResult'] != '':
                if int(st['commitiResult']) == 1:
                    result = '上市委会议通过' 
                elif int(st['commitiResult']) == 2:
                    result = '有条件通过' 
                elif int(st['commitiResult']) == 3:
                    result = '上市委会议未通过' 
                elif int(st['commitiResult']) == 6:
                    result = '暂缓审议' 
            else:
                result = '上市委会议' 
                
        elif int(st[status_name]) == 4:
            status = '提交注册'
            result = '提交注册'
        
        elif int(st[status_name]) == 5:
            status = '注册结果'
            if st['registeResult'] != '':
                if int(st['registeResult']) == 1:
                    result = '注册生效'
                elif int(st['registeResult']) == 2:
                    result = '不予注册'
                elif int(st['registeResult']) == 3:
                    result = '终止注册'
            else:
                result = '注册结果'
                
        elif int(st[status_name]) == 6:
            status = '注册结果'
            result = '已发行'
            
        elif int(st[status_name]) == 7:
            suspend = st['suspendStatus']
            status = '中止'
            if suspend == 1 or suspend == '1':
                result = '中止（财报更新）' 
            elif suspend == 2 or suspend == '2':    
                result = '中止（其他事项）' 
            else:
                result = '中止及财报更新'
                
        elif int(st[status_name]) == 8:
            status = '终止'
            result = '终止'      
                     
        elif int(st[status_name]) == 9:
            status = '上市委会议' 
            if st['commitiResult'] != '':
                if int(st['commitiResult']) == 4:
                    result = '复审委会议通过'   
                elif int(st['commitiResult']) == 5:
                    result = '复审委会议不通过'  
            else:
                result = '复审委会议'
            
        elif int(st[status_name]) == 10:
            status = '上市委会议'
            result = '补充审核'
        
        else:
            status = '-'
            result = '-'
        
        return status,result

# 处理科创板原始数据   
def sh_process(raw_data):
    '''
    科创板数据清洗
    raw_data: 原始数据保存路径
    '''
    milestone = []

    for st in raw_data['status']:
        
        sta,result = sh_phase(st,'auditStatus')

        temp_dict = {
            'status': sta,
            'date': st['publishDate'],
            'result': result
        }
        milestone.append(temp_dict)

    info = raw_data['info'][0]

    numInst = len(info['intermediary'])
    
    # 保荐机构、律师事务所、会计师事务所、评估机构
    personList_spr = []
    ints_spr = []
    lawFirm = []
    personList_law = []
    personList_acc = []
    personList_app = []
    accFirm = []
    evalFirm = []
    spr_abbr = []
    acct_abbr = []
    law_abbr = []
    eval_abbr = []
    flag = 0
    for num in range(numInst):
        temp = info['intermediary'][num]
        i_order = temp['i_intermediaryOrder']
        i_type = temp['i_intermediaryType']
        # 1代表保荐机构
        
        if i_type == 1:
            person = info['intermediary'][num]['i_person'] 
            # 选出保荐代表人
            k = [ssx['i_p_personName'] for ssx in person if ssx['i_p_jobType'] == 22 or ssx['i_p_jobType'] == 23]
            inst = [info['intermediary'][num]['i_intermediaryName'] ]
            inst_abbr = [info['intermediary'][num]['i_intermediaryAbbrName'] ]
            personList_spr.append(k)
            ints_spr.append(inst)
            spr_abbr.append(inst_abbr)
            flag = flag + 1
            
        # 2代表会计师事务所
        if i_type == 2:
            sx = info['intermediary'][num]['i_person'] 
            k = [ssx['i_p_personName'] for ssx in sx if ssx['i_p_jobType'] == 32 or ssx['i_p_jobType'] == 33]
            acctfirm = info['intermediary'][num]['i_intermediaryName'] 
            inst_abbr = info['intermediary'][num]['i_intermediaryAbbrName']
            personList_acc = k
            accFirm.append(acctfirm)
            acct_abbr.append(inst_abbr)
            
        # 3代表律师事务所
        if i_type == 3:
            sx = info['intermediary'][num]['i_person'] 
            k = [
                ssx['i_p_personName'] for ssx in sx
                if ssx['i_p_jobType'] == 42 or ssx['i_p_jobType'] == 43
            ]
            lawfirm = info['intermediary'][num]['i_intermediaryName']
            inst_abbr = info['intermediary'][num]['i_intermediaryAbbrName']
            law_abbr.append(inst_abbr)
            personList_law = k
            lawFirm.append(lawfirm)
        
        # 4代表评估机构
        if i_type == 4:
            sx = info['intermediary'][num]['i_person'] 
            k = [
                ssx['i_p_personName'] for ssx in sx
                if ssx['i_p_jobType'] == 52 or ssx['i_p_jobType'] == 53
            ]
            evalinst = info['intermediary'][num]['i_intermediaryName'] 
            inst_abbr = info['intermediary'][num]['i_intermediaryAbbrName']
            personList_app = k
            evalFirm.append(evalinst)
            eval_abbr.append(inst_abbr)
    
    if flag == 1:        
        sprrep = ', '.join(personList_spr[0])
        sprFirm = ', '.join(ints_spr[0])
        spr_ab = ', '.join(spr_abbr[0])
        jspr = ''
        jsprAbbr = ''
        jsprRep = ''
    elif flag == 2:
        sprrep = ', '.join(personList_spr[0])
        sprFirm = ', '.join(ints_spr[0])
        spr_ab = ', '.join(spr_abbr[0])
        jsprRep = ', '.join(personList_spr[1])
        jspr = ', '.join(ints_spr[1])
        jsprAbbr = ', '.join(spr_abbr[1])
    
    acctsgnt = ', '.join(personList_acc)
    acctfirm = ','.join(accFirm)
    acct_ab = ','.join(acct_abbr)
    
    lglsgnt = ', '.join(personList_law)
    lawfirm = ', '.join(lawFirm)
    law_ab = ','.join(law_abbr)
    
    evalsgnt = ', '.join(personList_app)
    evalinst = ', '.join(evalFirm)
    eval_ab = ','.join(eval_abbr)
    # clean up info
    cleaned_dict = {}
    
    cleaned_dict['baseInfo'] = {
        'projID': info['stockAuditNum'],                                  # 项目id
        'cmpName': info['stockIssuer'][0]['s_issueCompanyFullName'],      # 公司名称
        'cmpAbbrname': info['stockIssuer'][0]['s_issueCompanyAbbrName'],  # 公司简称
        'projType': 'IPO',                                                # 项目类型
        'phase': sh_phase(info,'currStatus')[0],                          # 审核阶段
        'currStatus': sh_phase(info,'currStatus')[1],                     # 审核状态
        'region': info['stockIssuer'][0]['s_province'],                   # 所属地区
        'csrcCode': info['stockIssuer'][0]['s_csrcCodeDesc'],             # 所属行业
        'acptDate': raw_data['status'][0]['publishDate'],                 # 受理日期
        'updtDate': raw_data['status'][-1]['publishDate'],                # 更新日期
        'issueAmount': info['planIssueCapital'],                          # 融资金额  
        'sprInst': sprFirm,                                               # 保荐机构
        'sprInstAbbr':spr_ab,                                             # 保荐机构简称
        'jsprInst':jspr,                                  # 联合保荐机构（若有）
        'jsprInstAbbr':jsprAbbr,                             # 联合保荐机构简称
        'sprRep': sprrep,                                                 # 保荐代表人
        'jsprRep':jsprRep,                           # 联合保荐机构代表人
        'acctFirm':  acctfirm,  # 会计师事务所
        'acctSgnt': acctsgnt,  # 签字会计师
        'lawFirm': lawfirm,  # 律师事务所
        'lglSgnt': lglsgnt,  # 签字律师
        'evalInst': evalinst,  # 评估机构
        'evalSgnt': evalsgnt,  # 签字评估师
        'projMilestone': milestone,  # 项目里程碑
        'auditMarket': '科创板'
    }  # 所属交易所

    disclosureMaterial= []
    responseAttachment = []
    meetingAttachment = []
    terminationAttachment = []
    registrationAttachment = []
    for file in raw_data['release']:

        temp_dict = {
            'fname': file['fileTitle'],  #  文件名
            'ftype': '',  #  文件所属类型   
            'fphynm': file['filename'],  #  文件唯一识别名
            'ftitle': file['fileTitle'],  #  文件标题
            'fstatus': '',  #  文件所属环节
            'fdate': file['publishDate'],  #  文件日期
            'fpath': file['filePath'],  #  文件地址
            'fext': '',  #  文件后缀
            'other': ''
        }
        if file['fileType'] == 30:
            disclosureMaterial.append(temp_dict)
            temp_dict['ftype'] = '招股说明书'
            temp_dict['fstatus'] = file['auditStatus']
        elif file['fileType'] == 36:
            disclosureMaterial.append(temp_dict)
            temp_dict['ftype'] = '发行保荐书'
            temp_dict['fstatus'] = file['auditStatus']
        elif file['fileType'] == 37:
            disclosureMaterial.append(temp_dict)
            temp_dict['ftype'] = '上市保荐书'
            temp_dict['fstatus'] = file['auditStatus']
        elif file['fileType'] == 32:
            disclosureMaterial.append(temp_dict)
            temp_dict['ftype'] = '审计报告'
            temp_dict['fstatus'] = file['auditStatus']
        elif file['fileType'] == 33:
            disclosureMaterial.append(temp_dict)
            temp_dict['ftype'] = '法律意见书'
            temp_dict['fstatus'] = file['auditStatus']
        elif file['fileType'] == 6:
            responseAttachment.append(temp_dict)
        elif file['fileType'] == 5:
            responseAttachment.append(temp_dict)
        elif file['fileType'] == 35:
            registrationAttachment.append(temp_dict)
        elif file['fileType'] == 38:
            terminationAttachment.append(temp_dict)

    for file in raw_data['result']:
        temp_dict = {
            'fname': file['fileTitle'],   #  文件名
            'ftype': '',                  #  文件所属类型   
            'fphynm': file['fileName'],   #  文件唯一识别名
            'ftitle': file['fileTitle'],  #  文件标题
            'fstatus': '',                #  文件所属环节
            'fdate': file['publishDate'], #  文件日期
            'fpath': file['filePath'],    #  文件地址
            'fext': '',                   #  文件后缀
            'other': ''
        }
        if file['fileType'] == 1 or file['fileType'] == 2:
            meetingAttachment.append(temp_dict)
    cleaned_dict['disclosureMaterial'] = disclosureMaterial
    cleaned_dict['responseAttachment'] = responseAttachment
    cleaned_dict['meetingAttachment'] = meetingAttachment
    cleaned_dict['terminationAttachment'] = terminationAttachment
    cleaned_dict['registrationAttachment'] = registrationAttachment

    idx = 1
    temp2 = []
    if raw_data['res'] is not None:
        for item in raw_data['res']:
            if item['reasonDesc'] != '':
                others = {
                    'order': idx,
                    'reason': item['reasonDesc'],
                    'date': item['publishDate'],
                    'timestamp': item['timesave'].split(' ')[1]
                }
                idx += 1
                temp2.append(others)
        cleaned_dict['others'] = temp2
    else:
        cleaned_dict['others'] = ''
    return cleaned_dict


    

    