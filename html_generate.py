# -*- encoding: utf-8 -*-

# 辅助脚本用于生成各个项目的详情页

from bs4 import BeautifulSoup
import datetime
import os

# 获取交易所项目详情页中文件存储的地址
def get_baseurl(loc):
    if loc == 'sh':
        return 'https://static.sse.com.cn/stock'
    elif loc == 'sz':
        return 'http://reportdocs.static.szse.cn'
    else:
        os.error('交易所输入错误')
        
# 加载网页模板
def load_template(template_path):
    with open(template_path, 'r', encoding='utf-8') as f:
        soup = BeautifulSoup(f.read(),'html.parser')
    return soup

# 生成项目基本信息
def generate_info(data,soup):
    '''
    # 文件格式
    info[0] : data.cmpnm  公司全称
    info[1] : data.cmpsnm 公司简称
    info[2] : data.acptdt 受理日期
    info[3] : data.updtdt 更新日期
    info[4] : data.prjst  审核状态
    info[5] : data.issueAmount 预计融资金额
    info[6] : data.sprinst 保荐机构
    info[7] ：data.sprrep  保荐代表人
    info[8] : data.acctFirm 会计师事务所
    info[9] : data.acctsgnt 签字会计师
    info[10] : data.lawfm  律师事务所
    info[11] : data.lglsgnt 签字律师
    info[12] : data.evalinst 评估机构
    info[13] : data.evalsgnt 签字评估师
    info[14] : data.lastestAuditEndDate 最近一期审计基准日
    '''

    info = soup.findAll(attrs={'class':'info'})
    new_info = [data['baseInfo']['cmpName'],data['baseInfo']['cmpAbbrname'],data['baseInfo']['acptDate'],data['baseInfo']['updtDate'],data['baseInfo']['currStatus'],str(data['baseInfo']['issueAmount']),data['baseInfo']['sprInst'],data['baseInfo']['sprRep'],data['baseInfo']['acctFirm'],data['baseInfo']['acctSgnt'],data['baseInfo']['lawFirm'],data['baseInfo']['lglSgnt'],data['baseInfo']['evalInst'],data['baseInfo']['evalSgnt']]
    for (tag,new_string) in zip(info,new_info):
        tag.string.replace_with(new_string)

# 生成标题
def generate_title(data,soup):
    title = soup.find(attrs={'class':'project-title'})
    # 补充深/沪标签
    title.string.replace_with(data['baseInfo']['cmpName'])

# 信息披露
def generate_release(data,soup,loc):
    baseurl = get_baseurl(loc)
    ftype = ['招股说明书','发行保荐书','上市保荐书','审计报告','法律意见书']
    for j in range(1,6):
        for i in range(3):
            tempsoup = soup.findAll(attrs={'class':'info-disc-table'})[0].findAll('tr')[j].findAll(attrs={'class':'text-center'})[i]
            d = [(material['fdate'],material['fpath']) for material in data['disclosureMaterial'] if material['ftype'] == ftype[j-1] if material['fstatus']== i+1]
            if len(d) == 0:
                tempsoup.string = "--" 
            else:
                d = sorted(d, key=lambda x: datetime.datetime.strptime(x[0], '%Y-%m-%d'), reverse=False)
                for time,link in d:
                    new_tag = soup.new_tag("a", href=baseurl+link, style="display:block;")
                    new_tag.string = time
                    tempsoup.append(new_tag)

# 问询与回复
def generate_response(data,soup, loc):
    baseurl = get_baseurl(loc)
    tempsoup = soup.findAll(attrs={'class':'info-disc-table'})[1].findAll('tbody')[0]
    length = len(data['responseAttachment'])
    if length ==0:
        new_soup = BeautifulSoup('<tr style="border:none;"><td colspan="10"><div class="report-nodata"><span class="nodata-text">暂无数据！</span></div></td></tr>','html.parser')
        tempsoup.append(new_soup)
    else:
        for i in range(length):
            new_soup = BeautifulSoup('<tr><td></td><td><a target="_blank"></td><td class="text_center"></td></tr>', 'html.parser')
            new_soup.a.string = data['responseAttachment'][i]['fname']
            new_soup.a['href'] = baseurl + data['responseAttachment'][i]['fpath']
            new_soup.td.string = str(i+1)
            new_soup.find(attrs={'class':'text_center'}).string = data['responseAttachment'][i]['fdate']
            tempsoup.append(new_soup)

# 上市委会议公告与结果
def generate_meeting(data,soup, loc):
    baseurl = get_baseurl(loc)
    tempsoup = soup.findAll(attrs={'class':'info-disc-table'})[2].findAll('tbody')[0]
    length = len(data['meetingAttachment'])
    if length == 0:
        new_soup = BeautifulSoup('<tr style="border:none;"><td colspan="10"><div class="report-nodata"><span class="nodata-text">暂无数据！</span></div></td></tr>','html.parser')
        tempsoup.append(new_soup)
    else:
        for i in range(length):
            new_soup = BeautifulSoup('<tr><td></td><td><a target="_blank"></td><td class="text_center"></td></tr>', 'html.parser')
            new_soup.a.string = data['meetingAttachment'][i]['ftitle']
            new_soup.a['href'] = baseurl + data['meetingAttachment'][i]['fpath']
            new_soup.td.string = str(i+1)
            new_soup.find(attrs={'class':'text_center'}).string = data['meetingAttachment'][i]['fdate']
            tempsoup.append(new_soup)

# 注册结果通知
def generate_registration(data,soup, loc):
    baseurl = get_baseurl(loc)
    if (data['terminationAttachment'] is not None) and (len(data['terminationAttachment'])!=0):
        soup.findAll(attrs={'class':'base-title'})[4].string.replace_with('终止审核通知')
        tempsoup = soup.findAll(attrs={'class':'info-disc-table'})[3].findAll('tbody')[0]
        length = len(data['terminationAttachment'])
        for i in range(length):
                new_soup = BeautifulSoup('<tr><td></td><td><a target="_blank"></td><td class="text_center">\
    </td></tr>', 'html.parser')
                new_soup.a.string = data['terminationAttachment'][i]['ftitle']
                new_soup.a['href'] = baseurl + data['terminationAttachment'][i]['fpath']
                new_soup.td.string = str(i+1)
                new_soup.find(attrs={'class':'text_center'}).string = data['terminationAttachment'][i]['fdate']
                tempsoup.append(new_soup)
    else:
        tempsoup = soup.findAll(attrs={'class':'info-disc-table'})[3].findAll('tbody')[0]
        length = len(data['registrationAttachment'])
        if length == 0:
            new_soup = BeautifulSoup('<tr style="border:none;"><td colspan="10"><div class="report-nodata"><span class="nodata-text">暂无数据！</span></div></td></tr>','html.parser')
            tempsoup.append(new_soup)
        else:
            for i in range(length):
                new_soup = BeautifulSoup('<tr><td></td><td><a target="_blank"></td><td class="text_center"></td></tr>', 'html.parser')
                new_soup.a.string = data['registrationAttachment'][i]['ftitle']
                new_soup.a['href'] = baseurl + data['registrationAttachment'][i]['fpath']
                new_soup.td.string = str(i+1)
                new_soup.find(attrs={'class':'text_center'}).string = data['registrationAttachment'][i]['fdate']
                tempsoup.append(new_soup)

# 其他
def generate_others(data,soup):
    tempsoup = soup.findAll(attrs={'class':'info-disc-table'})[4].findAll('tbody')[0]
    if data['others'] is None:
        new_soup = BeautifulSoup('<tr style="border:none;"><td colspan="10"><div class="report-nodata"><span class="nodata-text">暂无数据！</span></div></td></tr>','html.parser')
        tempsoup.append(new_soup)
    else:
        length = len(data['others'])
        for i in range(length):
            new_soup = BeautifulSoup('<tr><td></td><td><a target="_blank"></td><td class="text_center"></td></tr>', 'html.parser')
            new_soup.a.string = data['others'][i]['reason']
            new_soup.td.string = str(i+1)
            new_soup.find(attrs={'class':'text_center'}).string = data['others'][i]['date']
            tempsoup.append(new_soup)

# 进度条
def progressBar(data,soup):
    tempsoup = soup.find(attrs={'class':'project-dy-flow-con'})
    # stage = ['已受理','已问询','上市委会议','提交注册','注册结果']
    # stagenum = stage.index(data['prjst'])
    # for i in range(5):
    # projMilestone
    # profile = json.loads(data['pjdot'])
    # stage = [profile[p]['name'] for p in profile.keys() if p != '-1']
    milestone = data['baseInfo']['projMilestone']
    stage = [p['status'] for p in milestone]
    stagenum = stage.index(data['baseInfo']['projMilestone'][-1]['status'])
    for i in range(len(stage)):
        if i < stagenum:
        # width 应该是 100/len(stage)
            finish_soup = BeautifulSoup('<li class="finished" style="width:{}%;min-width:14%"><b><span class="title"></span><span class="date"></span></b></li>'.format(100/len(stage)),'html.parser')
            finish_soup.find(attrs={'class':'title'}).string = stage[i]
            # date,_ = profile[str(i)]['startTime'].split(' ')
            finish_soup.find(attrs={'class':'date'}).string= milestone[i]['date']
            tempsoup.append(finish_soup)
        if i==stagenum:
            dealing_soup = BeautifulSoup('<li class="dealing" style="width:20%;min-width:14%"><b><span class="title"></span><span class="date"></span></b><span class="other-style"><span></span></span></li>','html.parser')
            dealing_soup.find(attrs={'class':'title'}).string = stage[i]
            # date,_ =profile[str(i)]['startTime'].split(' ')
            dealing_soup.find(attrs={'class':'date'}).string= milestone[i]['date']
            tempsoup.append(dealing_soup)
        elif i > stagenum:
            pending_soup = BeautifulSoup('<li class="pengding" style="width:20%;min-width:14%"><b><span class="title"></span><span class="date"></span></b></li>','html.parser')
            pending_soup.find(attrs={'class':'title'}).string = stage[i]
            tempsoup.append(pending_soup)

# 存储网页
def save_html(soup,directory):
    # saved_path = r'C:\Users\chen\Desktop\测试网页' +'\\'+ data['baseInfo']['cmpName']+ '.html'
    
    with open (directory,"w", encoding='utf-8') as f:
    #   写文件用bytes而不是str，所以要转码  
        f.write(str(soup))

# 获取网页模板地址
def get_template_path():
    return r'F:\GFS intern\JinChao_Data\template\创业板发行上市审核信息公开网站-IPO详情.html'

# 主函数，生成项目对应的网页，并保存
def gen_html(data, savepath, loc):
    template_path = get_template_path()
    soup = load_template(template_path)
    generate_info(data,soup)
    generate_release(data,soup,loc)
    generate_response(data,soup,loc)
    generate_meeting(data,soup,loc)
    generate_registration(data,soup,loc)
    generate_others(data,soup)
    generate_title(data,soup)
    progressBar(data,soup)
    if data['baseInfo']['auditMarket'] == '科创板':
        #saved_path = savepath + '科创板/'+ data['baseInfo']['cmpName'].strip() + '/' + data['baseInfo']['cmpName'].strip() +'.html'
        saved_path = savepath + '科创板/' + data['baseInfo']['cmpName'].strip()
    elif data['baseInfo']['auditMarket'] == '创业板':
        saved_path = savepath + '创业板/' + data['baseInfo']['cmpName'].strip()
    if not os.path.exists(saved_path):
        os.makedirs(saved_path)
    save_html(soup,saved_path + data['baseInfo']['cmpName'].strip() +'.html')
    print("Success! {} HTML generated.".format(data['baseInfo']['cmpName']))
    
    #progressBar(data,soup)
    return 

