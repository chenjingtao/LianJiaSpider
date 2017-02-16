# -*- coding: utf-8 -*-
"""
@author: 冰蓝
@site: http://lanbing510.info
"""

import re
import urllib2
import sqlite3
import random
import threading
import csv
from bs4 import BeautifulSoup

import sys
reload(sys)
sys.setdefaultencoding("utf-8")

import LianJiaLogIn

#Some User Agents
hds=[{'User-Agent':'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.1.6) Gecko/20091201 Firefox/3.5.6'},\
    {'User-Agent':'Mozilla/5.0 (Windows NT 6.2) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.12 Safari/535.11'},\
    {'User-Agent':'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.2; Trident/6.0)'},\
    {'User-Agent':'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:34.0) Gecko/20100101 Firefox/34.0'},\
    {'User-Agent':'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/44.0.2403.89 Chrome/44.0.2403.89 Safari/537.36'},\
    {'User-Agent':'Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_8; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50'},\
    {'User-Agent':'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50'},\
    {'User-Agent':'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0'},\
    {'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.6; rv:2.0.1) Gecko/20100101 Firefox/4.0.1'},\
    {'User-Agent':'Mozilla/5.0 (Windows NT 6.1; rv:2.0.1) Gecko/20100101 Firefox/4.0.1'},\
    {'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_0) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.56 Safari/535.11'},\
    {'User-Agent':'Opera/9.80 (Macintosh; Intel Mac OS X 10.6.8; U; en) Presto/2.8.131 Version/11.11'},\
    {'User-Agent':'Opera/9.80 (Windows NT 6.1; U; en) Presto/2.8.131 Version/11.11'}]


#区域列表
regions = [u'浦东',u'闵行',u'宝山',u'徐汇',u'普陀',u'杨浦',u'长宁',u'松江',u'嘉定',u'黄浦',u'静安',u'闸北',u'虹口',u'青浦',u'奉贤',u'金山',u'崇明',u'上海周边']
regions = [u'浦东']

lock = threading.Lock()

class SQLiteWraper(object):
    """
    数据库的一个小封装，更好的处理多线程写入
    """
    def __init__(self,path,command='',*args,**kwargs):
        self.lock = threading.RLock() #锁
        self.path = path #数据库连接参数

        if command!='':
            conn=self.get_conn()
            cu=conn.cursor()
            cu.execute(command)

    def get_conn(self):
        conn = sqlite3.connect(self.path)#,check_same_thread=False)
        conn.text_factory=str
        return conn

    def conn_close(self,conn=None):
        conn.close()

    def conn_trans(func):
        def connection(self,*args,**kwargs):
            self.lock.acquire()
            conn = self.get_conn()
            kwargs['conn'] = conn
            rs = func(self,*args,**kwargs)
            self.conn_close(conn)
            self.lock.release()
            return rs
        return connection

    @conn_trans
    def execute(self,command,method_flag=0,conn=None):
        cu = conn.cursor()
        try:
            if not method_flag:
                cu.execute(command)
            else:
                cu.execute(command[0],command[1])
            conn.commit()
        except sqlite3.IntegrityError,e:
            #print e
            return -1
        except Exception, e:
            print e
            return -2
        return 0

    @conn_trans
    def fetchall(self,command="select name from xiaoqu",conn=None):
        cu=conn.cursor()
        lists=[]
        try:
            cu.execute(command)
            lists=cu.fetchall()
        except Exception,e:
            print e
            pass
        return lists


def gen_xiaoqu_insert_command(info_dict):
    """
    生成小区数据库插入命令
    """
    info_list=[u'小区链接',u'小区名称',u'大区域',u'小区域',u'建造时间',u'地铁',u'挂牌均价',u'挂牌数',u'挂牌链接']
    t=[]
    for il in info_list:
        if il in info_dict:
            t.append(info_dict[il])
        else:
            t.append('')
    t=tuple(t)
    command=(r"insert into xiaoqu values(?,?,?,?,?,?,?,?,?)",t)
    return command


def gen_chengjiao_insert_command(info_dict):
    """
    生成成交记录数据库插入命令
    """
    info_list=[u'链接',u'小区名称',u'大区域',u'小区域',u'户型',u'面积',u'朝向',u'楼层',u'建造时间',u'装修',u'签约时间',u'签约单价',u'签约总价',u'房产类型',u'学区',u'地铁',u'地址']
    t=[]
    for il in info_list:
        if il in info_dict:
            t.append(info_dict[il])
        else:
            t.append('')
    t=tuple(t)
    csv_cj_writer.writerow(t)
    command=(r"insert into chengjiao values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",t)
    return command


def gen_ershoufang_insert_command(info_dict):
    """
    生成成交记录数据库插入命令
    """
    info_list=[u'链接',u'小区名称',u'户型',u'面积',u'朝向',u'楼层',u'建造时间',u'装修',u'挂牌单价',u'挂牌总价',u'房产类型',u'交易',u'地铁',u'地址']
    t=[]
    for il in info_list:
        if il in info_dict:
            t.append(info_dict[il])
        else:
            t.append('')
    t=tuple(t)
    csv_es_writer.writerow(t)
    command=(r"insert into ershoufang values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",t)
    return command


def xiaoqu_spider(db_xq,url_page=u"http://sh.lianjia.com/xiaoqu/pg1rs%E6%B5%A6%E4%B8%9C/"):
    """
    爬取页面链接中的小区信息
    """
    print url_page
    try:
        req = urllib2.Request(url_page,headers=hds[random.randint(0,len(hds)-1)])
        source_code = urllib2.urlopen(req,timeout=10).read()
        plain_text=unicode(source_code)#,errors='ignore')
        soup = BeautifulSoup(plain_text, "lxml")
    except (urllib2.HTTPError, urllib2.URLError), e:
        print e
        exit(-1)
    except Exception,e:
        print e
        exit(-1)

    xiaoqu_list=soup.findAll('div',{'class':'info-panel'})
    for xq in xiaoqu_list:
        info_dict={}
        """
        href
        """
        href = u"http://sh.lianjia.com"+xq.find('a')["href"]
        info_dict.update({u'小区链接':href})
        """
        name
        """
        info_dict.update({u'小区名称':xq.find('a').text})
        content=unicode(xq.find('div',{'class':'con'}).renderContents().strip())
        p = re.compile(r".*>(.*)</a>.*>(.*)</a>.*<span>.*</span>(.*)", re.DOTALL)
        info = re.match(p,content)
        if info:
            info = info.groups()
            p = re.compile(r".*\s(.*\d).*", re.DOTALL)
            years = re.match(p, info[2])
            year = years.groups()
            info_dict.update({u'大区域':info[0]})
            info_dict.update({u'小区域':info[1]})
            info_dict.update({u'建造时间':year[0]})
        """
        subway
        """
        subway = xq.find('span',{'class','fang-subway-ex'})
        if subway:
            info_dict.update({u'地铁':subway.find('span').text})
        else:
            info_dict.update({u'地铁':"NA"})
        """
        sale price
        """
        p = re.compile(r"(\S*).*", re.DOTALL)
        price = re.match(p, xq.find('div',{'class':'price'}).find('span',{'class':'num'}).text).groups()[0]
        info_dict.update({u'挂牌均价':price})
        """
        sale num & href
        """
        sale = xq.find('div',{'class':'square'}).find('a')
        sale_num = sale.find('span',{'class':'num'}).text
        sale_href = sale["href"]
        info_dict.update({u'挂牌数':sale_num})
        info_dict.update({u'挂牌链接':u"http://sh.lianjia.com"+sale_href})
        info_list=[u'小区链接',u'小区名称',u'大区域',u'小区域',u'建造时间',u'地铁',u'挂牌均价',u'挂牌数',u'挂牌链接']
        t=[]
        for il in info_list:
            if il in info_dict:
                t.append(info_dict[il])
            else:
                t.append('')
        t=tuple(t)
        command=(r"insert into xiaoqu values(?,?,?,?,?,?,?,?,?)",t)
        #command=gen_xiaoqu_insert_command(info_dict)
        db_xq.execute(command,1)
        csv_writer.writerow(t)

def do_xiaoqu_spider(db_xq,region=u"浦东"):
    """
    爬取大区域中的所有小区信息
    """
    url=u"http://sh.lianjia.com/xiaoqu/rs"+region+"/"
    try:
        req = urllib2.Request(url,headers=hds[random.randint(0,len(hds)-1)])
        source_code = urllib2.urlopen(req,timeout=5).read()
        plain_text=unicode(source_code)#,errors='ignore')
        soup = BeautifulSoup(plain_text, "lxml")
    except (urllib2.HTTPError, urllib2.URLError), e:
        print e
        return
    except Exception,e:
        print e
        return
    total_num = soup.find('div',{'class':'list-head clear'}).find('span').text
    total_pages = int((int(total_num) + 19) / 20)

    threads=[]
    for i in range(total_pages):
        url_page=u"http://sh.lianjia.com/xiaoqu/d%drs%s/" % (i+1,region)
        xiaoqu_spider(db_xq,url_page)
        """
        t=threading.Thread(target=xiaoqu_spider,args=(db_xq,url_page))
        threads.append(t)
    for t in threads:
        t.start()
    for t in threads:
        t.join()
        """
    print u"爬下了 %s 区全部的小区信息" % region


def chengjiao_spider(db_cj,url_page=u"http://sh.lianjia.com/chengjiao/pg1rs%E4%B8%9C%E5%9F%8E%E6%96%B0%E6%9D%91/"):
    """
    爬取页面链接中的成交记录
    """
    try:
        req = urllib2.Request(url_page,headers=hds[random.randint(0,len(hds)-1)])
        source_code = urllib2.urlopen(req,timeout=10).read()
        plain_text=unicode(source_code)#,errors='ignore')
        soup = BeautifulSoup(plain_text, "lxml")
    except (urllib2.HTTPError, urllib2.URLError), e:
        print e
        exception_write('chengjiao_spider',url_page)
        return
    except Exception,e:
        print e
        exception_write('chengjiao_spider',url_page)
        return

    cj_list=soup.findAll('div',{'class':'info-panel'})
    for cj in cj_list:
        info_dict={}
        href=u"http://sh.lianjia.com"+cj.find('a')["href"]
        if not href:
            continue
        info_dict.update({u'链接':href})
        content=cj.find('h2').text.split()
        if content:
            info_dict.update({u'小区名称':content[0]})
            info_dict.update({u'户型':content[1]})
            info_dict.update({u'面积':content[2]})
        content = cj.find('div',{'class':'con'})
        if content:
            info = content.text.strip().split()
            if len(info)>=1:
                info_dict.update({u'大区域':info[0]})
            if len(info)>=2:
                info_dict.update({u'小区域':info[1]})
            if len(info)>=4:
                info_dict.update({u'楼层':info[3]})
            if len(info)>=6:
                info_dict.update({u'朝向':info[5]})
            if len(info)>=8:
                info_dict.update({u'装修':info[7]})
        content=cj.findAll('div',{'class':'div-cun'})
        if content:
            info_dict.update({u'签约时间':content[0].text})
            info_dict.update({u'签约单价':content[1].text})
            info_dict.update({u'签约总价':content[2].text})
        content=cj.find('div',{'class':'introduce'})
        if content:
            content=content.text.strip().split()
            for c in content:
                if c.find(u'满')!=-1:
                    info_dict.update({u'房产类型':c})
                elif c.find(u'学')!=-1:
                    info_dict.update({u'学区':c})
                elif c.find(u'距')!=-1:
                    info_dict.update({u'地铁':c})
                else:
                    print "Nothing to add"
        try:
            req1 = urllib2.Request(href,headers=hds[random.randint(0,len(hds)-1)])
            source_code1 = urllib2.urlopen(req1,timeout=10).read()
            plain_text1=unicode(source_code1)#,errors='ignore')
            soup1 = BeautifulSoup(plain_text1, "lxml")
        except (urllib2.HTTPError, urllib2.URLError), e:
            print e
            exception_write('chengjiao_spider',href)
            return
        except Exception,e:
            print e
            exception_write('chengjiao_spider',href)
            return
        table=soup1.find('table',{'class':'aroundInfo'}).findAll('tr')[1].findAll('td')[1]
        year = table.text.strip().split()
        info_dict.update({u'建造时间':year[1]})
        info_dict.update({u'地址':soup1.find('p',{'class':'addrEllipsis fl ml_5'})["title"]})

        command=gen_chengjiao_insert_command(info_dict)
        db_cj.execute(command,1)


def ershoufang_spider(db_es,url_page=u"http://sh.lianjia.com/ershoufang/pg1rs%E4%B8%9C%E5%9F%8E%E6%96%B0%E6%9D%91/"):
    """
    爬取页面链接中的记录
    """
    try:
        req = urllib2.Request(url_page,headers=hds[random.randint(0,len(hds)-1)])
        source_code = urllib2.urlopen(req,timeout=10).read()
        plain_text=unicode(source_code)#,errors='ignore')
        soup = BeautifulSoup(plain_text, "lxml")
    except (urllib2.HTTPError, urllib2.URLError), e:
        print e
        exception_write('ershoufang_spider',url_page)
        return
    except Exception,e:
        print e
        exception_write('ershoufang_spider',url_page)
        return

    cj_list=soup.findAll('div',{'class':'info-panel'})
    for cj in cj_list:
        info_dict={}
        href=u"http://sh.lianjia.com"+cj.find('a')["href"]
        if not href:
            continue
        info_dict.update({u'链接':href})
        info=cj.find('div',{'class':'where'}).findAll('span')
        info_dict.update({u'小区名称':info[0].text})
        info_dict.update({u'户型':info[1].text})
        info_dict.update({u'面积':info[2].text})
        try:
            req1 = urllib2.Request(href,headers=hds[random.randint(0,len(hds)-1)])
            source_code1 = urllib2.urlopen(req1,timeout=10).read()
            plain_text1=unicode(source_code1)#,errors='ignore')
            soup1 = BeautifulSoup(plain_text1, "lxml")
        except (urllib2.HTTPError, urllib2.URLError), e:
            print e
            exception_write('ershoufang_spider',href)
            return
        except Exception,e:
            print e
            exception_write('ershoufang_spider',href)
            return
        info_dict.update({u'地址':soup1.find('p',{'class':'addrEllipsis fl ml_5'})["title"]})
        tr=soup1.find('table',{'class':'aroundInfo'}).findAll('tr')
        info_dict.update({u'挂牌单价':tr[0].text.split()[1]})
        td=tr[1].findAll('td')
        info_dict.update({u'楼层':td[0].text.strip().split()[1]})
        info_dict.update({u'建造时间':td[1].text.strip().split()[1]})
        td=tr[2].findAll('td')
        info_dict.update({u'装修':td[0].text.strip().split()[1]})
        info_dict.update({u'朝向':td[1].text.strip().split()[1]})
        """
        subway
        """
        subway = soup.find('span',{'class','fang-subway-ex'})
        if subway:
            info_dict.update({u'地铁':subway.find('span').text})
        else:
            info_dict.update({u'地铁':"NA"})
        """
        price
        """
        price = soup.find('div',{'class':'col-3'}).find('span').text+'万'
        info_dict.update({u'挂牌总价':price})
        """
        style
        """
        style = soup1.find('div',{'class':'transaction'}).findAll('li')
        t = style[1].text.strip().split()[1]+style[1].text.strip().split()[2]+style[1].text.strip().split()[3]
        info_dict.update({u'房产类型':t})
        info_dict.update({u'交易':style[2].text.strip().split()[1]})
        command=gen_ershoufang_insert_command(info_dict)
        db_es.execute(command,1)


def xiaoqu_chengjiao_spider(db_cj,xq_name=u"东城新村"):
    """
    爬取小区成交记录
    """
    url=u"http://sh.lianjia.com/chengjiao/rs"+urllib2.quote(xq_name)+"/"
    try:
        req = urllib2.Request(url,headers=hds[random.randint(0,len(hds)-1)])
        source_code = urllib2.urlopen(req,timeout=10).read()
        plain_text=unicode(source_code)#,errors='ignore')
        soup = BeautifulSoup(plain_text, "lxml")
    except (urllib2.HTTPError, urllib2.URLError), e:
        print e
        exception_write('xiaoqu_chengjiao_spider',xq_name)
        return
    except Exception,e:
        print e
        exception_write('xiaoqu_chengjiao_spider',xq_name)
        return
    total_num = soup.find('div',{'class':'list-head clear'}).find('span').text
    total_pages = int((int(total_num) + 19) / 20)

    threads=[]
    for i in range(total_pages):
        url_page=u"http://sh.lianjia.com/chengjiao/pg%drs%s/" % (i+1,urllib2.quote(xq_name))
        t=threading.Thread(target=chengjiao_spider,args=(db_cj,url_page))
        threads.append(t)
    for t in threads:
        t.start()
    for t in threads:
        t.join()


def xiaoqu_ershoufang_spider(db_es,xq_name=u"东城新村"):
    """
    爬取小区sale记录
    """
    url=u"http://sh.lianjia.com/ershoufang/rs"+urllib2.quote(xq_name)+"/"
    try:
        req = urllib2.Request(url,headers=hds[random.randint(0,len(hds)-1)])
        source_code = urllib2.urlopen(req,timeout=10).read()
        plain_text=unicode(source_code)#,errors='ignore')
        soup = BeautifulSoup(plain_text, "lxml")
    except (urllib2.HTTPError, urllib2.URLError), e:
        print e
        exception_write('xiaoqu_ershoufang_spider',xq_name)
        return
    except Exception,e:
        print e
        exception_write('xiaoqu_ershoufang_spider',xq_name)
        return
    num = soup.find('div', {'class':'secondcon fl'}).findAll('span',{'class':'botline'})[1].find('strong').text
    total_num = int(num)
    total_pages = int((int(total_num) + 19) / 20)

    threads=[]
    for i in range(total_pages):
        url_page=u"http://sh.lianjia.com/ershoufang/pg%drs%s/" % (i+1,urllib2.quote(xq_name))
        t=threading.Thread(target=ershoufang_spider,args=(db_es,url_page))
        threads.append(t)
    for t in threads:
        t.start()
    for t in threads:
        t.join()


def do_xiaoqu_chengjiao_spider(db_xq,db_cj):
    """
    批量爬取小区成交记录
    """
    count=0
    xq_list=db_xq.fetchall()
    for xq in xq_list:
        xiaoqu_chengjiao_spider(db_cj,xq[0])
        count+=1
        #print 'Spidered %d xiaoqu' % count
    print 'done'


def do_xiaoqu_ershoufang_spider(db_xq,db_es):
    """
    批量爬取小区成交记录
    """
    count=0
    xq_list=db_xq.fetchall()
    for xq in xq_list:
        xiaoqu_ershoufang_spider(db_es,xq[0])
        count+=1
        #print 'Spidered %d xiaoqu on sale houses' % count
    print 'done'


def exception_write(fun_name,url):
    """
    写入异常信息到日志
    """
    lock.acquire()
    f = open('log.txt','a')
    line="%s %s\n" % (fun_name,url)
    f.write(line)
    f.close()
    lock.release()


def exception_read():
    """
    从日志中读取异常信息
    """
    lock.acquire()
    f=open('log.txt','r')
    lines=f.readlines()
    f.close()
    f=open('log.txt','w')
    f.truncate()
    f.close()
    lock.release()
    return lines


def exception_spider(db_cj):
    """
    重新爬取爬取异常的链接
    """
    count=0
    excep_list=exception_read()
    while excep_list:
        for excep in excep_list:
            excep=excep.strip()
            if excep=="":
                continue
            excep_name,url=excep.split(" ",1)
            if excep_name=="chengjiao_spider":
                chengjiao_spider(db_cj,url)
                count+=1
            elif excep_name=="xiaoqu_chengjiao_spider":
                xiaoqu_chengjiao_spider(db_cj,url)
                count+=1
            else:
                print "wrong format"
            print "have spidered %d exception url" % count
        excep_list=exception_read()
    print 'all done ^_^'


if __name__=="__main__":

    f = open('log.txt','w+')
    f.close()

    #爬下所有的小区信息
    for region in regions:
        command="create table if not exists xiaoqu (href TEXT primary key UNIQUE, name TEXT, regionb TEXT, regions TEXT, year TEXT, subway TEXT, sale_price TEXT, sale_num TEXT, sale_link TEXT)"
        db_xq=SQLiteWraper('lianjia-sh-'+region+'.db',command)
        command="create table if not exists chengjiao (href TEXT primary key UNIQUE, name TEXT, regionb TEXT, regions TEXT, style TEXT, area TEXT, orientation TEXT, floor TEXT, year TEXT, look TEXT, sign_time TEXT, unit_price TEXT, total_price TEXT,fangchan_class TEXT, school TEXT, subway TEXT, address TEXT)"
        db_cj=SQLiteWraper('lianjia-sh-'+region+'.db',command)
        command="create table if not exists ershoufang (href TEXT primary key UNIQUE, name TEXT, style TEXT, area TEXT, orientation TEXT, floor TEXT, year TEXT, look TEXT, unit_price TEXT, total_price TEXT,fangchan_class TEXT, trade TEXT, subway TEXT, address TEXT)"
        db_es=SQLiteWraper('lianjia-sh-'+region+'.db',command)
        csv_file = open(region+".csv","wb")
        csv_writer = csv.writer(csv_file, delimiter=',')
        do_xiaoqu_spider(db_xq,region)
        csv_cj_file = open(region+"-cj.csv","wb")
        csv_cj_writer = csv.writer(csv_cj_file, delimiter=',')
        do_xiaoqu_chengjiao_spider(db_xq,db_cj)
        csv_es_file = open(region+"-esf.csv","wb")
        csv_es_writer = csv.writer(csv_es_file, delimiter=',')
        do_xiaoqu_ershoufang_spider(db_xq,db_es)
        exception_spider(db_cj)
        exception_spider(db_es)
        csv_file.close()
        csv_cj_file.close()
        csv_es_file.close()

