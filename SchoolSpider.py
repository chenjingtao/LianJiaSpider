# -*- coding: utf-8 -*-
"""
@site: http://www.bitbite.cn
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
regions = [u'明珠小学', u'福山外国语', u'六师附小', u'海桐小学', u'打一小学']

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
    csv_sc_writer.writerow(t)
    command=(r"insert into school values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",t)
    return command

def ershoufang_spider(db_sc,url_page=u"http://sh.lianjia.com/ershoufang/x1x2x3x4d2rs%E5%85%AD%E5%B8%88%E9%99%84%E5%B0%8F/"):
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
        """
        subway
        """
        subway = cj.find('span',{'class','fang-subway-ex'})
        if subway:
            info_dict.update({u'地铁':subway.find('span').text})
        else:
            info_dict.update({u'地铁':"NA"})
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
        if soup1.find('p',{'class':'addrEllipsis fl ml_5'}):
            info_dict.update({u'地址':soup1.find('p',{'class':'addrEllipsis fl ml_5'})["title"]})
        if soup1.find('table',{'class':'aroundInfo'}):
            tr=soup1.find('table',{'class':'aroundInfo'}).findAll('tr')
            info_dict.update({u'挂牌单价':tr[0].text.split()[1]})
            td=tr[1].findAll('td')
            info_dict.update({u'楼层':td[0].text.strip().split()[1]})
            info_dict.update({u'建造时间':td[1].text.strip().split()[1]})
            td=tr[2].findAll('td')
            info_dict.update({u'装修':td[0].text.strip().split()[1]})
            info_dict.update({u'朝向':td[1].text.strip().split()[1]})
            """
            price
            """
            price = soup1.find('div',{'class':'mainInfo bold'}).text
            info_dict.update({u'挂牌总价':price})
        """
        style
        """
        if soup1.find('div',{'class':'transaction'}):
            style = soup1.find('div',{'class':'transaction'}).findAll('li')
            t = style[1].text.strip().split()[1]+style[1].text.strip().split()[2]+style[1].text.strip().split()[3]
            info_dict.update({u'房产类型':t})
            info_dict.update({u'交易':style[2].text.strip().split()[1]})
        command=gen_ershoufang_insert_command(info_dict)
        db_sc.execute(command,1)


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
            if excep_name=="ershoufang_spider":
                ershoufang_spider(db_sc, url)
                count+=1
            else:
                print "wrong format"
            print "have spidered %d exception url" % count
        excep_list=exception_read()
    print 'all done ^_^'

def do_school_spider(db_sc, region=u"明珠小学"):
    """
    scratch all schooles
    """
    url=u"http://sh.lianjia.com/ershoufang/"
    """
    add options
    """
    url += "b600to900"  #price (600-900w)
    url += "m60to140"   #space
    url += "l2l3"       #rooms
    url += "y2y3y4y5"   #age
    url += "c1c2"       #floor
    url += "u2u3"       #trade year
    url += "x1x2x3x4"   #zhuangxiu
    url += "o1"         #type
    url_tmp = url
    url += "rs" + region + "/"

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
    print "total houses: %s" % total_num
    print "total pages: %d" % total_pages

    threads=[]
    for i in range(total_pages):
        url_page = url_tmp + "d%drs" % (i+1) + region + "/"
        #ershoufang_spider(db_sc, url_page)
        t=threading.Thread(target=ershoufang_spider,args=(db_sc,url_page))
        threads.append(t)
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    print u"爬下了 %s 对口学区房信息" % region


if __name__=="__main__":

    f = open('log.txt','w+')
    f.close()

    #爬下所有的学区信息
    for region in regions:
        command="create table if not exists school (href TEXT primary key UNIQUE, name TEXT, style TEXT, area TEXT, orientation TEXT, floor TEXT, year TEXT, look TEXT, unit_price TEXT, total_price TEXT,fangchan_class TEXT, trade TEXT, subway TEXT, address TEXT)"
        db_sc=SQLiteWraper('lianjia-sh-'+region+'.db',command)
        csv_sc_file = open(region+"-school.csv","wb")
        csv_sc_writer = csv.writer(csv_sc_file, delimiter=',')
        do_school_spider(db_sc, region)
        exception_spider(db_sc)
        csv_sc_file.close()

