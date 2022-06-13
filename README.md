# OpenSea-API-crawler
# -*- coding: utf-8 -*-
"""
Created on Thu Nov 28 10:26:10 2019

@author: ZhuangF
"""

'''
抓取商業司公司、分公司、商業基本資料，並針對中文與外語欄位去切割，放置在正確的欄位裡。

'''
#reset-sf
from lxml import etree
import pandas as pd
import numpy as np
#from bs4 import BeautifulSoup
import requests
import re
import json
import sys
import copy
import traceback
#import test
#import time
import datetime
import time
import pyodbc
from urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)
import string

chipattern = r'[\u3432\u3a17\u4a12\u4e00-\u9fff]+'

#抓取基本資料欄位名稱(公司、商業、分公司(共用))
def Cmpy_col(restext):
    col_cmpt = etree.HTML(restext).xpath('//*[@id="tabBusmContent" or @id="tabCmpyContent"]/div/table/tbody/tr/td[1]/text()')
    if col_cmpt:    
        col_cmpt = [i.strip() for i in col_cmpt]
        return col_cmpt
    else:
        return []

#定位基本資料欄位裡人名位置(公司、商業、分公司(共用))
def col_cmpt_match(col_cmpt):
    col_match_pos = []
    for i in col_cmpt:
        if (re.findall("姓名$", i)):
            col_match_pos.append(col_cmpt.index(i))
    if col_match_pos:
        return col_match_pos
    else:
        for i in col_cmpt:
            if (re.findall("人$", i)):
                col_match_pos.append(col_cmpt.index(i))
        if col_match_pos:
            return col_match_pos
        else:
            return []

#20210312
#找統一編號欄位位置(公司、商業、分公司(共用))
def col_cmpt_match_reg(col_cmpt):
    col_match_pos = []
    for i in col_cmpt:
        if (re.findall("統一編號", i)):
            col_match_pos.append(col_cmpt.index(i))
    if col_match_pos:
        return col_match_pos
    else:
        return []

#20210312
#抓取前統編與變更日期(公司、商業、分公司(共用))
def regchange_records(reg_match_pos,restext):
    #變更日期: CMP_EXRegNo_CHG_Date、前統編:CMP_EX_RegNo、執行紀錄:CMP_EXRegNo_Log
    global chipattern
    if not reg_match_pos:
        return {"CMP_EXRegNo_CHG_Date":"","CMP_EX_RegNo":"","CMP_EXRegNo_Log":"empty list"}
    else:
        try:
            value = etree.HTML(restext).xpath('//*[@id="tabBusmContent" or @id="tabCmpyContent"]/div/table[1]/tbody/tr['+str(reg_match_pos[0]+1)+']/td[2]/text()')
            value = list(filter(None,[m.strip() for m in value]))
            #判斷是否有統編更動。
            if len(value)==2:
                before_regno_list = list(filter(None,[j.strip() for j in re.split('\t|\n|\r',value[1])]))
                if len(before_regno_list)==3:
                    firststring = before_regno_list[0]
                    chi_stringofdate = re.findall(chipattern,firststring,re.I)
                    
                    if len(chi_stringofdate)==3:
                        for m in chi_stringofdate:
                            firststring = firststring.replace(m,"")
                        
                        realdate = firststring.strip()
                    else:
                        realdate = ""
                    
                    #處理數字
                    beforeregstring = re.findall(r'\d+', before_regno_list[-1])
                    if beforeregstring and len(beforeregstring[0])==8:
                        regbefor = beforeregstring[0]
                        log = "Y"
                    else:
                        regbefor = ""
                        log = "統編處理有誤"
                else:
                    realdate = ""
                    regbefor = ""
                    log = "字串不滿足三等份"
            else:
                realdate = ""
                regbefor = ""
                log = "統編沒有更動"
        except:
            realdate = ""
            regbefor = ""
            log = "BUG"

        return {"CMP_EXRegNo_CHG_Date":realdate,"CMP_EX_RegNo":regbefor,"CMP_EXRegNo_Log":log}




'''
#章程所訂外文公司名稱。
def col_cmpt_ENGname(col_cmpt,restext):
    def col_cmpt_ENGname_pos(col_cmpt):
        col_match_pos = []
        for i in col_cmpt:
            if (re.findall("章程所訂外文公司名稱", i)):
                col_match_pos.append(col_cmpt.index(i))
        if col_match_pos:
            return col_match_pos
        else:
            return []

    dict_temp = {}
    colsposition = col_cmpt_ENGname_pos(col_cmpt)
    if colsposition:
        K = etree.HTML(restext).xpath('//*[@id="tabBusmContent" or @id="tabCmpyContent"]/div/table[1]/tbody/tr['+str(colsposition[0]+1)+']/td[1]/text()')
        if K:
            key = K[0].strip()
        else:
            key = "章程所訂外文公司名稱"
        
        V = etree.HTML(restext).xpath('//*[@id="tabBusmContent" or @id="tabCmpyContent"]/div/table[1]/tbody/tr['+str(colsposition[0]+1)+']/td[2]//text()')
        if V:
            value = V[0].strip()
        else:
            value = ""
        dict_temp[key] = value
        return dict_temp
    else:
        return {}
'''
'''
#20200205 增加
#20200225 修正
#20200414 修正
#20200721 修正 "商業名稱" --> "總(本)商業名稱"
#20201013 修正 增加公司名稱日文 CMP_JPname
#re.findall(r"總\S本\S商業名稱", "總(本)商業名稱")
#附註 : 如果未來需要抓取 分支機構名稱、商業名稱... 直接新增就好 key=="公司名稱" 多加幾個or 
'''

#切割公司中英日名，與抓取基本資料欄位資料(公司、商業、分公司(共用))
def col_cmpt_Otherinfo(col_cmpt,restext):
    global chipattern
    def col_cmpt_Otherinfo_pos(col_cmpt):
        col_match_pos = []
        for i in col_cmpt:
            #公司屬性
            if (re.findall("章程所訂外文公司名稱", i)):
                col_match_pos.append((col_cmpt.index(i),"章程所訂外文公司名稱"))
                    
            if (re.findall("公司屬性", i)):
                col_match_pos.append((col_cmpt.index(i),"公司屬性"))
            
            if (re.match("公司名稱", i)):
                col_match_pos.append((col_cmpt.index(i),"公司名稱"))
                
            if (re.match("總機構統一編號", i)):
                col_match_pos.append((col_cmpt.index(i),"總機構統一編號"))
            
            if (re.match("已發行股份總數", i)):
                col_match_pos.append((col_cmpt.index(i),"已發行股份總數(股)"))
                
            if (re.match("每股金額", i)):
                col_match_pos.append((col_cmpt.index(i),"每股金額(元)"))
            
            if '總(本)商業名稱' in i:
                col_match_pos.append((col_cmpt.index(i),'總(本)商業名稱'))
                
            if "現況" in i:
                col_match_pos.append((col_cmpt.index(i),'現況'))
                
            if "分支機構現況" in i:
                col_match_pos.append((col_cmpt.index(i),'現況'))
                
            if "公司狀況" in i:
                col_match_pos.append((col_cmpt.index(i),'現況'))
                
                
        if col_match_pos:
            return col_match_pos
        else:
            return []

    dict_temp = {}
    colsposition = col_cmpt_Otherinfo_pos(col_cmpt)

    if colsposition:
        for i in colsposition:
            K = etree.HTML(restext).xpath('//*[@id="tabBusmContent" or @id="tabCmpyContent"]/div/table[1]/tbody/tr['+str(i[0]+1)+']/td[1]/text()')
            if K:
                key = K[0].strip()
            else:
                key = i[1]
            
            V = etree.HTML(restext).xpath('//*[@id="tabBusmContent" or @id="tabCmpyContent"]/div/table[1]/tbody/tr['+str(i[0]+1)+']/td[2]/text()')
            if any(xx in key for xx in ["總機構統一編號","總(本)商業名稱"]):
                V = etree.HTML(restext).xpath('//*[@id="tabBusmContent" or @id="tabCmpyContent"]/div/table[1]/tbody/tr['+str(i[0]+1)+']/td[2]//text()')
            else:
                V = etree.HTML(restext).xpath('//*[@id="tabBusmContent" or @id="tabCmpyContent"]/div/table[1]/tbody/tr['+str(i[0]+1)+']/td[2]/text()')
            
            if V:
                lv = list(filter(None,[ii.strip().replace('\n', '').replace('\t','').replace('\r','') for ii in V]))
                for a in range(len(lv)):
                    if (re.findall("發文號", lv[a])):
                        del lv[a]
                
                if key=="公司名稱":
                    #有沒有圖片?
                    imglist = etree.HTML(restext).xpath('//*[@id="tabBusmContent" or @id="tabCmpyContent"]/div/table[1]/tbody/tr['+str(i[0]+1)+']/td[2]/img/@src')
                    
                    if imglist:
                        img = True
                    else:
                        img = False
                    
                    if img:
                        lv = ["癵" + lv[0]]
                    
                    count = 0
                    chinese = []
                    english = []
                    if len(lv)==2:
                        
                        for ii in lv:
                            if u'\u4e00' <= ii <= u'\u9fff':
                                count = count+1
                                chinese.append(ii)
                            elif re.findall(r'[a-z]+',ii,re.I):
                                english.append(ii)
                            else:
                                count = count+1
                                chinese.append(ii)                        
                        
                        if count ==1:
                            value = chinese[0]
                            eng = english[0]
                            #去頭去尾非英文
                            non_en_last = []
                            non_en_f = []
                            for en in range(len(eng)):
                                if not re.findall(r'[a-z]+',eng[en],re.I):
                                    non_en_f.append(eng[en])
                                else:
                                    break
                            
                            for en in range(len(eng)-1,0,-1):
                                if not re.findall(r'[a-z]+',eng[en],re.I):
                                    non_en_last.append(eng[en])
                                else:
                                    break
                            #判斷數字
                            numberstring = []
                            if non_en_f:
                                for numb in range(len(non_en_f)):
                                    if non_en_f[numb].isdigit():
                                        numberstring.append(non_en_f[numb])
                            
                            eng = eng.replace("".join(non_en_f),"",1)
                            if numberstring:
                                eng = "".join(numberstring) + eng
                            
                            if non_en_last:
                                if non_en_last[-1]==".":
                                    non_en_last = non_en_last[:-1]
                                non_en_last = "".join(non_en_last)
                                eng = eng[::-1].replace("".join(non_en_last),"",1)[::-1]
                                
                            dict_temp[key+"英文"] = eng
                        
                        else:
                            #外國公司基本資料
                            tab = False
                            tabcmpycontent = etree.HTML(restext).xpath('//*[@id="tabCmpyContent"]/h3/text()')
                            if tabcmpycontent:
                                if "外國公司" in tabcmpycontent[0]:
                                    tab = True
                            
                            countj = 0
                            chinesej = []
                            japanese = []
                            for ii in lv:
                                if any(xx in ii for xx in ["日商","日本商"]):
                                    countj = countj+1
                                    chinesej.append(ii)
                                else:
                                    japanese.append(ii)

                            if countj ==1 & tab == True:
                                value = chinesej[0]
                                jap = japanese[0]
                                dict_temp[key+"日文"] = jap

                            #20220505
                            elif (countj ==2) & (tab == True) and not japanese:
                                value = ''.join(chinesej[0].split())
                                jap = ''.join(chinesej[1].split())
                                dict_temp[key+"日文"] = jap
                                
                            elif (countj ==0) & (any(xx in ii for ii in lv for xx in ["株式會社"])) and japanese:
                                for mj in lv:
                                    if "株式會社" in mj:
                                        jap = ''.join(mj.split())
                                        lv.remove(mj)
                                        break
                                
                                dict_temp[key+"日文"] = jap
                                value = ''.join(lv[0].split())
                                
                            else:
                                value = ''.join(lv)
                                engvalue = re.findall(r'[a-z]+',value,re.I)
                                if engvalue:
                                    non_en_last = []#尾端非英文字串
                                    non_en_f = []#開頭非英文字串
                                    engvalue = copy.deepcopy(value)
                                    for en in range(len(engvalue)):
                                        if not re.findall(r'[a-z]+',engvalue[en],re.I):
                                            non_en_f.append(engvalue[en])
                                        else:
                                            break
                                    for en in range(len(engvalue)-1,0,-1):
                                        if not re.findall(r'[a-z]+',engvalue[en],re.I):
                                            non_en_last.append(engvalue[en])
                                        else:
                                            break
                                    engvalue = engvalue.replace("".join(non_en_f),"",1)#將開頭非英文的字串取代
                                    if non_en_last:
                                        if non_en_last[-1]==".":
                                            non_en_last = non_en_last[:-1]
                                        non_en_last = "".join(non_en_last)
                                        engvalue = engvalue[::-1].replace("".join(non_en_last),"",1)[::-1]
                                    dict_temp[key+"英文"] = engvalue
                                    value = re.findall(chipattern, value,re.I)[0]
                            
                    else:
                        #如果字串被切成三段或以上
                        if len(lv)==3:
                            value = lv[0]
                        else:
                            value = ' '.join(lv)

                
                elif any(xx in key for xx in ["已發行股份總數(股)","每股金額(元)"]):
                    value = V[0].strip().replace(",", '').replace(",",'')

                else:
                    if len(V)>1:
                        value = ' '.join(lv)
                    else:
                        value = V[0].strip()
            else:
                value = ""
            dict_temp[key] = value
        #將公司名稱裡的"癵"字串用"□"取代
        if "公司名稱" in dict_temp.keys():
            if "癵" in dict_temp["公司名稱"]:
                dict_temp["公司名稱"] = dict_temp["公司名稱"].replace("癵", "□")
        
        return dict_temp
    else:
        return {}


#抓取股權資料(公司、商業、分公司(共用))
def col_cmpt_match_shareholder(col_cmpt,restext):
    col_list_pos = [col_cmpt.index(i) for i in col_cmpt if '股權狀況' in i]
    shareholder_info = {}
    if col_list_pos:
        for m in col_list_pos:
            key = etree.HTML(restext).xpath('//*[@id="tabBusmContent" or @id="tabCmpyContent"]/div/table[1]/tbody/tr['+str(m+1)+']/td[1]/text()')[0].strip()
            try:
                value = etree.HTML(restext).xpath('//*[@id="tabBusmContent" or @id="tabCmpyContent"]/div/table[1]/tbody/tr['+str(m+1)+']/td[2]//text()')
                if len(value)>1:
                    value = ', '.join(list(filter(None,[l.strip() for l in value])))
                else:
                    value = value[0].strip()
                    for b in ['\t','\n','\r']:
                        value = value.replace(b, '')
            except:
                value = ""
            shareholder_info[key] = value
        return shareholder_info
    else:
        return shareholder_info

'''
#20200311修正。
#20200313修正。
#20200428修正 負責人英文人名也放在中文欄位的問題。
#2020/6/30 針對 "陳明達 Chen, Ming-Ta" 格式做出了排除。
#20210312 商業合夥人出資額若無資料 放None 
#20210526圖形字
'''

#中英文姓名字串分割(公司、商業、分公司(共用))
def name_2json(col_match_pos,restext):
    global chipattern
    def special_char1(dict_name,value,key):
        if any(x in value for x in [",",","]):
            #有","也有"(",")" 直接略過
            for spe in [["(",")"],["（","）"],["（",")"],["(","）"],["〈","〉"]]:
                if any(x in value for x in spe):
                    value = ""
                    dict_name[key+"_LATIN"] = ""
                    break
            else:
                #有","
                temp_split = value.split(',')
                if len(temp_split)==2:
                    
                    str1 = temp_split[1].strip()
                    str0 = temp_split[0].strip()
                    
                    if not re.findall(chipattern, str1,re.I) and re.findall(r'[a-z]+',str0[-1],re.I):
                        chitemp = re.findall(chipattern, str0,re.I)#含有中文的字串
                        
                        if chitemp:
                            non_en_f = []
                            numberstring = []
                            latintemp = value.strip(chitemp[0]).strip()#取代含有中文的字串剩下來的字串
                            for en in range(len(latintemp)):
                                if not re.findall(r'[a-z]+',latintemp[en],re.I):
                                    non_en_f.append(latintemp[en])
                                else:
                                    break                            
                            
                            latintemp = latintemp.replace("".join(non_en_f),"",1)#把結尾非英文字的字串取代
                            dict_name[key+"_LATIN"] = latintemp
                            
                            if len(chitemp)>1:
                                value = "".join(chitemp)
                            else:
                                value = chitemp[0]

                        else:
                            value = ""
                            dict_name[key+"_LATIN"] = ""
                    else:
                        value = ""
                        dict_name[key+"_LATIN"] = ""
                
                else:
                    value = ""
                    dict_name[key+"_LATIN"] = ""
        
        else:
            #沒有","但有"(",")"
            for spe in [["(",")"],["（","）"],["（",")"],["(","）"],["〈","〉"]]:
                if all(x in value for x in spe):
                    value_br = list(filter(None,re.split("\\"+spe[0]+"|\\"+spe[1],value)))
                    #括號切完變兩半
                    if len(value_br)==2:
                        #
                        count = 0
                        chinese = []
                        english = []
                        value_br = sorted(value_br,reverse=True)                    
                        for ii in value_br:
                            if u'\u4e00' <= ii <= u'\u9fff':
                                count = count+1
                                chinese.append(ii)
                            else:
                                english.append(ii)
                        if count ==1:
                            value = chinese[0].replace(" ","").strip()
                            dict_name[key+"_LATIN"] = english[0].strip()
                        
                        elif count ==2:
                            dict_name[key+"_LATIN"] = ""
                        
                        else:
                            dict_name[key+"_LATIN"] = value.strip()
                            value = ""
                        
                        break
                    #括號切完變三半
                    elif len(value_br)==3:
                        if not u'\u4e00' <= value_br[1] <= u'\u9fff':
                            chinese_list = re.findall(chipattern, value,re.I)
                            value_eng = replaceMultiple(value, chinese_list, "")
                            
                            value = "".join(chinese_list)
                            replace_char = []
                            value_eng = value_eng.strip()
                            for u in range(len(value_eng)-1,-1,-1):
                                if not re.match(r'[a-z]+',value_eng[u],re.I):
                                    replace_char.append(value_eng[u])
                                
                            for u in replace_char:
                                value_eng = value_eng.strip(u)
                            
                            dict_name[key+"_LATIN"] = value_eng
                            break
                        
                        else:
                            english_list = re.findall(r'[a-z]+',value,re.I)
                            for eng in english_list:
                                value = value.replace(eng,"",1)
                            value = value.strip()
                            replace_char = []
                            for u in range(len(value)-1,-1,-1):
                                if not u'\u4e00' <= value[u] <= u'\u9fff':
                                    replace_char.append(value[u])
                                    
                            for u in replace_char:
                                value = value.strip(u)

                            dict_name[key+"_LATIN"] = " ".join(english_list)
                            break
                    else:
                        break
                    
            else:
                value_br = re.findall(r'[a-z]+',value,re.I)
                value_chi = re.findall(chipattern, value,re.I)
                #只有英
                if value_br and not value_chi:
                    engvalue = copy.deepcopy(value)
                    non_en_last = []
                    non_en_f = []
                    for en in range(len(engvalue)):
                        if not re.findall(r'[a-z]+',engvalue[en],re.I):
                            non_en_f.append(engvalue[en])
                        else:
                            break
                    for en in range(len(engvalue)-1,0,-1):
                        if not re.findall(r'[a-z]+',engvalue[en],re.I):
                            non_en_last.append(engvalue[en])
                        else:
                            break
                    engvalue = engvalue.replace("".join(non_en_f),"",1)
                    if non_en_last:
                        engvalue = engvalue[::-1].replace("".join(non_en_last),"",1)[::-1]
                    dict_name[key+"_LATIN"] = engvalue
                    value = copy.deepcopy(engvalue)
                
                #只有中
                elif not value_br and value_chi:
                    non_chi_last = []#開頭字串非中文
                    non_chi_f = []#結尾字串非中文
                    
                    for chi in range(len(value)):
                        if not re.findall(chipattern,value[chi],re.I):
                            non_chi_f.append(value[chi])
                        else:
                            break
                        
                    for chi in range(len(value)-1,0,-1):
                        if not re.findall(chipattern,value[chi],re.I):
                            non_chi_last.append(value[chi])
                        else:
                            break
                    value = value.replace("".join(non_chi_f),"",1)
                    if non_chi_last:
                        value = value[::-1].replace("".join(non_chi_last),"",1)[::-1]
                    
                    value = value.replace(" ","").replace("　","").strip()
                    dict_name[key+"_LATIN"] = ""
                
                #有英也有中
                elif value_br and value_chi:
                    engvalue = copy.deepcopy(value)
                    chivalue = copy.deepcopy(value)
                    non_en_last = []
                    non_en_f = []
                    for en in range(len(engvalue)):
                        if not re.findall(r'[a-z]+',engvalue[en],re.I):
                            non_en_f.append(engvalue[en])
                        else:
                            break
                    for en in range(len(engvalue)-1,0,-1):
                        if not re.findall(r'[a-z]+',engvalue[en],re.I):
                            non_en_last.append(engvalue[en])
                        else:
                            break
                    engvalue = engvalue.replace("".join(non_en_f),"",1)
                    if non_en_last:
                        engvalue = engvalue[::-1].replace("".join(non_en_last),"",1)[::-1]
                    dict_name[key+"_LATIN"] = engvalue
                    value = chivalue.replace(engvalue,"",1).replace(" ","").replace("　","").strip()
                else:
                    dict_name[key+"_LATIN"] = ""
        
        return dict_name,value    

    dict_name = {}
    for i in col_match_pos:
        key = etree.HTML(restext).xpath('//*[@id="tabBusmContent" or @id="tabCmpyContent"]/div/table[1]/tbody/tr['+str(i+1)+']/td[1]/text()')[0].strip()
        try:
            value = etree.HTML(restext).xpath('//*[@id="tabBusmContent" or @id="tabCmpyContent"]/div/table[1]/tbody/tr['+str(i+1)+']/td[2]//text()')
            
            imglist = etree.HTML(restext).xpath('//*[@id="tabBusmContent" or @id="tabCmpyContent"]/div/table[1]/tbody/tr['+str(i+1)+']/td[2]//img/@src')
            
            if imglist:
                img = True
            else:
                img = False
            
            if len(value)>1:
                #出資額:在裡面沒有","
                #一般中文名字是不會有","的    .replace(',', '')            
                if img:
                    if len(list(filter(None,[m.strip() for m in value])))<3:
                        value = [m.strip() for m in value]
                        
                        imglist_len = len(imglist)
                        countvalue = sum(1 for ii in value if ii == "")#計數有多少空字串
                        
                        if len(imglist)>countvalue:
                            
                            for m in range(len(value)):
                                if value[m]:
                                    pass
                                else:
                                    if countvalue>1:
                                        value[m] = value[m].replace('', "癵")
                                        imglist_len = imglist_len-1
                                    else:
                                        value[m] = value[m].replace('', "癵"*imglist_len)
                            #20220414
                            if not "癵" in value:
                                value = value + ["癵"]
                                
                        elif len(imglist)==countvalue:
                            
                            for m in range(len(value)):
                                if value[m]:
                                    pass
                                else:
                                    if countvalue>1:
                                        value[m] = value[m].replace('', "癵")
                                        imglist_len = imglist_len-1
                                    else:
                                        value[m] = value[m].replace('', "癵"*imglist_len)
                        else:
                            value = [m if m else m.replace('', "癵") for m in value]
                    else:
                        value = list(filter(None,[m.strip() for m in value]))
                
                else:
                    value = list(filter(None,[m.strip() for m in value]))
                #姓名裡有多列與出資額成對
                if any(x in valu for valu in value for x in [":","："]):
                    dict_count2 = {}
                    dict_list = []
                    for vv in value:
                        
                        if ":" in vv:
                            split_temp = re.split(':',vv)
                            
                            if ("出資額" in split_temp[0]) and (not split_temp[1]):
                                split_temp[1] = None
                            
                            elif ("出資額" in split_temp[0]) and (split_temp[1]):
                                dict_count2[split_temp[0]] = split_temp[1]
                            else:
                                dict_count2["姓名"] = vv.replace("　","").replace("╲","")

                        elif "：" in vv:
                            split_temp = re.split('：',vv)
                            
                            if ("出資額" in split_temp[0]) and (not split_temp[1]):
                                split_temp[1] = None
                                
                            elif ("出資額" in split_temp[0]) and (split_temp[1]):
                                dict_count2[split_temp[0]] = split_temp[1]
                            else:
                                dict_count2["姓名"] = vv.replace("　","").replace("╲","")
                        
                        else:
                            if img:
                                if "姓名" in dict_count2.keys():
                                    namemerge = dict_count2["姓名"] + vv.replace("　","") + "□"
                                    dict_count2["姓名"] = namemerge
                                else:
                                    dict_count2["姓名"] = vv.replace("　","")
                            else:
                                dict_count2["姓名"] = vv.replace("　","").replace("╲","")
  
                        
                        if len(dict_count2)==2:
                            dict_list.append(dict_count2)
                            dict_count2 = {}
                    
                    if  dict_count2:
                        dict_list.append(dict_count2)
                    
                    value_origin = ''.join(value)
                    value = dict_list
                    dict_name[key+"_LATIN"] = ""
                    
                else:
                    value = ''.join(value)
                    value_origin = copy.deepcopy(value)
                    dict_name,value = special_char1(dict_name,value,key)
            
            else:
                #0505
                #沒有以上例外情況的中英文姓名切割
                value = value[0].strip()
                value_lst = list(filter(None,[j.strip() for j in re.split('\t|\n|\r',value)]))
                if len(value_lst)==1:
                    
                    value = value_lst[0]
                    value_origin = copy.deepcopy(value)
                    #1.xxx 2.yyy 姓名格式
                    number_list = re.findall('[0-9]+', value)
                    if number_list:
                        number_list_int = [int(x) for x in number_list]
                        if len(number_list_int)>1:
                            diff_number = list(set(np.diff(number_list_int)))
                            if diff_number[0]==1:#判斷序列是否連續間隔為一
                                for iiii in number_list:                                
                                    value = value.replace(iiii, "!@!").replace(".", "")
                                    value_multiple_name = value.split("!@!")
                                    value_multiple_name = list(filter(None,[j.strip() for j in value_multiple_name]))
                                    
                                dict_list = []
                                dict_liste = []
                                
                                for ab in range(len(value_multiple_name)):
                                    dict_count_c = {}
                                    dict_count_e = {}
                                    e_c = special_char(value_multiple_name[ab])
                                    dict_count_c["姓名"] = e_c[1]
                                    dict_count_e["姓名_LATIN"] = e_c[0]
                                    dict_list.append(dict_count_c)
                                    dict_liste.append(dict_count_e)
   
                                value = dict_list
                                dict_name[key+"_LATIN"] = dict_liste
                            else:
                                dict_name,value = special_char1(dict_name,value,key)
                        else:
                            dict_name,value = special_char1(dict_name,value,key)
                    else:
                        dict_name,value = special_char1(dict_name,value,key)
                    
                elif len(value_lst)==2:
                    count = 0
                    chinese = []
                    english = []
                    for ii in value_lst:
                        if u'\u4e00' <= ii <= u'\u9fff':
                            count = count+1
                            chinese.append(ii)
                        else:
                            english.append(ii)
                    if count ==1:
#                        value = chinese[0]
#                        value_origin = copy.deepcopy(value)
                        value_origin = copy.deepcopy(re.sub("[\t\r\n\f]", '', value))
                        value = chinese[0].replace(" ","")
                        
                        dict_name[key+"_LATIN"] = english[0]
                    else:
                        value = ' '.join(value_lst)
                        value_origin = copy.deepcopy(value)
                        dict_name[key+"_LATIN"] = ""
                else:
                    #掉到這邊超級怪的，遇到再修正。
                    value = ' '.join(value_lst)
                    value_origin = copy.deepcopy(value)
                    dict_name[key+"_LATIN"] = ""
        except:
            value = ""
            value_origin = ""
            dict_name[key+"_LATIN"] = ""
        
        if type(value)==list:
            dict_name[key+"中文"] = value
        else:
            #再檢查是否有中文，如果有中文才走，如果沒中文直接放
            if re.findall(chipattern,value,re.I):
                dict_name[key+"中文"] = value.replace(" ","").replace("　","")
            else:
                dict_name[key+"中文"] = value
            
        dict_name[key] = value_origin
    #將"癵"取代成"□"
    for k in list(dict_name.keys()):
        if "癵" in dict_name[k]:
            dict_name[k] = dict_name[k].replace("癵","□")
    
#    json.dumps(dict_name, ensure_ascii=False)
    return dict_name

#抓取公司歷史資料:但好像暫時沒有被使用到
def history_info(restext):
    if etree.HTML(restext).xpath('//*[@id="tabHistory"]/text()'):
        history_col = [i.strip() for i in etree.HTML(restext).xpath('//*[@id="tabHistoryContent"]/div/table[1]/thead/tr/th/text()')]#div[3]
        row_count = len(etree.HTML(restext).xpath('//*[@id="tabHistoryContent"]/div/table[1]/tbody/tr'))
        list_temp2 = []
#        regno = []
        for i in range(row_count):
            list_temp = []
            for m in range(len(history_col)):
                #不用再區分了~
                cnn = etree.HTML(restext).xpath('//*[@id="tabHistoryContent"]/div/table/tbody/tr['+ str(i+1)+']'+'/td['+str(m+1)+']//text()')
                if cnn:
                    cnn2 = list(filter(None,[n.strip() for n in cnn]))
                    
                    if len(cnn2)==1:
                        list_temp.append(cnn2[0].replace(',', ''))
                    elif len(cnn2)>1:
                        list_temp.append(', '.join(cnn2))
                    else:
                        list_temp.append(np.nan)
                else:
                    list_temp.append(np.nan)
            list_temp2.append(list_temp)
        df_history = pd.DataFrame(list_temp2,columns=history_col).dropna(axis=0, thresh=5)
#        df_history = df_history.drop(columns=['序號'])
        df_history = df_history.rename(index=lambda s: 'History_'+df_history['序號'][s])
        df_history = df_history.drop(columns=['序號'])
        return json.loads(df_history.to_json(orient='index', force_ascii=False))
    else:
        return {}

#抓取公司歷史資料，換頁循環
def hyper_history(restext,s):
    global GUI
    global headers1
    dict_history = {}
    try:
        his_temp = history_info(restext)
        if his_temp:
            dict_history.update(his_temp)
            page = list(filter(None,[i.strip() for i in etree.HTML(restext).xpath('//*[@id="QueryCmpyDetail_queryCmpyDetail"]/nav/ul/li//text()')]))
            if page:
                for p in page:
                    if "頁" in p:
                        page.remove(p)
                page.remove("1")
                for a in page:
                    data2 = {"banNo":GUI, \
                             "CPageHistory":a, \
                             "eng":"false", \
                             "historyPage":"Y"}
                    res1 = s.post("https://findbiz.nat.gov.tw/fts/query/QueryCmpyDetail/queryCmpyDetail.do",data=data2,headers = headers1)
                    dict_history.update(history_info(res1.text))
                return dict_history
            else:
                return dict_history
        else:
            return {}
    except:
        print("something went wrong")
        return {}

'''
#第一步: ["第一頁","1","2","3","最後頁"]
list(filter(None,[i.strip() for i in etree.HTML(res.text).xpath('//*[@id="QueryCmpyDetail_queryCmpyDetail"]/nav/ul/li//text()')]))

#第二步: 移除"頁"
a.remove(i) for i in a if "頁" in i

#第三步: 移除"1"，然後再送出post請求循環抓取。建一個空的dict{} 循環update。而因為序號是接力的，所以不影響。

'''


#如果用判斷是否有下層節點a 再去抓統編，會比較完整。不是用位置去判斷。
#已改過2019 12 02
#20210526
#抓取公司董監事資料
def supervisor_info(restext):
    #法人移位關鍵字串
    Pname_filter = ["委任","號函","辭職","解任","委任關係","管理人","撤銷","判決","處分","暫缺","死亡","缺額","停權","暫不","缺員","補選","代表","廢止","遺產"]

    CMPname_filter_E = ["Limited","ltd","LIMITED","INC.","LTD.","LTD","Ltd","Inc.","Ltd.","INC","LP","B.V.","CORP","BHD","GMBH","L.P","GmbH","LLC", \
                        "L. P.","Partnership","Corporation","corporation","CORPORATION","Corpo ration","corp","Corp","limited","CAPITAL","Capital"]
    
    CMPname_filter_C = ["公司","基金會","財團法人","社團法人","有限","農會","合作金庫","株式會社", "工會","大學","基金","工業","學會","中心", \
                        "機械","科技","合夥","投資","電纜","委員會","建設開發","政府","公會","合作社","學院","協會","社團","機構","股份"]
    
    if etree.HTML(restext).xpath('//*[@id="tabShareHolder"]/text()'):
        Supervisor_col = [i.strip() for i in etree.HTML(restext).xpath('//*[@id="tabShareHolderContent"]/div/table/thead/tr/th/text()')]#div[3]
        row_count = len(etree.HTML(restext).xpath('//*[@id="tabShareHolderContent"]/div/table/tbody/tr'))
        list_temp2 = []
        regno3 = []
        regno4 = []
        mergereg = []
        for i in range(row_count):
            list_temp = []
            
            for m in range(len(Supervisor_col)):
                
                if m+1 == 3 or m+1 == 4:
                    cmn = etree.HTML(restext).xpath('//*[@id="tabShareHolderContent"]/div/table/tbody/tr['+ str(i+1)+']'+'/td['+str(m+1)+']//text()')
                    
                    imglist = etree.HTML(restext).xpath('//*[@id="tabShareHolderContent"]/div/table/tbody/tr['+ str(i+1)+']'+'/td['+str(m+1)+']/img/@src')
                    
                    if imglist:
                        img = True
                    else:
                        img = False
                    
                    if cmn:
                        if img:#圖片?
                            if len(list(filter(None,[ii.strip() for ii in cmn])))<3:
                                
                                cmn = [m.strip() for m in cmn]
                                imglist_len = len(imglist)
                                countvalue = sum(1 for ii in cmn if ii == "")
                                
                                if len(imglist)>countvalue:
                                    
                                    for mm in range(len(cmn)):
                                        if cmn[mm]:
                                            pass
                                        else:
                                            if countvalue>1:
                                                cmn[mm] = cmn[mm].replace('', "癵")
                                                imglist_len = imglist_len-1
                                            else:
                                                cmn[mm] = cmn[mm].replace('', "癵"*imglist_len)
                                    #20220413
                                    if not "癵" in cmn:
                                        cmn = cmn + ["癵"]
                                                
                                elif len(imglist)==countvalue:
                                    
                                    for mm in range(len(cmn)):
                                        if cmn[mm]:
                                            pass
                                        else:
                                            if countvalue>1:
                                                cmn[mm] = cmn[mm].replace('', "癵")
                                                imglist_len = imglist_len-1
                                            else:
                                                cmn[mm] = cmn[mm].replace('', "癵"*imglist_len)
                                else:
                                    cmn = [m if m else m.replace('', "癵") for m in cmn]
                            else:
                                cmn = list(filter(None,[m.strip() for m in cmn]))
                        else:
                            cmn = list(filter(None,[m.strip() for m in cmn]))
                        #沒有圖片先把字串併在一起
                        cmn_clear = ''.join(cmn)
                        if cmn_clear:
                            list_temp.append(cmn_clear)
                        else:
                            list_temp.append("")
                    else:
                        list_temp.append(np.nan)
                        
                    cmn_js_query = etree.HTML(restext).xpath('//*[@id="tabShareHolderContent"]/div/table/tbody/tr['+ str(i+1)+']'+'/td['+str(m+1)+']/a')
                    #是否藏有統編?
                    if m+1 == 3:
                        if cmn_js_query:
                            #用-2 取代 1(20200420)
                            regno3.append(cmn_js_query[0].get("onclick").split(',')[-2].translate({ord("'"): None}))
                        else:
                            regno3.append(np.nan)
                    else:
                        if cmn_js_query:
                            #用-2 取代 1(20200420)
                            regno4.append(cmn_js_query[0].get("onclick").split(',')[-2].translate({ord("'"): None}))
                        else:
                            regno4.append(np.nan)

                else:
                    cnn = etree.HTML(restext).xpath('//*[@id="tabShareHolderContent"]/div/table/tbody/tr['+ str(i+1)+']'+'/td['+str(m+1)+']/text()')
                    if cnn:
                        cnn2 = cnn[0].strip()
                        if cnn2:
                            list_temp.append(cnn2.replace('\t', '').replace('\n', '').replace('\r', '').replace(',', ''))
                        else:
                            list_temp.append(np.nan)
                    else:
                        list_temp.append(np.nan)
                        
            list_temp2.append(list_temp)
        
        for n in range(len(regno3)):
            
            if isinstance(regno3[n],str) and not isinstance(regno4[n],str):
                mergereg.append(regno3[n])
            
            elif not isinstance(regno3[n],str) and isinstance(regno4[n],str):
                mergereg.append(regno4[n])
                
            elif isinstance(regno3[n],str) and isinstance(regno4[n],str):
                mergereg.append(regno4[n])
            else:
                mergereg.append(np.nan)
        
        df_supervisor = pd.DataFrame(list_temp2,columns=Supervisor_col)
        result = pd.concat([df_supervisor, pd.DataFrame(mergereg,columns=["公司統編"])], axis=1, join='inner').dropna(axis=0, how='all')
        result = result.drop(columns=['序號'])
        result = result.dropna(how="all")
        
        #統編link搬移        
        result["所代表法人"] = np.where(pd.notnull(result["公司統編"])&(result["所代表法人"]==""),result["姓名"],result["所代表法人"])
        result["姓名"] = np.where((result["所代表法人"]==result["姓名"]),"",result["姓名"])
        #國別比對搬移
        country_list = country_list_func()
        result["所代表法人"] = np.where(pd.Series([True if next(((value,key) for key, value in country_list.items() if key.lower() in result["姓名"][m]),None) else False for m in range(len(result))])&(result["所代表法人"]==""), \
              result["姓名"],result["所代表法人"])
        result["姓名"] = np.where((result["所代表法人"]==result["姓名"]),"",result["姓名"])
        
        result["name_Length"] = result["姓名"].str.len()>=5
        
        if result["name_Length"].any():
            
            name_correct_final = []
            for d in range(len(result)):
                
                if result["所代表法人"][d] =="" and result["name_Length"][d]==True:
                    if re.findall(r'[a-z]+',result["姓名"][d][-4:],re.I):
                        for match in CMPname_filter_E:
                            if result["姓名"][d].endswith(match):
                                name_correct_final.append(True)
                                break
                        else:
                            #放特殊匹配的地方。
                            name_correct_final.append(False)
                    else:
                
                        for match1 in Pname_filter:
                            if match1 in result["姓名"][d]:
                                name_correct_final.append(False)
                                break
                        else:
                            
                            for match in CMPname_filter_C:
                                if match in result["姓名"][d]:
                                    name_correct_final.append(True)
                                    break
                            else:
                                #放特殊匹配的地方。
                                name_correct_final.append(False)

                else:
                    name_correct_final.append(False)
                        
            result["所代表法人"] = np.where(pd.Series(name_correct_final),result["姓名"],result["所代表法人"])
            result["姓名"] = np.where((result["所代表法人"]==result["姓名"]),"",result["姓名"])
        else:
            pass
        
        result = result.drop(columns=["name_Length"])
        result = result.dropna(how="all")
        #轉dict()
        return json.loads(result.to_json(orient='index', force_ascii=False))
    else:
        return {}


#取代字串的小函數
def replaceMultiple(mainString, toBeReplaces, newString):
    for elem in toBeReplaces :
        if elem in mainString :
            mainString = mainString.replace(elem, newString,1)
    return  mainString

#20200325
'''
def special_char(value):

    if not isinstance (value,str):
        value = ""
    
    if any(x in value for x in [",",","]):
        value = ""
        engvalue = ""
    else:
        for spe in [["(",")"],["（","）"],["（",")"],["(","）"],["〈","〉"]]:
            if all(x in value for x in spe):
                value_br = list(filter(None,re.split("\\"+spe[0]+"|\\"+spe[1],value)))
                
                if len(value_br)==2:
                        
                    count = 0
                    chinese = []
                    english = []
                    value_br = sorted(value_br,reverse=True)                    
                    for ii in value_br:
                        if u'\u4e00' <= ii <= u'\u9fff':
                            count = count+1
                            chinese.append(ii)
                        else:
                            english.append(ii)
                    if count ==1:
                        value = chinese[0].replace(" ","").strip()
                        engvalue = english[0].strip()
                        
                    elif count ==2:
                        engvalue = ""
                        
                    else:
                        engvalue = value.strip()
                        value = ""
                        
                    break

                elif len(value_br)==3:
                    if not u'\u4e00' <= value_br[1] <= u'\u9fff':
                        chinese_list = re.findall(r'[\u4e00-\u9fff]+', value,re.I)
                        value_eng = replaceMultiple(value, chinese_list, "")
                            
                        value = "".join(chinese_list)
                        replace_char = []
                        value_eng = value_eng.strip()
                        for u in range(len(value_eng)-1,-1,-1):
                            if not re.match(r'[a-z]+',value_eng[u],re.I):
                                replace_char.append(value_eng[u])
                                
                        for u in replace_char:
                            value_eng = value_eng.strip(u)
                            
                        engvalue = value_eng
                        break
                        
                    else:
                        english_list = re.findall(r'[a-z]+',value,re.I)
                        for eng in english_list:
                            value = value.replace(eng,"",1)
                        value = value.strip()
                        replace_char = []
                        for u in range(len(value)-1,-1,-1):
                            if not u'\u4e00' <= value[u] <= u'\u9fff':
                                replace_char.append(value[u])
                                    
                        for u in replace_char:
                            value = value.strip(u)

                        engvalue = " ".join(english_list)
                        break
                else:
                    engvalue = ""
                    break
                    
        else:
            value_br = re.findall(r'[a-z]+',value,re.I)
            if value_br:
                engvalue = ' '.join(list(filter(None,[m for m in value_br])))
                for char in value_br:
                    value = value.replace(char,"",1)
                value = value.strip()
    
                if len(value)==1:
                    if not u'\u4e00' <= value <= u'\u9fff':
                        value = value.replace(value,"").replace(" ","")
                else:
                        
                        #有問題? 因為每取代一個就要減一 長度就會不同。
                    for num in range(len(value)):
                        if not u'\u4e00' <= value[num] <= u'\u9fff':
                            value = value.replace(value[num],"")
                        else:
                            break
                        
                    replace_char2 = []
                    for num in range(len(value)-1,0,-1):
                        if not u'\u4e00' <= value[num] <= u'\u9fff':
                            replace_char2.append(value[num])
                        else:
                            break
                    for u in replace_char2:
                        value = value.strip(u)

                    value = value.strip().replace(" ","")
            else:
                engvalue = ""
    return engvalue,value
'''

#給公司董監事欄位用的姓名切割函數
def special_char(value):
    global chipattern
    if not isinstance (value,str):
        value = ""
        #判斷是不是有括號在字串裡面
    for spe in [["(",")"],["（","）"],["（",")"],["(","）"],["〈","〉"]]:
        if all(x in value for x in spe):
            value_br = list(filter(None,re.split("\\"+spe[0]+"|\\"+spe[1],value)))
            if len(value_br)==2:
                    
                count = 0
                chinese = []
                english = []
                value_br = sorted(value_br,reverse=True)                    
                for ii in value_br:
                    ii = ii.strip()
                    if u'\u4e00' <= ii <= u'\u9fff':
                        count = count+1
                        chinese.append(ii)
                    else:
                        english.append(ii)
                
                if count ==1:
                    value = chinese[0].replace(" ","").strip()
                    engvalue = english[0].strip()
                    
                elif count ==2:
                    engvalue = ""
                    
                else:
                    engvalue = value.strip()
                    value = ""
                    
                break

            elif len(value_br)==3:
                if not u'\u4e00' <= value_br[1] <= u'\u9fff':
                    chinese_list = re.findall(chipattern, value,re.I)
                    value_eng = replaceMultiple(value, chinese_list, "")
                        
                    value = "".join(chinese_list)
                    replace_char = []
                    value_eng = value_eng.strip()
                    for u in range(len(value_eng)-1,-1,-1):
                        if not re.match(r'[a-z]+',value_eng[u],re.I):
                            replace_char.append(value_eng[u])
                            
                    for u in replace_char:
                        value_eng = value_eng.strip(u)
                        
                    engvalue = value_eng
                    break
                    
                else:
                    english_list = re.findall(r'[a-z]+',value,re.I)
                    for eng in english_list:
                        value = value.replace(eng,"",1)
                    value = value.strip()
                    replace_char = []
                    for u in range(len(value)-1,-1,-1):
                        if not u'\u4e00' <= value[u] <= u'\u9fff':
                            replace_char.append(value[u])
                                
                    for u in replace_char:
                        value = value.strip(u)

                    engvalue = " ".join(english_list)
                    break
            else:
                engvalue = ""
                break
                
    else:
        #沒有括號
        value_br = re.findall(r'[a-z]+',value,re.I)
        value_chi = re.findall(chipattern, value,re.I)            
        #只有英
        if value_br and not value_chi:
            engvalue = copy.deepcopy(value)
            non_en_last = []
            non_en_f = []
            for en in range(len(engvalue)):
                if not re.findall(r'[a-z]+',engvalue[en],re.I):
                    non_en_f.append(engvalue[en])
                else:
                    break
            for en in range(len(engvalue)-1,0,-1):
                if not re.findall(r'[a-z]+',engvalue[en],re.I):
                    non_en_last.append(engvalue[en])
                else:
                    break
                
            engvalue = engvalue.replace("".join(non_en_f),"",1)
            if non_en_last:
                engvalue = engvalue[::-1].replace("".join(non_en_last),"",1)[::-1]
            value = copy.deepcopy(engvalue)
        
        #只有中
        elif not value_br and value_chi:
            non_chi_last = []#結尾不是中文
            non_chi_f = []#開頭不是中文

            for chi in range(len(value)):
                
                if not re.findall(chipattern,value[chi],re.I):
                    non_chi_f.append(value[chi])
                else:
                    break
            
            for chi in range(len(value)-1,0,-1):

                if not re.findall(chipattern,value[chi],re.I):
                    non_chi_last.append(value[chi])
                else:
                    break                
            
            value = value.replace("".join(non_chi_f),"",1)
            if non_chi_last:
                value = value[::-1].replace("".join(non_chi_last),"",1)[::-1]
            value = value.replace(" ","").replace("　","").strip()
            engvalue = ""

        #有英有中。中文字串裡會包含非英文的所有部分。
        elif value_br and value_chi:

            engvalue = copy.deepcopy(value)
            chivalue = copy.deepcopy(value)
            non_en_last = []
            non_en_f = []
            for en in range(len(engvalue)):
                if not re.findall(r'[a-z]+',engvalue[en],re.I):
                    non_en_f.append(engvalue[en])
                else:
                    break
            for en in range(len(engvalue)-1,0,-1):
                if not re.findall(r'[a-z]+',engvalue[en],re.I):
                    non_en_last.append(engvalue[en])
                else:
                    break
        
            engvalue = engvalue.replace("".join(non_en_f),"",1)
            if non_en_last:
                engvalue = engvalue[::-1].replace("".join(non_en_last),"",1)[::-1]
            
            value = chivalue.replace(engvalue,"",1).replace(" ","").replace("　","").strip()

        #沒中沒英的其他語言
        else:
            engvalue = ""

    return engvalue,value

#2020/6/22
#抓取前名稱、變更日期(公司、商業、分公司(共用))
def cmptname_chgdate(col_cmpt,restext):
    global chipattern
    col_match_pos = []
    dict_name = {}
    for a in col_cmpt:
        if (re.match("公司名稱", a)):
            col_match_pos.append(col_cmpt.index(a))
    
    if col_match_pos:
        t = etree.HTML(restext).xpath('//*[@id="tabCmpyContent" or @id="tabBusmContent"]/div/table/tbody/tr['+str(col_match_pos[0]+1)+']/td[2]/text()')
    else:
        t = []
        
    if t:
        #字串裡面有發文號跟變更名稱嗎?
        for i in range(len(t)):
            if "發文號" and "變更名稱" in t[i]:
                cmptname_changedate_string = t[i]
                break
        else:
            cmptname_changedate_string = ""
        
        if cmptname_changedate_string:
            cmptname_changedate_string_split = list(filter(None,[j.strip() for j in re.split('\t|\n|\r',cmptname_changedate_string)]))
            
            if len(cmptname_changedate_string_split)==3:
                roccalender = cmptname_changedate_string_split[0]
                chi_stringofdate = re.findall(chipattern,roccalender,re.I)
                
                #處理日期
                if len(chi_stringofdate)==3:
                    for m in chi_stringofdate:
                        roccalender = roccalender.replace(m," ")
                    
                    date_list = re.split(' ',roccalender.strip())
                    date_finished = str(int(date_list[0])+1911)+"-"+ date_list[1]+"-"+date_list[2]
                else:
                    date_finished = ""
                    
                #處理前名稱。
                former_cmptname_org = cmptname_changedate_string_split[2]
                if "前名稱" in former_cmptname_org:
                    
                    former_cmptname_org_split = re.split('：|:',former_cmptname_org)
                    if len(former_cmptname_org_split)==2:
                        former_cmptname_finished = re.findall(chipattern,former_cmptname_org_split[1],re.I)[0]
                    else:
                        former_cmptname_finished = ""
                    
                else:
                    former_cmptname_finished = ""
                    
            else:
                date_finished = ""
                former_cmptname_finished = ""
        else:
            date_finished = ""
            former_cmptname_finished = ""
    else:
        date_finished = ""
        former_cmptname_finished = ""
    
    if date_finished:
        dict_name["CMP_Name_CHG_Date"] = date_finished
        dict_name["CMP_Name_Previous"] = former_cmptname_finished
        return dict_name
    else:
        return dict_name

#大陸公司名稱切割
def cmpt_nameCN_split(Foreigncompanyname):
    dict_name = {}
    if "公司名稱" in Foreigncompanyname.keys():
        value = Foreigncompanyname["公司名稱"]
        
        if all(x in value for x in ["(在臺灣地區公司名稱)","(在大陸地區公司名稱)"]):
            cmpname_tw = value.split("(在臺灣地區公司名稱)")[0]
            campname_cn = value.split("(在臺灣地區公司名稱)")[1].split("(在大陸地區公司名稱)")[0]
            dict_name["公司名稱"] = cmpname_tw
            dict_name["大陸公司名稱"] = campname_cn
            
        elif all(x in value for x in ["臺灣地區","大陸地區"]):
            
            for spe in [["(",")"],["（","）"],["（",")"],["(","）"],["〈","〉"]]:
                if all(xx in value for xx in spe):
                    value_br = list(filter(None,re.split("\\"+spe[0]+"|\\"+spe[1],value)))
                    
                    if len(value_br)==4:
                        for i,s in enumerate(value_br):
                            if "臺灣地區" in s:
                                if (i % 2) != 0:
                                    dict_name["公司名稱"] = value_br[i-1]
                            
                            if "大陸地區" in s:
                                if (i % 2) != 0:
                                    dict_name["大陸公司名稱"] = value_br[i-1]
        else:
            pass
    Foreigncompanyname.update(dict_name)
    return Foreigncompanyname

#國別清單
country_list_sean = {'安圭拉島商': ['安圭拉島', 'Anguilla', '660'],
                     '安圭接商': ['安圭拉島', 'Anguilla', '660'],
                     '英屬安吉拉群島商': ['安圭拉島', 'Anguilla', '660'],
                     '英屬安拉商': ['安圭拉島', 'Anguilla', '660'],
                     '巴哈馬商': ['巴哈馬', 'Bahamas', '44'],
                     '貝理斯商': ['貝里斯', 'Belize', '84'],
                     '英屬百慕達群島商': ['百慕達', 'Bermuda', '60'],
                     '英屬威京群島商': ['英屬維京群島', 'British Virgin Island', 't11'],
                     '英屬維京': ['英屬維京群島', 'British Virgin Island', 't11'],
                     '英屬維群島商': ['英屬維京群島', 'British Virgin Island', 't11'],
                     '英屬維精群島商': ['英屬維京群島', 'British Virgin Island', 't11'],
                     '英屬曼群島商': ['開曼群島', 'Cayman Islands', '136'],
                     '英屬開曼': ['開曼群島', 'Cayman Islands', '136'],
                     '英屬蓋曼群島': ['開曼群島', 'Cayman Islands', '136'],
                     '蓋曼商': ['開曼群島', 'Cayman Islands', '136'],
                     '英署開曼群島商': ['開曼群島', 'Cayman Islands', '136'],
                     '義大利': ['義大利', 'Italy', '380'],
                     '馬來西亞': ['馬來西亞', 'Malaysia', '458'],
                     '馬爾他商': ['馬爾他', 'Malta', '470'],
                     '馬紹爾群島共和國商': ['馬歇爾群島', 'Marshall Islands', '584'],
                     '模里西斯': ['模里西斯', 'Mauritius', '480'],
                     '波蘭商': ['波蘭', 'Poland', '616'],
                     '聖文森及格瑞娜丁商': ['聖文森及格瑞那丁', 'Saint Vincent & the Grenadines', '670'],
                     '蕯摩亞南': ['薩摩亞', 'Samoa', '882'],
                     '蕯摩亞商': ['薩摩亞', 'Samoa', '882'],
                     '薩麻亞商': ['薩摩亞', 'Samoa', '882'],
                     '薩摩亞': ['薩摩亞', 'Samoa', '882'],
                     '薩摩爾商': ['薩摩亞', 'Samoa', '882'],
                     '薩犘亞商': ['薩摩亞', 'Samoa', '882'],
                     '席塞爾商': ['塞席爾', 'Seychelles', '690'],
                     '塞席爾': ['塞席爾', 'Seychelles', '690'],
                     '塞席爾群島商': ['塞席爾', 'Seychelles', '690'],
                     '新加坡': ['新加坡', 'Singapore', '702'],
                     '新家坡商': ['新加坡', 'Singapore', '702'],
                     '韓國': ['南韓', 'South Korea', '408'],
                     '西班商': ['西班牙', 'Spain', '724'],
                     '瑞土商': ['瑞士', 'Switzerland', '756'],
                     '瑞士': ['瑞士', 'Switzerland', '756'],
                     '英屬西印度群島特克及卡克群島': ['Turks & Caicos Islands', 'Turks & Caicos Islands', '796'],
                     '英屬海峽群島商': ['英國', 'United Kingdom', '826'],
                     '英屬曼島商': ['英國', 'United Kingdom', '826'],
                     '美國': ['美國', 'United States', '840']}
#台灣清單
country_list_sean_tw = {'社團法人': ['台灣', 'Taiwan', '158'],
                        '財團法人': ['台灣', 'Taiwan', '158'],
                        '嘉義市': ['台灣', 'Taiwan', '158'],
                        '新竹市': ['台灣', 'Taiwan', '158'],
                        '高雄市': ['台灣', 'Taiwan', '158'],
                        '基隆市': ['台灣', 'Taiwan', '158'],
                        '臺中市': ['台灣', 'Taiwan', '158'],
                        '台中市': ['台灣', 'Taiwan', '158'],
                        '臺南市': ['台灣', 'Taiwan', '158'],
                        '台南市': ['台灣', 'Taiwan', '158'],
                        '臺北市': ['台灣', 'Taiwan', '158'],
                        '台北市': ['台灣', 'Taiwan', '158'],
                        '桃園市': ['台灣', 'Taiwan', '158'],
                        '新北市': ['台灣', 'Taiwan', '158'],
                        '彰化縣': ['台灣', 'Taiwan', '158'],
                        '嘉義縣': ['台灣', 'Taiwan', '158'],
                        '金門縣': ['台灣', 'Taiwan', '158'],
                        '新竹縣': ['台灣', 'Taiwan', '158'],
                        '花蓮縣': ['台灣', 'Taiwan', '158'],
                        '宜蘭縣': ['台灣', 'Taiwan', '158'],
                        '連江縣': ['台灣', 'Taiwan', '158'],
                        '苗栗縣': ['台灣', 'Taiwan', '158'],
                        '南投縣': ['台灣', 'Taiwan', '158'],
                        '澎湖縣': ['台灣', 'Taiwan', '158'],
                        '屏東縣': ['台灣', 'Taiwan', '158'],
                        '臺東縣': ['台灣', 'Taiwan', '158'],
                        '台東縣': ['台灣', 'Taiwan', '158'],
                        '雲林縣': ['台灣', 'Taiwan', '158'],
                        '彰化市': ['台灣', 'Taiwan', '158'],
                        '宜蘭市': ['台灣', 'Taiwan', '158'],
                        '屏東市': ['台灣', 'Taiwan', '158'],
                        '台東市': ['台灣', 'Taiwan', '158'],
                        '溪州鄉': ['台灣', 'Taiwan', '158'],
                        '溪湖鎮': ['台灣', 'Taiwan', '158'],
                        '竹塘鄉': ['台灣', 'Taiwan', '158'],
                        '二林鎮': ['台灣', 'Taiwan', '158'],
                        '二水鄉': ['台灣', 'Taiwan', '158'],
                        '芳苑鄉': ['台灣', 'Taiwan', '158'],
                        '芬園鄉': ['台灣', 'Taiwan', '158'],
                        '福興鄉': ['台灣', 'Taiwan', '158'],
                        '和美鎮': ['台灣', 'Taiwan', '158'],
                        '線西鄉': ['台灣', 'Taiwan', '158'],
                        '秀水鄉': ['台灣', 'Taiwan', '158'],
                        '花壇鄉': ['台灣', 'Taiwan', '158'],
                        '鹿港鎮': ['台灣', 'Taiwan', '158'],
                        '北斗鎮': ['台灣', 'Taiwan', '158'],
                        '埤頭鄉': ['台灣', 'Taiwan', '158'],
                        '埔心鄉': ['台灣', 'Taiwan', '158'],
                        '埔鹽鄉': ['台灣', 'Taiwan', '158'],
                        '伸港鄉': ['台灣', 'Taiwan', '158'],
                        '社頭鄉': ['台灣', 'Taiwan', '158'],
                        '大城鄉': ['台灣', 'Taiwan', '158'],
                        '大村鄉': ['台灣', 'Taiwan', '158'],
                        '田中鎮': ['台灣', 'Taiwan', '158'],
                        '田尾鄉': ['台灣', 'Taiwan', '158'],
                        '員林市': ['台灣', 'Taiwan', '158'],
                        '永靖鄉': ['台灣', 'Taiwan', '158'],
                        '溪口鄉': ['台灣', 'Taiwan', '158'],
                        '竹崎鄉': ['台灣', 'Taiwan', '158'],
                        '中埔鄉': ['台灣', 'Taiwan', '158'],
                        '番路鄉': ['台灣', 'Taiwan', '158'],
                        '新港鄉': ['台灣', 'Taiwan', '158'],
                        '六腳鄉': ['台灣', 'Taiwan', '158'],
                        '鹿草鄉': ['台灣', 'Taiwan', '158'],
                        '梅山鄉': ['台灣', 'Taiwan', '158'],
                        '民雄鄉': ['台灣', 'Taiwan', '158'],
                        '布袋鎮': ['台灣', 'Taiwan', '158'],
                        '朴子市': ['台灣', 'Taiwan', '158'],
                        '水上鄉': ['台灣', 'Taiwan', '158'],
                        '太保市': ['台灣', 'Taiwan', '158'],
                        '大林鎮': ['台灣', 'Taiwan', '158'],
                        '大埔鄉': ['台灣', 'Taiwan', '158'],
                        '東石鄉': ['台灣', 'Taiwan', '158'],
                        '義竹鄉': ['台灣', 'Taiwan', '158'],
                        '尖石鄉': ['台灣', 'Taiwan', '158'],
                        '芎林鄉': ['台灣', 'Taiwan', '158'],
                        '竹北市': ['台灣', 'Taiwan', '158'],
                        '竹東鎮': ['台灣', 'Taiwan', '158'],
                        '橫山鄉': ['台灣', 'Taiwan', '158'],
                        '新豐鄉': ['台灣', 'Taiwan', '158'],
                        '新埔鎮': ['台灣', 'Taiwan', '158'],
                        '湖口鄉': ['台灣', 'Taiwan', '158'],
                        '關西鎮': ['台灣', 'Taiwan', '158'],
                        '峨眉鄉': ['台灣', 'Taiwan', '158'],
                        '寶山鄉': ['台灣', 'Taiwan', '158'],
                        '北埔鄉': ['台灣', 'Taiwan', '158'],
                        '五峰鄉': ['台灣', 'Taiwan', '158'],
                        '吉安鄉': ['台灣', 'Taiwan', '158'],
                        '卓溪鄉': ['台灣', 'Taiwan', '158'],
                        '鳳林鎮': ['台灣', 'Taiwan', '158'],
                        '豐濱鄉': ['台灣', 'Taiwan', '158'],
                        '富里鄉': ['台灣', 'Taiwan', '158'],
                        '新城鄉': ['台灣', 'Taiwan', '158'],
                        '秀林鄉': ['台灣', 'Taiwan', '158'],
                        '瑞穗鄉': ['台灣', 'Taiwan', '158'],
                        '光復鄉': ['台灣', 'Taiwan', '158'],
                        '壽豐鄉': ['台灣', 'Taiwan', '158'],
                        '萬榮鄉': ['台灣', 'Taiwan', '158'],
                        '玉里鎮': ['台灣', 'Taiwan', '158'],
                        '礁溪鄉': ['台灣', 'Taiwan', '158'],
                        '壯圍鄉': ['台灣', 'Taiwan', '158'],
                        '羅東鎮': ['台灣', 'Taiwan', '158'],
                        '南澳鄉': ['台灣', 'Taiwan', '158'],
                        '三星鄉': ['台灣', 'Taiwan', '158'],
                        '蘇澳鎮': ['台灣', 'Taiwan', '158'],
                        '大同鄉': ['台灣', 'Taiwan', '158'],
                        '頭城鎮': ['台灣', 'Taiwan', '158'],
                        '冬山鄉': ['台灣', 'Taiwan', '158'],
                        '五結鄉': ['台灣', 'Taiwan', '158'],
                        '員山鄉': ['台灣', 'Taiwan', '158'],
                        '造橋鄉': ['台灣', 'Taiwan', '158'],
                        '卓蘭鎮': ['台灣', 'Taiwan', '158'],
                        '竹南鎮': ['台灣', 'Taiwan', '158'],
                        '後龍鎮': ['台灣', 'Taiwan', '158'],
                        '西湖鄉': ['台灣', 'Taiwan', '158'],
                        '公館鄉': ['台灣', 'Taiwan', '158'],
                        '苗栗市': ['台灣', 'Taiwan', '158'],
                        '南庄鄉': ['台灣', 'Taiwan', '158'],
                        '三灣鄉': ['台灣', 'Taiwan', '158'],
                        '三義鄉': ['台灣', 'Taiwan', '158'],
                        '獅潭鄉': ['台灣', 'Taiwan', '158'],
                        '大湖鄉': ['台灣', 'Taiwan', '158'],
                        '泰安鄉': ['台灣', 'Taiwan', '158'],
                        '頭份市': ['台灣', 'Taiwan', '158'],
                        '頭屋鄉': ['台灣', 'Taiwan', '158'],
                        '通霄鎮': ['台灣', 'Taiwan', '158'],
                        '銅鑼鄉': ['台灣', 'Taiwan', '158'],
                        '苑裡鎮': ['台灣', 'Taiwan', '158'],
                        '集集鎮': ['台灣', 'Taiwan', '158'],
                        '中寮鄉': ['台灣', 'Taiwan', '158'],
                        '竹山鎮': ['台灣', 'Taiwan', '158'],
                        '信義鄉': ['台灣', 'Taiwan', '158'],
                        '仁愛鄉': ['台灣', 'Taiwan', '158'],
                        '國姓鄉': ['台灣', 'Taiwan', '158'],
                        '鹿谷鄉': ['台灣', 'Taiwan', '158'],
                        '名間鄉': ['台灣', 'Taiwan', '158'],
                        '南投市': ['台灣', 'Taiwan', '158'],
                        '埔里鎮': ['台灣', 'Taiwan', '158'],
                        '水里鄉': ['台灣', 'Taiwan', '158'],
                        '草屯鎮': ['台灣', 'Taiwan', '158'],
                        '魚池鄉': ['台灣', 'Taiwan', '158'],
                        '七美鄉': ['台灣', 'Taiwan', '158'],
                        '西嶼鄉': ['台灣', 'Taiwan', '158'],
                        '湖西鄉': ['台灣', 'Taiwan', '158'],
                        '馬公市': ['台灣', 'Taiwan', '158'],
                        '白沙鄉': ['台灣', 'Taiwan', '158'],
                        '望安鄉': ['台灣', 'Taiwan', '158'],
                        '長治鄉': ['台灣', 'Taiwan', '158'],
                        '潮州鎮': ['台灣', 'Taiwan', '158'],
                        '車城鄉': ['台灣', 'Taiwan', '158'],
                        '佳冬鄉': ['台灣', 'Taiwan', '158'],
                        '九如鄉': ['台灣', 'Taiwan', '158'],
                        '春日鄉': ['台灣', 'Taiwan', '158'],
                        '竹田鄉': ['台灣', 'Taiwan', '158'],
                        '枋寮鄉': ['台灣', 'Taiwan', '158'],
                        '枋山鄉': ['台灣', 'Taiwan', '158'],
                        '恆春鎮': ['台灣', 'Taiwan', '158'],
                        '新埤鄉': ['台灣', 'Taiwan', '158'],
                        '新園鄉': ['台灣', 'Taiwan', '158'],
                        '崁頂鄉': ['台灣', 'Taiwan', '158'],
                        '高樹鄉': ['台灣', 'Taiwan', '158'],
                        '來義鄉': ['台灣', 'Taiwan', '158'],
                        '里港鄉': ['台灣', 'Taiwan', '158'],
                        '麟洛鄉': ['台灣', 'Taiwan', '158'],
                        '林邊鄉': ['台灣', 'Taiwan', '158'],
                        '琉球鄉': ['台灣', 'Taiwan', '158'],
                        '瑪家鄉': ['台灣', 'Taiwan', '158'],
                        '滿州鄉': ['台灣', 'Taiwan', '158'],
                        '牡丹鄉': ['台灣', 'Taiwan', '158'],
                        '南州鄉': ['台灣', 'Taiwan', '158'],
                        '內埔鄉': ['台灣', 'Taiwan', '158'],
                        '三地門鄉': ['台灣', 'Taiwan', '158'],
                        '獅子鄉': ['台灣', 'Taiwan', '158'],
                        '泰武鄉': ['台灣', 'Taiwan', '158'],
                        '東港鎮': ['台灣', 'Taiwan', '158'],
                        '萬巒鄉': ['台灣', 'Taiwan', '158'],
                        '萬丹鄉': ['台灣', 'Taiwan', '158'],
                        '霧臺鄉': ['台灣', 'Taiwan', '158'],
                        '鹽埔鄉': ['台灣', 'Taiwan', '158'],
                        '長濱鄉': ['台灣', 'Taiwan', '158'],
                        '成功鎮': ['台灣', 'Taiwan', '158'],
                        '池上鄉': ['台灣', 'Taiwan', '158'],
                        '金峰鄉': ['台灣', 'Taiwan', '158'],
                        '海端鄉': ['台灣', 'Taiwan', '158'],
                        '關山鎮': ['台灣', 'Taiwan', '158'],
                        '蘭嶼鄉': ['台灣', 'Taiwan', '158'],
                        '綠島鄉': ['台灣', 'Taiwan', '158'],
                        '鹿野鄉': ['台灣', 'Taiwan', '158'],
                        '卑南鄉': ['台灣', 'Taiwan', '158'],
                        '太麻里鄉': ['台灣', 'Taiwan', '158'],
                        '達仁鄉': ['台灣', 'Taiwan', '158'],
                        '大武鄉': ['台灣', 'Taiwan', '158'],
                        '東河鄉': ['台灣', 'Taiwan', '158'],
                        '延平鄉': ['台灣', 'Taiwan', '158'],
                        '二崙鄉': ['台灣', 'Taiwan', '158'],
                        '西螺鎮': ['台灣', 'Taiwan', '158'],
                        '虎尾鎮': ['台灣', 'Taiwan', '158'],
                        '口湖鄉': ['台灣', 'Taiwan', '158'],
                        '古坑鄉': ['台灣', 'Taiwan', '158'],
                        '林內鄉': ['台灣', 'Taiwan', '158'],
                        '崙背鄉': ['台灣', 'Taiwan', '158'],
                        '麥寮鄉': ['台灣', 'Taiwan', '158'],
                        '褒忠鄉': ['台灣', 'Taiwan', '158'],
                        '北港鎮': ['台灣', 'Taiwan', '158'],
                        '水林鄉': ['台灣', 'Taiwan', '158'],
                        '四湖鄉': ['台灣', 'Taiwan', '158'],
                        '臺西鄉': ['台灣', 'Taiwan', '158'],
                        '大埤鄉': ['台灣', 'Taiwan', '158'],
                        '斗六市': ['台灣', 'Taiwan', '158'],
                        '斗南鎮': ['台灣', 'Taiwan', '158'],
                        '土庫鎮': ['台灣', 'Taiwan', '158'],
                        '東勢鄉': ['台灣', 'Taiwan', '158'],
                        '莿桐鄉': ['台灣', 'Taiwan', '158'],
                        '元長鄉': ['台灣', 'Taiwan', '158'],
                        '花蓮市': ['台灣', 'Taiwan', '158'],
                        '阿里山鄉': ['台灣', 'Taiwan', '158'],
                        '金沙鎮': ['台灣', 'Taiwan', '158'],
                        '金城鎮': ['台灣', 'Taiwan', '158'],
                        '金湖鎮': ['台灣', 'Taiwan', '158'],
                        '金寧鄉': ['台灣', 'Taiwan', '158'],
                        '烈嶼鄉': ['台灣', 'Taiwan', '158'],
                        '烏坵鄉': ['台灣', 'Taiwan', '158'],
                        '北竿鄉': ['台灣', 'Taiwan', '158'],
                        '東引鄉': ['台灣', 'Taiwan', '158'],
                        '南竿鄉': ['台灣', 'Taiwan', '158'],
                        '莒光鄉': ['台灣', 'Taiwan', '158'],
                        '釣魚台': ['台灣', 'Taiwan', '158']}

country_list_sean2 = {'馬紹爾群島': ['馬歇爾群島', 'Marshall Islands', '584'],
                      'Seychelles': ['塞席爾', 'Seychelles', '690'],
                      'Samoa': ['薩摩亞', 'Samoa', '882'],
                      'British Virgin Islands': ['開曼群島', 'Cayman Islands', '136'],
                      'Belize': ['貝里斯', 'Belize', '84'],
                      '(BVI)': ['英屬維京群島', 'British Virgin Island', 't11'],
                      '(B.V.I.)': ['英屬維京群島', 'British Virgin Island', 't11'],
                      'Cayman': ['開曼群島', 'Cayman Islands', '136'],
                      '(開曼)': ['開曼群島', 'Cayman Islands', '136'],
                      '(香港)': ['香港', 'Hong Kong', '344'],
                      '株式会社': ['日本', 'Japan', '392'],
                      '株式會社': ['日本', 'Japan', '392']}

country_list2 = country_list_sean2 | country_list_sean_tw | country_list_sean
#dnb資料庫裡的國別清單
def country_list_func():
    dbstring = "Driver={SQL Server};Server=10.252.45.153;Database=AriaDB;Uid=sa;Pwd=codered9;"
    record_select = "select * from OPAL_Country_List"
    country_list = {}
    try:
        conn = pyodbc.connect(dbstring)
        cursor = conn.execute(record_select)
        for distro in cursor.fetchall():
            country_list[distro.Foreign_Branch]=(distro.Opal_Country_Cname,distro.Opal_Country_Ename,distro.Opal_Country_Code)
        
        conn.close()
    except Exception as e:
        print("Unexpected error:" + str(e))
        
    finally:
        pass
        # conn.close()
    return country_list


#20200325修正。
#20210526
#公司董監事欄位加上國別資訊
def supervisor_info_add_country(supervisor_data):
    global chipattern
    def country_func(supervisor_data,country_list,country_list2):
        #當supervisor_data為空，country_result也會是空。
        country_result = {}
        company_name_result = {}
        people_name = {}
        for i in supervisor_data:
            country = {}
            company_name = {}
            people_name_temp = {}
            
            #20200709
            if not isinstance (supervisor_data[i]["所代表法人"],str):
                supervisor_data[i]["所代表法人"] = ""
            
            if supervisor_data[i]["所代表法人"]:
                #若country_list是空，則country_match也會是空
                country_match = [(value,key) for key, value in country_list.items() if key.lower() in supervisor_data[i]["所代表法人"]]
                
                if not country_match:
                    country_match2 = [(value,key) for key, value in country_list2.items() if key.lower() in supervisor_data[i]["所代表法人"]]
                else:
                    country_match2 = []
                
#                [(('美國', 'United States', '840'), '美商')]
                if country_match:
                    country["國別中文"] = country_match[0][0][0]
                    country["國別英文"] = country_match[0][0][1]
                    country["國別代號"] = country_match[0][0][2]
                    supervisor_repla = supervisor_data[i]["所代表法人"].replace(country_match[0][1],"").strip()
                else:
                    if country_match2:
                        country["國別中文"] = country_match2[0][0][0]
                        country["國別英文"] = country_match2[0][0][1]
                        country["國別代號"] = country_match2[0][0][2]
                        supervisor_repla = supervisor_data[i]["所代表法人"]
                    else:
                        
                        country["國別中文"] = ""
                        country["國別英文"] = ""
                        country["國別代號"] = ""
                        supervisor_repla = supervisor_data[i]["所代表法人"]
                    
                if supervisor_data[i]["公司統編"]:
                    company_name["所代表法人中文"] = ""
                    company_name["所代表法人英文"] = ""
                else:
                    for spe in [["(",")"],["（","）"],["（",")"],["(","）"],["〈","〉"]]:
                        if all(x in supervisor_repla for x in spe):
                            value_br = list(filter(None,re.split("\\"+spe[0]+"|\\"+spe[1],supervisor_repla)))
                            
                            for m in range(len(value_br)):
                                if any(xx in value_br[m] for xx in [":","：","國籍"]):
                                    value_br.remove(value_br[m])
                                                                        
                            if ' ' in value_br:
                                value_br.remove(' ')
                            if '  ' in value_br:
                                value_br.remove('  ')
                            
                            #也許出現":","：","國籍"刪減後成長度為一。
                            if len(value_br)==1:
                                if not re.findall(r'[a-z]+',value_br[0],re.I):
                                    company_name["所代表法人中文"] = value_br[0].replace("癵","□")#沒有英文就放中文
                                    company_name["所代表法人英文"] = ""
                                else:
                                    company_name["所代表法人中文"] = ""
                                    company_name["所代表法人英文"] = value_br[0]  
                                break
                            elif len(value_br)==2:
                                count = 0
                                chinese = []
                                english = []
                                value_br = sorted(value_br,reverse=True)
                                
                                for ii in value_br:
                                    if not re.findall(r'[a-z]+',ii,re.I):
                                        count = count+1
                                        chinese.append(ii)
                                    else:
                                        english.append(ii)
                                    
                                if count ==1:
                                    company_name["所代表法人中文"] = chinese[0].replace(" ","").replace("癵","□").strip()
                                    company_name["所代表法人英文"] = english[0].strip()
                                        
                                elif count ==2:
                                    company_name["所代表法人中文"] = supervisor_repla.replace("癵","□")
                                    company_name["所代表法人英文"] = ""
                                        
                                else:
                                    company_name["所代表法人中文"] = supervisor_repla.replace("癵","□")
                                    company_name["所代表法人英文"] = supervisor_repla
                                        
                                break
                            elif len(value_br)==3:
                                count = 0
                                for con in value_br:
                                    if re.findall(r'[a-z]+',con,re.I):
                                        count = count+1

                                if count ==3:
                                    company_name["所代表法人中文"] = supervisor_repla.replace("癵","□")
                                    company_name["所代表法人英文"] = supervisor_repla
                                elif count ==0:
                                    company_name["所代表法人中文"] = supervisor_repla.replace("癵","□")
                                    company_name["所代表法人英文"] = ""
                                else:
                                    company_name["所代表法人中文"] = supervisor_repla.replace("癵","□")
                                    company_name["所代表法人英文"] = supervisor_repla
                                break
                            else:
                                company_name["所代表法人中文"] = supervisor_repla.replace("癵","□")
                                company_name["所代表法人英文"] = supervisor_repla
                                break
                    else:
                        value_test = copy.deepcopy(supervisor_repla)
                        eng_value = re.findall(r'[a-z]+',value_test,re.I)#英
                        chi_value = re.findall(chipattern,value_test)#中
                        jpy_value = re.findall(u"[\u30a0-\u30ff\u3040-\u309f]+",value_test,re.I)#日
                        
                        if jpy_value:
                            if eng_value and not chi_value:
                                company_name["所代表法人中文"] = ''.join(jpy_value).replace("癵","□")
                                company_name["所代表法人英文"] = ' '.join(eng_value)
                                
                            elif chi_value and not eng_value:
                                company_name["所代表法人中文"] = value_test.replace("癵","□")
                                company_name["所代表法人英文"] = ""
                            else:
                                company_name["所代表法人中文"] = value_test.replace("癵","□")
                                company_name["所代表法人英文"] = value_test               
                
                        elif eng_value and chi_value:
                            non_chi_f = []#開頭非中文
                            non_chi_last = []#結尾非中文
                            for num in range(len(value_test)):
                                if not u'\u4e00' <= value_test[num] <= u'\u9fff':
                                    non_chi_f.append(value_test[num])
                                else:
                                    break
                                
                            for num in range(len(value_test)-1,0,-1):
                                if not u'\u4e00' <= value_test[num] <= u'\u9fff':
                                    non_chi_last.append(value_test[num])
                                else:
                                    break                
                
                            repla_nonchi = list(filter(None,[''.join(non_chi_f),''.join(non_chi_last[::-1])]))
                            for repla in repla_nonchi:
                                value_test = value_test.replace(repla,"")
                                
                            if re.findall(r'[a-z]+',value_test,re.I):
                                #混合型
                                company_name["所代表法人中文"] = supervisor_repla.replace("癵","□")
                                company_name["所代表法人英文"] = supervisor_repla
                            else:
                                    
                                #一般型態 中英 或英中，或是英中英? (英中英就放過他吧跳過)
                                #把中文抽出來，然後用中文去取代原字串。
                                chinese_list = re.findall(chipattern,supervisor_repla)
                                for ch in chinese_list:
                                    supervisor_repla = supervisor_repla.replace(ch,"")
                                    
                                #英文的部分，前面的特殊符號刪去，後面的特殊符號都保留然後數數。
                                non_en_last = []
                                non_en_f = []
                
                                for en in range(len(supervisor_repla)):
                                    if not re.findall(r'[a-z]+',supervisor_repla[en],re.I):
                                        non_en_f.append(supervisor_repla[en])
#                                       supervisor_repla = supervisor_repla.replace(supervisor_repla[en],"")
                                    else:
                                        break
                                    
                                for en in range(len(supervisor_repla)-1,0,-1):
                                    if not re.findall(r'[a-z]+',supervisor_repla[en],re.I):
                                        non_en_last.append(supervisor_repla[en])
                                    else:
                                        break               

                                supervisor_repla = supervisor_repla.replace("".join(non_en_f),"",1)
                                
                                if non_en_last:
                                    if non_en_last[-1]==".":
                                        non_en_last = non_en_last[:-1]
                                    non_en_last = "".join(non_en_last)
                                    supervisor_repla = supervisor_repla[::-1].replace("".join(non_en_last),"",1)[::-1]
                                
                                company_name["所代表法人中文"] = "".join(chinese_list).replace("癵","□")
                                company_name["所代表法人英文"] = supervisor_repla
                            
                        elif chi_value:
                            company_name["所代表法人中文"] = supervisor_repla.replace("癵","□")
                            company_name["所代表法人英文"] = ""
                        else:
                            company_name["所代表法人中文"] = ""
                            company_name["所代表法人英文"] = supervisor_repla
                
                country_result[i] = country
                company_name_result[i] = company_name
            
            else:
                country["國別中文"] = ""
                country["國別英文"] = ""
                country["國別代號"] = ""
                company_name["所代表法人中文"] = ""
                company_name["所代表法人英文"] = ""
                country_result[i] = country
                company_name_result[i] = company_name               
                
            if "姓名" in supervisor_data[i].keys():
                englishvalue,chinesevalue = special_char(supervisor_data[i]["姓名"])                
                people_name_temp["姓名中文"] = chinesevalue.replace("癵","□")
                people_name_temp["姓名英文"] = englishvalue
                people_name[i] = people_name_temp
            
            else:
                people_name_temp["姓名中文"] = ""
                people_name_temp["姓名英文"] = ""
                people_name[i] = people_name_temp
            
            supervisor_data[i]["所代表法人"] = supervisor_data[i]["所代表法人"].replace("癵","□")
            supervisor_data[i]["姓名"] = supervisor_data[i]["姓名"].replace("癵","□")
            
        return country_result,company_name_result,people_name
    
    country_list = country_list_func()#dnb國別清單
    country_result,company_name_result,people_name = country_func(supervisor_data,country_list,country_list2)
    
    for m in supervisor_data:
        supervisor_data[m].update(people_name[m])
        supervisor_data[m].update(company_name_result[m])
        supervisor_data[m].update(country_result[m])
    return supervisor_data       


def Convert(lst):
    res_dct = {lst[i]: lst[i + 1] for i in range(0, len(lst), 2)} 
    return res_dct 

#公司基本資料
def cmpt_info(restext):
    try:
        if etree.HTML(restext).xpath('//*[@id="tabCmpy"]/text()'):
            col_cmpt_his_row = len(Cmpy_col(restext))
            dict_temp = {}
            for i in range(col_cmpt_his_row):
                key = etree.HTML(restext).xpath('//*[@id="tabBusmContent" or @id="tabCmpyContent"]/div/table[1]/tbody/tr['+str(i+1)+']/td[1]/text()')[0].strip()
                value = list(filter(None,[m.strip().replace(',', '') for m in etree.HTML(restext).xpath('//*[@id="tabBusmContent" or @id="tabCmpyContent"]/div/table[1]/tbody/tr['+str(i+1)+']/td[2]/text()')]))
                if len(value)>1:
                    for l in range(len(value)):
                        value[l] = value[l].replace('\t', '').replace('\n', '').replace('\r', '').replace('\xa0', '')
                    value = ', '.join(value)
                    dict_temp[key] = value
                elif len(value)==0:
                    dict_temp[key] = ""
                else:
                    dict_temp[key] = value[0]
            dict_temp2 = {k: v for k, v in dict_temp.items() if v and "日期" not in k}
            return dict_temp2
        else:
            return {}
    except:
        return {}


#抓取歷史資料裡的公司基本資料
'''
def history_firstpost(restext,s):
    global headers1
    his_url = "https://findbiz.nat.gov.tw/fts/history/QueryCmpyDetail/queryCmpyDetail.do"
    try:
        if etree.HTML(restext).xpath('//*[@id="tabHistory"]/text()'):
            row_count = len(etree.HTML(restext).xpath('//*[@id="tabHistoryContent"]/div/table[1]/tbody/tr'))
            if row_count>0:
                cmn_js_query = etree.HTML(restext).xpath('//*[@id="tabHistoryContent"]/div/table/tbody/tr['+ str(1)+']'+'/td['+str(2)+']/a')
                if cmn_js_query:
                    payload_his = {"banNo":cmn_js_query[0].get("onclick").split(',')[1].translate({ord("'"): None}), \
                                   "brBanNo":"", \
                                   "cmpyModifNo":cmn_js_query[0].get("onclick").split(',')[0].split("'")[1]}
                    res_his = s.post(his_url, headers = headers1,data = payload_his)
                    return cmpt_info(res_his.text)
                else:
                    return {}
            else:
                return {}
        else:
            return {}
    except:
        return {}

#因為我有抓歷史資料的清單，所以歷史資料有抓到，但是history1顯示n代表有問題。
def history_chg(restext,s):
    b_a = {}
    now = cmpt_info(restext)
    before = history_firstpost(restext,s)
    
    if before:
        b_a["CompanyProfile_History1"] = "Y"
    else:
        b_a["CompanyProfile_History1"] = "N"

    if before and now:
        shared_items = {k: now[k] for k in now if k in before and now[k] == before[k]}
        for key in list(shared_items.keys()):
            if key in now:
                del now[key]
            if key in before:
                del before[key]
        #如果都是空，就會是空集合。
        allkey = set().union(*[now,before])
        
        for i in allkey:
            if i not in now:
                now[i]=""
            if i not in before:
                before[i]=""
                
        if before and now:
            b_a["CompanyProfile_change"] = "Y"
            b_a["CompanyProfile_before"] = before
            b_a["CompanyProfile_after"] = now
            return b_a
        else:
            b_a["CompanyProfile_change"] = "N"
            return b_a
    else:
        return b_a
'''
#抓取歷史異動資料，提供給history_chg函數去使用的小函數。
def history_firstpost(restext,s):
    global headers1
    his_url = "https://findbiz.nat.gov.tw/fts/history/QueryCmpyDetail/queryCmpyDetail.do"
    try:
        if etree.HTML(restext).xpath('//*[@id="tabHistory"]/text()'):
            row_count = len(etree.HTML(restext).xpath('//*[@id="tabHistoryContent"]/div/table[1]/tbody/tr'))#row數數
            if row_count>0:
                cmn_js_query = etree.HTML(restext).xpath('//*[@id="tabHistoryContent"]/div/table/tbody/tr['+ str(1)+']'+'/td['+str(2)+']/a')
                if cmn_js_query:
                    payload_his = {"banNo":cmn_js_query[0].get("onclick").split(',')[1].translate({ord("'"): None}), \
                                   "brBanNo":"", \
                                   "cmpyModifNo":cmn_js_query[0].get("onclick").split(',')[0].split("'")[1]}
                    res_his = s.post(his_url, headers = headers1,data = payload_his)
                    return (cmpt_info(res_his.text),supervisor_info(res_his.text))
                else:
                    return ({},{})
            else:
                return ({},{})
        else:
            return ({},{})
    except:
        return ({},{})

#抓取歷史異動資料
def history_chg(restext,s,supervisor_data):
    b_a = {}
    supervisor_change = {}
    now = cmpt_info(restext)
    before,supervisor_before = history_firstpost(restext,s)
    
    if before:
        b_a["CompanyProfile_History1"] = "Y"
    else:
        b_a["CompanyProfile_History1"] = "N"

    if supervisor_before:
        b_a["Supervisors_History1"] = "Y"
    else:
        b_a["Supervisors_History1"] = "N"

    if before and now:
        shared_items = {k: now[k] for k in now if k in before and now[k] == before[k]}
        for key in list(shared_items.keys()):
            if key in now:
                del now[key]
            if key in before:
                del before[key]
        #如果都是空，就會是空集合。
        allkey = set().union(*[now,before])
        
        for i in allkey:
            if i not in now:
                now[i]=""
            if i not in before:
                before[i]=""
        if before and now:
            b_a["CompanyProfile_change"] = "Y"
            b_a["CompanyProfile_after"] = now
            b_a["CompanyProfile_before"] = before

        else:
            b_a["CompanyProfile_change"] = "N"
    else:
        b_a["CompanyProfile_change"] = "N"

    if supervisor_data and supervisor_before:
        supervisor_data_df = pd.DataFrame.from_dict(supervisor_data).T
        supervisor_data_df = supervisor_data_df[supervisor_data_df.columns[::-1]]
        supervisor_data_df = supervisor_data_df.drop(columns=['公司統編'])
        supervisor_data_df = supervisor_data_df.rename(index=lambda s: 'Supervisors_After_'+s)

        supervisor_data_df2 = pd.DataFrame.from_dict(supervisor_before).T
        supervisor_data_df2 = supervisor_data_df2[supervisor_data_df2.columns[::-1]]
        supervisor_data_df2 = supervisor_data_df2.drop(columns=['公司統編'])
        supervisor_data_df2 = supervisor_data_df2.rename(index=lambda s: 'Supervisors_Before_'+s)
        supervisor_diff = pd.concat([supervisor_data_df,supervisor_data_df2]).drop_duplicates(keep=False)
        supervisor_diff_dict = json.loads(supervisor_diff.to_json(orient='index', force_ascii=False))
        #若{}代表是空的。代表前後沒有異動，所以都被去重複去掉了。
        if supervisor_diff_dict:
            supervisor_change.update({"supervisor_change":"Y"})
        else:
            supervisor_change.update({"supervisor_change":"N"})
    else:
        supervisor_change.update({"supervisor_change":"N"})
        supervisor_diff_dict = {}
    return b_a,supervisor_change,supervisor_diff_dict


#欄位名稱對照字典
mapping_dict = {'公司屬性': 'CMP_Type',
                '公司名稱': 'CMP_Cname',
                "大陸公司名稱":"CMP_Cname_CN",
                '公司名稱英文': 'CMP_Ename',
                '公司名稱日文': 'CMP_JPname',
                '股權狀況': 'Equity_Status',
                '負責人姓名中文': 'Responsible_Name_CHI_Loop',
                "負責人姓名_LATIN":"Responsible_Name_ENG_Loop",
                "代表人姓名_LATIN":"Responsible_Name_ENG",
                "在中華民國境內負責人_LATIN":"Responsible_Name_ENG",
                "在中華民國境內代表人_LATIN":"Responsible_Name_ENG",
                "訴訟及非訴訟代理人姓名_LATIN":"Responsible_Name_ENG",
                "代表人姓名中文":"Responsible_Name_CHI",
                "在中華民國境內負責人中文":"Responsible_Name_CHI",
                "訴訟及非訴訟代理人姓名中文":"Responsible_Name_CHI",
                "在中華民國境內代表人中文":"Responsible_Name_CHI",
                "代表人姓名":"Responsible_Name_ORG",
                "負責人姓名":"Responsible_Name_ORG",
                "在中華民國境內負責人":"Responsible_Name_ORG",
                "訴訟及非訴訟代理人姓名":"Responsible_Name_ORG",
                "在中華民國境內代表人":"Responsible_Name_ORG",
                "公司出進口廠商英文名稱":"ImEx_CMP_Ename",
                "分公司出進口廠商英文名稱":"BR_CMP_Ename",
                "章程所訂外文公司名稱":"Charter_CMP_EName",
                "分公司經理姓名_LATIN":"BR_Responsible_Name_ENG",
                "分公司經理姓名中文":"BR_Responsible_Name_CHI",
                "分公司經理姓名":"BR_Responsible_Name_ORG",
                "總機構統一編號":"Business_HQ_RegNo",
                "總(本)商業名稱":"Business_HQ_Name",
                "分支機構經理人姓名_LATIN":"Responsible_Name_ENG",
                "分支機構經理人姓名中文":"Responsible_Name_CHI",
                "分支機構經理人姓名":"Responsible_Name_ORG",
                "合夥人姓名":"Partner_Name_ORG",
                "合夥人姓名中文":"Partner_Name_CHI_Loop",
                "合夥人姓名_LATIN":"Partner_Name_ENG_Loop",
                "每股金額(元)":"Share_Value",
                "已發行股份總數(股)":"Equity_Amount",
                "現況":"Status",
                "分支機構現況" : "Status_of_Branch",
                "停業起日期" : "Suspended_From",
                "停業迄日期" : "Suspended_End",
                "歇業日期" : "Dissolved_Since",
                "公司狀況":"Status",
                "分公司狀況":"Status_of_Branch"}

mapping_dict_loop = {"Responsible_Name_CHI_Loop":{'出資額(元)':'Responsible_Capital','姓名':'Responsible_Name_CHI'},
                    "Responsible_Name_ENG_Loop":{'出資額(元)':'Responsible_Capital','姓名':'Responsible_Name_ENG'},
                    "Partner_Name_CHI_Loop":{'出資額(元)':'Partner_Capital','姓名':'Partner_Name_CHI'},
                    "Partner_Name_ENG_Loop":{"姓名_LATIN":"Partner_Name_ENG",'出資額(元)':'Partner_Capital'},
                    "BR_Responsible_Name_CHI":{'姓名':"BR_Responsible_Name_CHI"},
                    "BR_Responsible_Name_ENG":{'姓名_LATIN':"BR_Responsible_Name_ENG"}}
#mapping董監事欄位名稱
def mapsupervisor(supervisor_data):
    new_dict = {}
    mapdict = {"職稱":"Person_Position","姓名":"Person_Name_ORG","姓名中文":"Person_Name_CHI", \
               "姓名英文":"Person_Name_ENG","所代表法人":"Juristic_Person_Name_ORG", \
               "所代表法人中文":"Juristic_Person_Name_CHI","所代表法人英文":"Juristic_Person_Name_ENG", \
               "公司統編":"Juristic_Person_RegNo","持有股份數":"Shares", \
               "出資額":"Capital","國別中文":"Country_CHI","國別英文":"Country_ENG","國別代號":"Country_Code","持有股份數(股)":"Shares","出資額(元)":"Capital"}
    for i in supervisor_data.keys():
        oldsingledict = supervisor_data[i]
        new_dict[i] = dict((mapdict[key], value) for (key, value) in oldsingledict.items())
    return new_dict

#mapping欄位名稱
def mapkey(oldsingledict,mapdict):    
    collist = list(set(oldsingledict).intersection(set(mapdict)))
    collist = sorted(collist)
    for key in collist:
        if key in mapdict.keys():
            oldsingledict[mapdict[key]] = oldsingledict.pop(key)#20210316 排序
    return oldsingledict

'''
def mapcmpt(result,mapping_dict_loop):
    for i in result.keys():
        if i in mapping_dict_loop.keys() and type(result[i])==list:
            templist = result[i]
            new_list = []
            for m in range(len(templist)):
                tempdict = templist[m]
                new_list.append(dict((mapping_dict_loop[i][key], value) for (key, value) in tempdict.items()))
            result[i] = new_list
    return result
'''

#2021/03/18 mapping欄位名稱
def mapcmpt(result,mapping_dict_loop):
    for i in result.keys():
        if i in mapping_dict_loop.keys() and type(result[i])==list:
            templist = result[i]
            new_list = []
            for m in range(len(templist)):
                tempdict = templist[m]
                new_list.append(dict((mapping_dict_loop[i][key], value) for (key, value) in tempdict.items()))

                if any("Partner" in value for value in list(new_list[m].keys())):
                    new_list[m].update({"Partner_Person_Position":"合夥人"})
                
                if any("Responsible" in value for value in list(new_list[m].keys())):
                    new_list[m].update({"Responsible_Position":"負責人"})               
                
            result[i] = new_list
    return result


#41657016
#25121939

def replace_right(source, target, replacement, replacements=None):
    return replacement.join(source.rsplit(target, replacements))



#-----------------------------------------------------------------------------------------------------------------


if __name__ == '__main__':
    start = str(datetime.datetime.now())
    #統編輸入限制8碼
    GUI = "20019490"
    if len(GUI)==8:
        try:
            s = requests.Session()
            s.keep_alive = False
            headers = {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.108 Safari/537.36", \
                       "Host":"findbiz.nat.gov.tw", \
                       "Sec-Fetch-User":"?1", \
                       "Upgrade-Insecure-Requests":"1", \
                       "Sec-Fetch-Mode":"navigate", \
                       "Sec-Fetch-Site":"same-origin", \
                       "Sec-Fetch-Dest":"document", \
                       "Connection":"keep-alive", \
                       "Cache-Control":"max-age=0", \
                       "Accept-Language":"zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7"}
            
            headers1 = {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.108 Safari/537.36", \
                        "Referer":"https://findbiz.nat.gov.tw/fts/query/QueryList/queryList.do"}
            
            urls = ["https://findbiz.nat.gov.tw/fts/query/QueryBar/queryInit.do", \
                    "https://findbiz.nat.gov.tw/fts/query/QueryList/queryList.do"]
            
            s.get(urls[0] , headers = headers ,verify=False)
            payload = {
                    "validatorOpen":"N",
                    "rlPermit":"0",
                    "qryCond":GUI, #統一編號
                    "infoType":"D", #名稱或統一編號或工廠登記編號
                    "qryType":"cmpyType", #公司
                    "cmpyType":"true",
                    "qryType":"brCmpyType", #分公司
                    "brCmpyType":"true",
                    "qryType":"busmType", #商業
                    "busmType":"true",
                    "factType":"",
                    "lmtdType":"",
                    "isAlive":"all", #登記現況
                    }
            
            #time out
            time.sleep(4)
            res = s.post(urls[1], data = payload, headers = headers1, timeout = 40)
            #搜尋後結果有幾個連結
            link_sec2 = list(set(["https://findbiz.nat.gov.tw"+i.get("href") for i in etree.HTML(res.text).xpath('//a[@class="hover"]')]))
            
#            time.sleep(4)
            if link_sec2:
                result = {}
                cmpt_attributes_list = []
                cmptnamechgdate_list = []
                msg_list = []
                result_list = []
                
                for li in link_sec2:
                    msg_list_temp = []
                    
                    time.sleep(2.5)
                    #分公司
                    if "QueryBrCmpyDetail" in li:
                        res = s.get(li, headers = headers1)
                        imoutport = list(filter(None,[j.strip() for j in etree.HTML(res.text).xpath('//*[@id="linkMoea"]/text()')]))#進出口資料
                        col_cmpt = Cmpy_col(res.text)#欄位名稱
                        cmpt_attributes = etree.HTML(res.text).xpath('//*[@id="tabCmpy" or @id="tabBusm"]/text()')
                    
                        if cmpt_attributes:
                            cmpt_attributes_list.append(cmpt_attributes[0])
                            msg_list_temp.append('CMP_Type Success')
#                            cmpt_attribute = {"公司屬性":cmpt_attributes[0]}
                        else:
                            msg_list_temp.append('CMP_Type missing')
                            pass
#                            cmpt_attribute = {}
                    
                        if col_cmpt:
                            col_match_pos = col_cmpt_match(col_cmpt)#欄位位置比對
                            manager_name = name_2json(col_match_pos,res.text)#姓名切割
                            stock_right = col_cmpt_match_shareholder(col_cmpt,res.text)#抓取股權資料
                            Foreign_Company_Name = col_cmpt_Otherinfo(col_cmpt,res.text)#切割公司中英日名
                            Foreign_Company_Name = cmpt_nameCN_split(Foreign_Company_Name)#大陸公司名切割
                            cmptnamechgdate = cmptname_chgdate(col_cmpt,res.text)#抓取前名稱變更日期
                            cmptnamechgdate_list.append(cmptnamechgdate)#公司改變前名稱
                            msg_list_temp.append("CMP_Profile Success")
                            #抓統編變更
                            reg_match_pos = col_cmpt_match_reg(col_cmpt)
                            regchangedict = regchange_records(reg_match_pos,res.text)
                        
                        else:
                            possible_error_msglist = etree.HTML(res.text).xpath('//tr/td/span/text()')
                            if possible_error_msglist:
                                possible_error_msglist = list(filter(None,[l.replace('\t','').replace('\n', '').replace('\r', '').replace('\xa0', '') for l in possible_error_msglist]))
                                if possible_error_msglist:
                                    possible_error = ', '.join(possible_error_msglist)
                                    print(possible_error)
                                else:
                                    possible_error = "CMP_Profile missing"
                            else:
                                possible_error = "CMP_Profile missing"
                            
                            msg_list_temp.append(possible_error)
                            print("Company info's columns are blank")
                            manager_name = {}
                            Foreign_Company_Name = {}
                            stock_right = {}
                            regchangedict = {"CMP_EXRegNo_CHG_Date":"","CMP_EX_RegNo":"","CMP_EXRegNo_Log":"無法定位基本資料欄位位置"}
                        
                        #有進出口資料後處理
                        if imoutport:
                            if imoutport[0][0]=="(" and imoutport[0][-1]==")":
                                imoutport_string = imoutport[0].replace("(", '', 1)
                                imoutport_string = replace_right(imoutport_string, ")", "", 1)
                                
                            elif imoutport[0][0]=="(" and imoutport[0][-1]!=")":
                                imoutport_string = imoutport[0].replace("(", '', 1)
                            
                            elif imoutport[0][0]!="(" and imoutport[0][-1]==")":
                                imoutport_string = replace_right(imoutport[0], ")", "", 1)
                            else:
                                imoutport_string = imoutport[0]
                                
                            imoutport_splt = re.split('：|:',imoutport_string)
                            if len(imoutport_splt)==2:
                                imoutport_dict = Convert(imoutport_splt)
                                if '出進口廠商英文名稱' in imoutport_dict.keys():
                                    imoutport_dict['分公司出進口廠商英文名稱'] = imoutport_dict.pop('出進口廠商英文名稱')
                            else:
                                imoutport_dict = {}
                                
                        else:
                            imoutport_dict = {}

#                        result.update(cmpt_attribute)
                        result.update(manager_name)
                        result.update(stock_right)
                        result.update(imoutport_dict)
                        result.update(Foreign_Company_Name)
                        result.update(regchangedict)
                        # break

                    #公司、商業
                    elif "QueryCmpyDetail" in li or "QueryBusmDetail" in li:
                        res = s.get(li, headers = headers1)
                        imoutport = list(filter(None,[j.strip() for j in etree.HTML(res.text).xpath('//*[@id="linkMoea"]/text()')]))
                        col_cmpt = Cmpy_col(res.text)
                        cmpt_attributes = etree.HTML(res.text).xpath('//*[@id="tabCmpy"or @id="tabBusm"]/text()')
                    
                        if cmpt_attributes:
                            cmpt_attributes_list.append(cmpt_attributes[0])
#                            cmpt_attribute = {"公司屬性":cmpt_attributes[0]}
                            msg_list_temp.append('CMP_Type Success')
                        else:
                            msg_list_temp.append('CMP_Type missing')
                            pass
#                            cmpt_attribute = {}
                    
                        if col_cmpt:
                            col_match_pos = col_cmpt_match(col_cmpt)
                            manager_name = name_2json(col_match_pos,res.text)
                            stock_right = col_cmpt_match_shareholder(col_cmpt,res.text)
                            Foreign_Company_Name = col_cmpt_Otherinfo(col_cmpt,res.text)
                            Foreign_Company_Name = cmpt_nameCN_split(Foreign_Company_Name)

                            cmptnamechgdate = cmptname_chgdate(col_cmpt,res.text)
                            cmptnamechgdate_list.append(cmptnamechgdate)
                            msg_list_temp.append("CMP_Profile Success")
                            
                            #抓統編變更
                            reg_match_pos = col_cmpt_match_reg(col_cmpt)
                            regchangedict = regchange_records(reg_match_pos,res.text)                           
                            
                        else:
                            #沒有公司資料
                            possible_error_msglist = etree.HTML(res.text).xpath('//tr/td/span/text()')
                            if possible_error_msglist:
                                possible_error_msglist = list(filter(None,[l.replace('\t','').replace('\n', '').replace('\r', '').replace('\xa0', '') for l in possible_error_msglist]))
                                if possible_error_msglist:
                                    possible_error = ', '.join(possible_error_msglist)
                                    print(possible_error)
                                else:
                                    possible_error = "CMP_Profile missing"
                            else:
                                possible_error = "CMP_Profile missing"
                            
                            msg_list_temp.append(possible_error)
                            print("Company info's columns are blank")
                            manager_name = {}
                            stock_right = {}
                            Foreign_Company_Name = {}
                            regchangedict = {"CMP_EXRegNo_CHG_Date":"","CMP_EX_RegNo":"","CMP_EXRegNo_Log":"無法定位基本資料欄位位置"}
                            
                        if imoutport:
                            if imoutport[0][0]=="(" and imoutport[0][-1]==")":
                                imoutport_string = imoutport[0].replace("(", '', 1)
                                imoutport_string = replace_right(imoutport_string, ")", "", 1)
                                
                            elif imoutport[0][0]=="(" and imoutport[0][-1]!=")":
                                imoutport_string = imoutport[0].replace("(", '', 1)
                            
                            elif imoutport[0][0]!="(" and imoutport[0][-1]==")":
                                imoutport_string = replace_right(imoutport[0], ")", "", 1)
                            else:
                                imoutport_string = imoutport[0]
                                
                            imoutport_splt = re.split('：|:',imoutport_string)
                            if len(imoutport_splt)==2:
                                imoutport_dict = Convert(imoutport_splt)
                                if '出進口廠商英文名稱' in imoutport_dict.keys():
                                    imoutport_dict['公司出進口廠商英文名稱'] = imoutport_dict.pop('出進口廠商英文名稱')
                                    #20220505
                                    imoutport_dict['公司出進口廠商英文名稱'] = " ".join(imoutport_dict['公司出進口廠商英文名稱'].split())
                                    
                            else:
                                imoutport_dict = {}
                                
                        else:
                            imoutport_dict = {}
                        
                        history = hyper_history(res.text,s)
                        supervisor_data = supervisor_info(res.text)
#                        history_change = history_chg(res.text,s)
                        company_info_change,supervisor_change_YN,supervisor_change = history_chg(res.text,s,supervisor_data)
#                        必須放在歷史變動後。可是如果把歷史變動的區塊拆開就無須在意順序位置。
                        supervisor_data = supervisor_info_add_country(supervisor_data)
                        supervisor_data = mapsupervisor(supervisor_data)
                        
                        if supervisor_data:#轉換董監事資料的格式
                            for ke in list(supervisor_data.keys()):
                                key_order = sorted(supervisor_data[ke].keys(), key=lambda x:x.lower())
                                order_dict = {}
                                for kee in key_order:
                                    order_dict[kee] = supervisor_data[ke][kee]            
                                supervisor_data[ke] = order_dict
                        
                        for ID in supervisor_data.keys():
                            supervisor_data[ID]["ID"] = ID
                        supervisor_data = {"EXECUTIVES":[value for value in supervisor_data.values()]}
#                        result.update(cmpt_attribute)
                        result.update(manager_name)
                        result.update(stock_right)
                        result.update(imoutport_dict)
                        result.update(Foreign_Company_Name)
                        result.update(supervisor_data)
                        result.update(history)
                        result.update(company_info_change)
                        result.update(supervisor_change_YN)
                        result.update(supervisor_change)
                        result.update(regchangedict)

                    else:
                        msg_list_temp.append("The Essential String in the url is changed")
                        manager_name = {}
                        stock_right = {}
                    
                    msg_list.append(' & '.join(msg_list_temp))
                    
                    #很重要deepcopy 沒有用會錯
                    resultcopy = copy.deepcopy(result)
                    result_list.append(resultcopy)
                    
                    # print(result)
                    # print("----------------------------------------------------")
                
                
                # print(result_list)
                # print("test")
                
                msg_dict = {"Messages":','.join(msg_list)}
                if cmpt_attributes_list:
                    if len(cmpt_attributes_list)>1:
                        for n in range(len(cmpt_attributes_list)):
                            if "分公司" in cmpt_attributes_list[n]:
                                cmpt_attributes_list.append(cmpt_attributes_list.pop(cmpt_attributes_list.index(cmpt_attributes_list[n])))
                                break
                    cmpt_attribute = {"公司屬性":''.join(cmpt_attributes_list)}
                else:
                    cmpt_attribute = {"公司屬性":''}
                
                #是否有現況欄位
                Current_status = 0
                for jss in range(len(result_list)):
                    if "現況" in result_list[jss].keys():
                        Current_status = Current_status+1
                
                if Current_status > 1:
                    #20200622
                    if cmptnamechgdate_list:
                        cmptnamechgdate_list = list(filter(None, cmptnamechgdate_list))
                        if cmptnamechgdate_list:
                            for js in range(len(result_list)):
                                result_list[js].update(cmptnamechgdate_list[0])
                    
                    # mapping_status = {"公司狀況":"現況"}
                    for js in range(len(result_list)):
                        # result_list[js] = mapkey(result_list[js],mapping_status)
                        result_list_temp = result_list[js]["現況"]
                        cmpstatus = list(filter(None,[j.strip() for j in re.split('\t|\n|\r',result_list_temp)]))
                        
                        if cmpstatus:
                            if cmpstatus[0]=="停業":
                                
                                result_list[js]["現況"] = "停業"
                                try:
                                    
                                    startval = re.sub("[^0-9]", "", cmpstatus[2])
                                    startval2 = str(int(startval[:3])+1911) + "-" + startval[3:5] + "-" + startval[5:]
                                    
                                    endval = re.sub("[^0-9]", "", cmpstatus[4])
                                    endval2 = str(int(endval[:3])+1911) + "-" + endval[3:5] + "-" + endval[5:]
                                except:
                                    startval2 = ""
                                    endval2 = ""
                                
                                result_list[js]["停業起日期"] = startval2
                                result_list[js]["停業迄日期"] = endval2
    
                            else:#現況欄位的不同內容
                                for ww in ["廢止","廢止登記","解散","撤銷","廢止登記已清算完結","撤回登記已清算完結","破產","撤回登記","合併解散","破產程序終結(終止)","撤銷公司設立","解散已清算完結","廢止已清算完結"]:
                                    if cmpstatus[0]== ww:
                                        result_list[js]["現況"] = ww
                                        try:
                                            otherval = re.sub("[^0-9]", "", cmpstatus[1])
                                            if otherval:#歇業日期
                                                otherval2 = str(int(otherval[:3])+1911) + "-" + otherval[3:5] + "-"  +  otherval[5:]
                                                result_list[js]["歇業日期"] = otherval2
                                            else:
                                                otherval2 = ""
                                        except:
                                            otherval2 = ""
                                        
                                        break
                                else:
                                    pass
                        # print(js)
                        result_list[js].update(msg_dict)
                        result_list[js].update({"Num_record(s)":len(link_sec2)})
                        result_list[js].update(cmpt_attribute)
                        
                        # print("------------------------")
                        
                        # print(result_list[js])
                        
                        result_list[js] = mapkey(result_list[js],mapping_dict)
                        result_list[js] = mapcmpt(result_list[js],mapping_dict_loop)
                        
                        if "Charter_CMP_EName" in result_list[js].keys():
                            result_list[js]["Charter_CMP_EName"] = ' '.join(result_list[js]["Charter_CMP_EName"].split())
                    
                        if "Responsible_Name_CHI" in result_list[js].keys():
                            result_list[js]["Responsible_Name_CHI"] = result_list[js]["Responsible_Name_CHI"].translate({ord(c): None for c in string.whitespace})
                    
                        if "Responsible_Name_CHI_Loop" in result_list[js].keys():
                        
                            for sp in range(len(result_list[js]["Responsible_Name_CHI_Loop"])):
                                result_list[js]["Responsible_Name_CHI_Loop"][sp]["Responsible_Name_CHI"] = result_list[js]["Responsible_Name_CHI_Loop"][sp]["Responsible_Name_CHI"].translate({ord(c): None for c in string.whitespace})         
                    
                    print(json.dumps(result_list, ensure_ascii=False))
                
                else:
                    #20200622 公司名變更日
                    if cmptnamechgdate_list:
                        cmptnamechgdate_list = list(filter(None, cmptnamechgdate_list))
                        if cmptnamechgdate_list:
                            result.update(cmptnamechgdate_list[0])

                    mapping_status = {"公司狀況":"現況"}
                    result = mapkey(result,mapping_status)
                    
                    # print(result)
                    # print("-------------------------------------------------------------------")
                    
                    if "現況" in result.keys():
                        cmpstatus = list(filter(None,[j.strip() for j in re.split('\t|\n|\r',result["現況"])]))

                        if cmpstatus:
                            
                            if cmpstatus[0]=="停業":
                                
                                result["現況"] = "停業"
                                try:
                                    startval = re.sub("[^0-9]", "", cmpstatus[2])
                                    startval2 = str(int(startval[:3])+1911) + "-" + startval[3:5] + "-" + startval[5:]
                                    
                                    endval = re.sub("[^0-9]", "", cmpstatus[4])
                                    endval2 = str(int(endval[:3])+1911) + "-" + endval[3:5] + "-" + endval[5:]
                                except:
                                    startval2 = ""
                                    endval2 = ""
                                
                                result["停業起日期"] = startval2
                                result["停業迄日期"] = endval2
                            
                            else:
                                for ww in ["廢止","廢止登記","解散","撤銷","廢止登記已清算完結","撤回登記已清算完結","破產","撤回登記","合併解散","破產程序終結(終止)","撤銷公司設立","解散已清算完結","廢止已清算完結"]:
                                    if cmpstatus[0]== ww:
                                        result["現況"] = ww
                                        try:
                                            otherval = re.sub("[^0-9]", "", cmpstatus[1])
                                            if otherval:
                                                otherval2 = str(int(otherval[:3])+1911) + "-" + otherval[3:5] + "-"  +  otherval[5:]
                                                result["歇業日期"] = otherval2
                                            else:
                                                otherval2 = ""
                                        except:
                                            otherval2 = ""
                                        
                                        break
                                else:
                                    pass

                    result.update(msg_dict)
                    result.update({"Num_record(s)":len(link_sec2)})#有多少連結
                    result.update(cmpt_attribute)
                    result = mapkey(result,mapping_dict)#mapping key
                    result = mapcmpt(result,mapping_dict_loop)#mapping key
                    
                    if "Charter_CMP_EName" in result.keys():
                        result["Charter_CMP_EName"] = ' '.join(result["Charter_CMP_EName"].split())
                    
                    #20220321
                    if "Responsible_Name_CHI" in result.keys():
                        result["Responsible_Name_CHI"] = result["Responsible_Name_CHI"].translate({ord(c): None for c in string.whitespace})
                    
                    #20220413
                    if "Responsible_Name_CHI_Loop" in result.keys():
                        for sp in range(len(result["Responsible_Name_CHI_Loop"])):
                            if len(result["Responsible_Name_CHI_Loop"])==1 :
                                if not "Responsible_Name_CHI" in result["Responsible_Name_CHI_Loop"][0].keys():
                                    result["Responsible_Name_CHI_Loop"][0]["Responsible_Name_CHI"] = ""
                                else:
                                    result["Responsible_Name_CHI_Loop"][0]["Responsible_Name_CHI"] = result["Responsible_Name_CHI_Loop"][0]["Responsible_Name_CHI"].translate({ord(c): None for c in string.whitespace})
                            else:
                                result["Responsible_Name_CHI_Loop"][sp]["Responsible_Name_CHI"] = result["Responsible_Name_CHI_Loop"][sp]["Responsible_Name_CHI"].translate({ord(c): None for c in string.whitespace})

                    if "Responsible_Name_CHI_Loop" in result.keys():
                        delflag = False
                        for sp in range(len(result["Responsible_Name_CHI_Loop"])):
                            
                            if result["Responsible_Name_CHI_Loop"][sp]["Responsible_Name_CHI"]== "癵癵□" and len(result["Responsible_Name_CHI_Loop"][sp]["Responsible_Name_CHI"])==3:
                                delflag = True
                                del result["Responsible_Name_CHI_Loop"][sp]
                                break
                            
                        if delflag:
                            for sp in range(len(result["Responsible_Name_CHI_Loop"])):
                                if "癵癵□" in result["Responsible_Name_CHI_Loop"][sp]["Responsible_Name_CHI"] and len(result["Responsible_Name_CHI_Loop"][sp]["Responsible_Name_CHI"])>7:
                                    ss = result["Responsible_Name_CHI_Loop"][sp]["Responsible_Name_CHI"]
                                    ss = ss.replace("癵", "□")
                                    #識別中文
                                    if re.findall(chipattern,ss,re.I):
                                        #確定只有一個中文在裡面
                                        if len(re.findall(chipattern,ss,re.I))==1:
                                            result["Responsible_Name_CHI_Loop"][sp]["Responsible_Name_CHI"] = re.findall(chipattern,ss,re.I)[0] + "□"              
                       
                    print(json.dumps(result, ensure_ascii=False))

            else:
                possible_error_msglist = etree.HTML(res.text).xpath('//tr/td/span/text()')
                if possible_error_msglist:
                    possible_error_msglist = list(filter(None,[l.replace('\t','').replace('\n', '').replace('\r', '').replace('\xa0', '') for l in possible_error_msglist]))
                    if possible_error_msglist:
                        possible_error = ', '.join(possible_error_msglist)
                        Err_String = {'Message': possible_error}
                        print(possible_error)
                        print(json.dumps({"Messages":possible_error}, ensure_ascii=False))
                    else:
                        Err_String = {'Message': "Can not find results that match the input"}
                        print("Can not find results that match the input")
                        print(json.dumps({"Messages":"Can not find results that match the input"}, ensure_ascii=False))
                else:
                    Err_String = {'Message': "Can not find results that match the input"}
                    print("Can not find results that match the input")
                    print(json.dumps({"Messages":"Can not find results that match the input"}, ensure_ascii=False))
                
        except Exception as e:
            error_class = e.__class__.__name__ #取得錯誤類型
            detail = e.args[0] #取得詳細內容
            cl, exc, tb = sys.exc_info() #取得Call Stack
            lastCallStack = traceback.extract_tb(tb)[-1] #取得Call Stack的最後一筆資料
            lineNum = lastCallStack[1] #取得發生的行號
            funcName = lastCallStack[2] #取得發生的函數名稱
            errMsg = "File \line {}, in {}: [{}] {}".format(lineNum, funcName, error_class, detail)
            Err_String = {'Message': errMsg}
            print(errMsg)
            print(json.dumps({"Messages":errMsg}, ensure_ascii=False))
    else:
        print("Input length is less than 8")
        Err_String = {'Message': "Input length is less than 8"}
        print(json.dumps({"Messages":"Input length is less than 8"}, ensure_ascii=False))
        
    print("Start : " + start)
    print("End   : " + str(datetime.datetime.now()))

#字串編碼存入變數。
#一體適用。公司、

