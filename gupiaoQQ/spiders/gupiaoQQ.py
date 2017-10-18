import re
import scrapy
import types
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
from scrapy.http import Request
from ..mysqlpipelines.sql import Sql
import datetime




class Myspider(scrapy.Spider):
    name = 'gupiaoQQ'
    allowed_domains = ['gtimg.cn']
    bash_url = 'http://qt.gtimg.cn/q='
    last_url='&type=last'
    ready = []
    turn = []
    i = datetime.datetime.now()

    def start_requests(self):
        gupiao=Sql.select_gupiao(self)

        for gp in gupiao:
            market=gp['market']
            #code=gp['code']
            code = gp['code']
            url = self.bash_url+market+code
            # print(url)
            yield Request(url, self.parse_user,meta={'code':code})
            # yield Request(url, self.parse_user)
        for gg in self.ready:
            # print(gg[2])
            code=str(gg[2])
            gp=Sql.select_gupiaoDaily_maxone(1,code)
            print(type(gp))
            if gp is None :
                pass
            elif (float(gg[5])-float(gp['ma5'])>0.09)  and (float(gg[5])-float(gp['ma10'])>0.13) and (float(gg[5])-float(gp['ma20'])>0.14) and 0.0<(float(gg[5])-float(gp['ma5']))/float(gp['ma5'])<0.1 :
                print(type(gg),gg)
                code=gg[2]
                turnover=gg[38]
                vm=gg[6]
                total=gg[45]
                up=gg[32]
                balance=(float(gg[3])-float(gg[5]))/float(gp['close'])*100

                Sql.insert_realtime(1,code,turnover,vm,total,up,balance,self.i)
                # self.turn.append(gg)

        # print("符合条件的是：")
        # npall = np.array(self.turn)
        # result = pd.DataFrame(data=npall)
        # result=result.sort_values(by=6, axis = 0,ascending=False)
        #
        # print(result)
        # Sql.update_turn(1,str(self.turn))




    def parse_user(self, response):
        result = response.text
        gupiao=[]
        pattern = re.compile(r'~')
        # print(result)

        gp=re.split(pattern, result)
        # print(gp)
        # print(len(gp))
        # print(type(gp[4]))
        if gp[4]=='0.00' or gp[5]=='0.00':
            print("未开盘")
        elif (float(gp[5])-float(gp[4]))/float(gp[4])>0.01 and float(gp[3])-float(gp[5])>0:
            gp[5]=float(gp[5])
            self.ready.append(gp)


        # for g in range(0,len(gp)):
        #     if gp[g]=='1.91' :
        #         print(g)
        #
        # for g in range(0,len(gp)):
        #     if gp[g]=='1.91' :
        #         print(g)
        # result = result['record']
        # code = response.meta['code']
        # for rs in result:
        #     print(rs)
        #     item=GupiaoqqItem()
        #     item['date']=rs[0]
        #     print(item['date'])
        #     item['open'] = rs[1]
        #     item['high']=rs[2]
        #     item['close'] = rs[3]
        #     item['low'] = rs[4]
        #     item['volume'] = rs[5]
        #     item['chg'] = rs[6]
        #     item['p_chg'] = rs[7]
        #     item['ma5'] = rs[8]
        #     item['ma10'] = rs[9]
        #     item['ma20'] = rs[10]
        #     item['vma5'] = rs[11]
        #     item['vma10'] = rs[12]
        #     item['vma20'] = rs[13]
        #     if len(rs)==15 :
        #        item['turnover'] = rs[14]
        #     else :
        #         item['turnover'] = '~'
        #     item['code'] = code
        #     yield item

        #     print(rs[0])
        # print(len(result))
    pass
