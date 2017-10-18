from .sql import Sql
from gupiaoQQ.items import GupiaoqqItem


class gupiaoQQPipeline(object):

    def process_item(self, item, spider):
        #deferToThread(self._process_item, item, spider)
        if isinstance(item, GupiaoqqItem):
            date=item['date']
            open=item['open']
            high = item['high']
            close=item['close']
            low=item['low']
            volume=item['volume']
            chg=item['chg']
            p_chg=item['p_chg']
            ma5=item['ma5']
            ma10=item['ma10']
            ma20=item['ma20']
            vma5=item['vma5']
            vma10=item['vma10']
            vma20=item['vma20']
            turnover=item['turnover']
            code=item['code']

        Sql.insert_gupiaoDaily(self,date,open,high,close,low,volume,chg,p_chg,ma5,ma10,ma20,vma5,vma10,vma20,turnover,code)
        print('插入数据')
        return item

