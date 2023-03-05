# coding=utf-8
import sys
import os
import gc
import pandas as pd
import tqdm
import sqlite3
import argparse
import lxml
import requests;
import pymongo;
import configparser;
import re;

import creonAPI
import psycopg2
from utils import is_market_open, available_latest_date, preformat_cjk
from datetime import datetime;


class CreonDatareaderCLI:
    def __init__(self):
        self.objStockChart = creonAPI.CpStockChart()
        self.objCodeMgr = creonAPI.CpCodeMgr()
        self.rcv_data = dict()  # RQ후 받아온 데이터 저장 멤버
        self.sv_code_df = pd.DataFrame()
        self.db_code_df = pd.DataFrame()

        config=configparser.ConfigParser();
        config.read('config.ini', encoding='utf-8');
        self.db_name=config['db']['name'];
        self.host=config['db']['host'];
        self.port=int(config['db']['port']);

        sv_code_list = self.objCodeMgr.get_code_list(1) + self.objCodeMgr.get_code_list(2)
        sv_name_list = list(map(self.objCodeMgr.get_code_name, sv_code_list))
        self.sv_code_df = pd.DataFrame({'종목코드': sv_code_list,'종목명': sv_name_list},
                                       columns=('종목코드', '종목명'))
    def rename_collection(self):
        conn=pymongo.MongoClient(host=self.host, port=self.port);
        db=conn.get_database(self.db_name);
        for name in db.list_collection_names():
            if(name.lower()!=name):
                db[name].rename(name.lower());
    
    def delete_duplication(self):
        
        with pymongo.MongoClient(host=self.host, port=self.port) as con:
            db=con[self.db_name];
            for name in db.list_collection_names():
                cursor=db[name].aggregate(
                    [
                        {"$group":{"_id":"$date", "unique_ids":{"$addToSet":"$_id"}, "count":{"$sum":1}}},
                        {"$match":{"count":{"$gte":2}}}
                    ]
                );
                duplicate=[];
                for doc in list(cursor):
                    del doc["unique_ids"][0]
                    for id in doc["unique_ids"]:
                        duplicate.append(id);
                db[name].delete_many({"_id":{"$in":duplicate}});

    
    def update_price_db(self, tick_unit='day', ohlcv_only=False, code_list=[]):
        """
        tick_unit: '1min', '5min', 'day'. db에 값이 존재하는 경우
        ohlcv_only: ohlcv 이외의 데이터도 저장할지 여부. 이미 db_path가 존재할 경우, 입력값 무시하고 기존에 사용된 값 사용 
                    'day' 아닌경우 False 선택 불가 고정.
        """
        if tick_unit != 'day':
            ohlcv_only = True

        #MongoDB 연동
        conn=pymongo.MongoClient(host=self.host, port=self.port);
        db=conn.get_database(self.db_name);

        #DB에 저장된 종목 정보 가져와서 dataframe으로 저장      
        db_code_list=[];
        if(len(code_list)==0):
            for coll in db.list_collections():
                code=coll['name'].upper(); #API 사용을 위해서 대문자로 바꿔줌
                db_code_list.append(code);
            db_name_list=list(map(self.objCodeMgr.get_code_name, db_code_list));
        #code_list가 주어진 경우 API사용을 위해서 앞에 A를 붙여줌(주식인 경우만을 고려)
        else:
            for i in range(len(code_list)):
                code_list[i]='A'+code_list[i];
            db_name_list=list(map(self.objCodeMgr.get_code_name, db_code_list));

        db_latest_list = [0]*len(db_code_list);
        lastest_idx=-1;
        for i in range(len(db_code_list)):
            coll=db.get_collection(db_code_list[i].lower());
            ret=list(coll.find().sort("date",pymongo.DESCENDING).limit(1));
            if(len(ret)==1):
                db_latest_list[i]=ret[0]["date"];
                if(lastest_idx==-1): lastest_idx=i;

        # 현재 db에 저장된 'date' column의 tick_unit 확인
        # 현재 db에 저장된 column 명 확인. (ohlcv_only 여부 확인)
        if lastest_idx!=-1:
            coll=db.get_collection(db_code_list[lastest_idx]);
            dates = list(coll.find().sort("date",pymongo.DESCENDING).limit(2));
            if(len(dates)==2):
                date0=date0["date"];
                date1=date1["date"];

                # 날짜가 분 단위 인 경우
                if date0 > 99999999:
                    if date1 - date0 == 5: # 5분 간격인 경우
                        tick_unit = '5min'
                    else: # 1분 간격인 경우
                        tick_unit = '1min'
                elif date0%100 == 0: # 월봉인 경우
                    tick_unit = 'month'
                elif date0%10 == 0: # 주봉인 경우
                    tick_unit = 'week'
                else: # 일봉인 경우
                    tick_unit = 'day'

                # column개수로 ohlcv_only 여부 확인
                column_names = coll.find_one();
                if tick_unit=='day' and len(column_names) > 6:  # date, o, h, l, c, v
                    ohlcv_only = False
                else:
                    ohlcv_only = True

        db_code_df = pd.DataFrame({'종목코드': db_code_list, '종목명': db_name_list, '갱신날짜': db_latest_list},
                                  columns=('종목코드', '종목명', '갱신날짜'))
        fetch_code_df = db_code_df

        # 분봉/일봉에 대해서만 아래 코드가 효과가 있음.
        if not is_market_open():
            latest_date = available_latest_date()
            if tick_unit == 'day':
                latest_date = latest_date // 10000
            # 이미 DB 데이터가 최신인 종목들은 가져올 목록에서 제외한다
            already_up_to_date_codes = db_code_df.loc[db_code_df['갱신날짜']==latest_date]['종목코드'].values
            fetch_code_df = fetch_code_df.loc[fetch_code_df['종목코드'].apply(lambda x: x not in already_up_to_date_codes)]

        if tick_unit == '1min':
            count = 200000  # 서버 데이터 최대 reach 약 18.5만 이므로 (18/02/25 기준)
            tick_range = 1
        elif tick_unit == '5min':
            count = 100000
            tick_range = 5
        elif tick_unit == 'day':
            count = 10000  # 10000개면 현재부터 1980년 까지의 데이터에 해당함. 충분.
            tick_range = 1
        elif tick_unit == 'week':
            count = 2000
        elif tick_unit == 'month':
            count = 500

        if ohlcv_only:
            columns=['date','open', 'high', 'low', 'close', 'volume']
        else:
            columns=['date','open', 'high', 'low', 'close', 'volume',
                     '상장주식수', '외국인주문한도수량', '외국인현보유수량', '외국인현보유비율', '기관순매수', '기관누적순매수']


        with pymongo.MongoClient(host=self.host, port=self.port) as con:
            tqdm_range = tqdm.trange(len(fetch_code_df), ncols=100)
            for i in tqdm_range:
                code = fetch_code_df.iloc[i]
                update_status_msg = '[{}] {}'.format(code[0], code[1])
                tqdm_range.set_description(preformat_cjk(update_status_msg, 25))
                coll_name=code[0].lower();
                coll=con[self.db_name][coll_name];

                from_date = 0;
                cur_count=2000;
                if coll_name in db_code_df['종목코드'].tolist():
                    ret=list(coll.find().sort("date",pymongo.DESCENDING).limit(1));
                    if(len(ret)==1):
                        from_date = ret[0]["date"]; 
                if(from_date!=0):
                    savedTime=datetime.strptime(str(from_date),"%Y%m%d%H%M");
                    curTime=datetime.now().replace(second=0);
                    count_cand=(curTime-savedTime).seconds/60;
                    cur_count=min(cur_count, count_cand);

                if tick_unit == 'day':  # 일봉 데이터 받기
                    if self.objStockChart.RequestDWM(code[0], ord('D'), cur_count, self, from_date, ohlcv_only) == False:
                        continue
                elif tick_unit == '1min' or tick_unit == '5min':  # 분봉 데이터 받기
                    if self.objStockChart.RequestMT(code[0], ord('m'), tick_range, cur_count, self, from_date, ohlcv_only) == False:
                        continue
                elif tick_unit == 'week':  #주봉 데이터 받기
                    if self.objStockChart.RequestDWM(code[0], ord('W'), cur_count, self, from_date, ohlcv_only) == False:
                        continue
                elif tick_unit == 'month':  #주봉 데이터 받기
                    if self.objStockChart.RequestDWM(code[0], ord('M'), cur_count, self, from_date, ohlcv_only) == False:
                        continue
                df = pd.DataFrame(self.rcv_data, columns=columns, index=self.rcv_data['date'])
                df['date']=self.rcv_data['date'];
                # 기존 DB와 겹치는 부분 제거
                if from_date != 0:
                     df = df.loc[:str(from_date)];
                     df = df.iloc[:-1]
                if from_date==0:
                    coll.create_index("date");
                if(len(df)<1): continue;
                # 뒤집어서 저장 (결과적으로 date 기준 오름차순으로 저장됨)
                df = df.iloc[::-1]
                data=df.to_dict(orient='records');
                coll.insert_many(data);
                # 메모리 overflow 방지
                del df
                gc.collect()

def main_cli():
    parser = argparse.ArgumentParser(description='creon datareader CLI')
    parser.add_argument('--tick_unit', required=False, type=str, default='day', help='{1min, 5min, day, week, month}')
    parser.add_argument('--ohlcv_only', required=False, type=int, default=0, help='0: False, 1: True')
    args = parser.parse_args();

    creon = CreonDatareaderCLI()
    creon.update_price_db(args.tick_unit, args.ohlcv_only==1);

if __name__ == "__main__":
    main_cli();
