import pusherclient 
import logging
import time
import datetime
import pandas as pd
import json
import numpy as np
import pymongo
from pymongo import *


class market_env():
    def __init__(self):
        
        self.df_ob = pd.DataFrame(columns = ['MTS','BIDS_VOL','ASKS_VOL','BOOK_IMBA','BEST_BIDS','BEST_ASKS','spread'])
        
        #trading
        self.observation_list=[]
        self.observation = None
        self.observation_ = None
        self.MTS =None
        self.bal = 5000
        self.COMM = 0.0025
        self.TP = 0.2
        self.SL = 5
        self.vwap=None
        self.port = []
        self.action_space = [0,1,2]
        self.n_actions = len(self.action_space)
        self.n_features=6
        self.features = []
        self.PNL=0
        self.best_bid=0
        self.best_ask=0
        self.trading_log=pd.DataFrame(columns = ["Buy time","Sell time","Symbol","Buy_Price","Sell_Price","Size","PNL"])
        self.book_imba=None
        self.qty= 0
        self.sell_qty = 0
        self.reward=float(0)
        self.spread = 0
        self.reward=0
        self.total_qty = 0
        self.total_price = 0
        
    def timestamp(self,data):
        timestamp = data['timestamp']
        mille_sec = (int(timestamp))/1
        formatted_time = (datetime.datetime.utcfromtimestamp(mille_sec).strftime('%Y-%m-%d %H:%M:%S.%f'))
        return formatted_time

    def order_book_callback(self,data):
       
        data = json.loads(data)

        self.best_bid = float("{0:.2f}".format(pd.to_numeric(data['bids'][0][0])))
        self.best_ask = float("{0:.2f}".format(pd.to_numeric(data['asks'][0][0])))
        self.mts = self.timestamp(data)
        self.book_imba = ((sum([float(x[1]) for x in data["bids"]]) - sum([float(x[1]) for x in data["asks"]]))
                            /(sum([float(x[1]) for x in data["bids"]])+sum([float(x[1]) for x in data["asks"]])))
        self.spread= float("{0:.2f}".format(pd.to_numeric(self.best_ask-self.best_bid)))
        self.features = [self.best_bid,self.best_ask,self.book_imba,self.spread,self.bal,0 if self.port==[] else (self.best_bid - self.port[2])*self.port[3]]
        self.observation_list.append(self.features)
        self.observation = self.features
        self.qty= float(data['asks'][0][1])
        self.sell_qty =float(data['bids'][0][1])
        
    def connect_handler(self,data): 
        order_book_channel = self.pusher.subscribe('order_book_ethusd');
        order_book_channel.bind('data', self.order_book_callback)

    
    def disconnect(self):
        self.pusher.unsubscribe("live_trades_ethusd")
        self.pusher.unsubscribe("live_orders_ethusd")
        self.pusher.unsubscribe("order_books_ethusd")
        self.pusher.disconnect()
        
    def get_data(self,a):
        self.pusher = pusherclient.Pusher("de504dc5763aeef9ff52")
        self.pusher.connection.logger.setLevel(logging.WARNING)
        self.pusher.connection.bind('pusher:connection_established', self.connect_handler)
        self.pusher.connect()
        print("--------------------------------")
        print(" Total Profit: " , self.trading_log['PNL'])
        print("Floating profit: ",0 if self.port==[] else (self.best_bid - self.port[2])*self.port[3])
        print("Balance: ",self.bal )
        print("Port",self.port)
        print("Reward",self.reward)
        print("--------------------------------")

        if a ==0:
            time.sleep(5)

        else:
            time.sleep(0.1)
    
#======================================================================================

    def reset(self):
        self.port = []
        self.trading_log = pd.DataFrame(columns = ["MTS","Symbol","Buy_Price","Sell_Price","Size","Profit"])
        self.action_space=[1,0,-1]
        self.observation_list=[]
        self.observation = None
        self.observation_ = None
        self.MTS =None
        self.bal = 1000
        self.COMM = 0.0025
        self.TP = 0.2
        self.SL = 0.1
        self.port = []
        self.action_space = [1,0,2]
        self.n_actions = len(self.action_space)
        self.n_features=5
        self.features = []
        self.PNL=0
        self.best_bid=0
        self.best_ask=0
        self.trading_log=pd.DataFrame(columns = ["MTS","Symbol","Buy_Price","Sell_Price","Size","PNL"])
        self.book_imba=None
        self.qty=0
        self.reward=float(0)
        self.spread = 0
        self.reward=0
        
    
    
    def step(self,action):
        #action
        print(action)
        if action == 1: #buy
            if self.port == []:
                if self.best_ask * self.qty < self.bal:
                    self.port =[self.mts ,1,self.best_ask,self.qty]
                    self.bal = self.bal - (self.best_ask * self.qty) -(self.best_ask * self.qty)*self.COMM
                    self.reward += -(self.best_ask * self.qty)*self.COMM
                if self.best_ask*self.qty > self.bal:
                    print("Insufficient Balance")
                    self.reward += -1
                    
            if self.port!=[]:
                if  (self.port[3]*self.port[2])+ (self.best_ask*self.qty) < self.bal:
                    self.total_qty = self.port[3] + self.qty
                    self.total_price = (self.best_ask*self.qty + self.port[2]*self.port[3])/self.total_qty
                    self.port = [self.mts,1,self.total_price,self.total_qty]
                    self.bal = self.bal - self.best_ask * self.qty -(self.best_ask * self.qty)*self.COMM
                    self.reward += -(self.best_ask * self.qty)*self.COMM
                if  (self.port[3]*self.port[2])+ (self.best_ask*self.qty) > self.bal:
                    print("Insufficient Balance")
                    self.reward += -5
                
        elif action == 2: #sell
            if self.port != [] :
                if self.sell_qty >= self.port[3]:
                    self.trading_log = self.trading_log.append({
                            "Buy time": self.port[0],
                            "Sell time":self.mts,
                            "Symbol":"ETHUSD",
                            "Buy_Price":self.port[2],
                            "Sell_Price": self.best_bid,
                            "Size": self.port[3],
                            "PNL": (self.best_bid-self.port[2])*self.port[3]},ignore_index=True)
                    self.reward += (self.best_bid-self.port[2])*self.port[3]-(self.best_bid*self.port[3])*self.COMM
                    self.PNL += (self.best_bid-self.port[2])*self.port[3] -(self.best_bid*self.port[3])*self.COMM
                    self.bal = self.bal+(self.best_bid*self.port[3]) -(self.best_bid*self.port[3])*self.COMM
                    self.port = []
                elif self.sell_qty < self.port[3]:
                    self.trading_log = self.trading_log.append({
                            "Buy time": self.port[0],
                            "Sell time":self.mts,
                            "Symbol":"ETHUSD",
                            "Buy_Price":self.port[2],
                            "Sell_Price": self.best_bid,
                            "Size": self.sell_qty,
                            "PNL": (self.best_bid-self.port[2])*self.sell_qty},ignore_index=True)
                    self.total_qty = self.port[3]-self.sell_qty
                    self.total_price = ((self.port[2]*self.port[3])-(self.best_bid*self.sell_qty))/self.total_qty
                    self.port = [self.mts,1,self.total_price,self.total_qty]
                    self.reward += (self.best_bid-self.port[2])*self.sell_qty -(self.best_bid*self.sell_qty)*self.COMM
                    self.PNL += (self.best_bid-self.port[2])*self.sell_qty -(self.best_bid*self.sell_qty)*self.COMM
                    self.bal = self.bal+(self.best_bid*self.sell_qty) -(self.best_bid*self.sell_qty)*self.COMM
                    
            elif self.port == []:
                print("Cant Sell, No inventory")
                self.reward += -0.1
        elif action ==0:
            print("Hold")
        else:
            print("Undefined action and it is ",action)
        #reward
        #reward = floatingPNL *0.1
        if self.port == []:
            self.reward += 0
        
        if self.port != []:
            self.reward += (self.best_bid - self.port[2])*0.1*self.port[3] #floating profit
        
        if self.PNL <= 2000:
            done = True
        else: done = False
        
        self.observation_ = self.observation_list[-1]
        
        return np.asarray(self.observation_),self.reward,done
            
        

        
        
        
        
        
        
        
        
