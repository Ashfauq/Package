import pandas as pd
from time import time
import re
import os
import warnings
warnings.filterwarnings("ignore")
import copy
try:
    from pyspark.sql.types import *
except:
    pass



class Newfile():    
    def __init__(self,filename = "NO NAME.csv",data = 0,sent_col =0,content_col = 0,sheet = "sheet 1",ascii = False,config = 0 ):
        ''' Constructor for this class. '''
        self.test = data
        self.filename = filename
        self.content_col = content_col
        self.sheet = sheet
        self.ascii = ascii
        self.parent_config = config
        
        try:
            self.test.shape
            self.data = self.test
            self.data = self.data.reset_index(drop = True)
        except:
            if ".xlsx" in self.filename:
                try:
                    self.data = pd.read_excel(self.filename,encoding='iso-8859-1',error_bad_lines = False, sheetname= self.sheet)
                except:
                    self.data = pd.read_excel(self.filename,encoding='iso-8859-1',error_bad_lines = False)
            else:
                self.data = pd.read_csv(self.filename,encoding='iso-8859-1',error_bad_lines=False,index_col=False)            
            pass
        self.sent_col = sent_col
        self.sub_sent_col = sent_col
        self.filename = re.split(r"\\",self.filename)
        self.filename = self.filename[len(self.filename)-1]
        self.mainname = filename
        self.doc_col = "DocumentsKey"
        try:
            if self.data["DocumentsKey"]:
                self.doc_col = "DocumentsKey"
        except:
            self.data["p_unique_id"] = [ each for each in range(0,self.data.shape[0])]
            self.doc_col = "p_unique_id"
            pass
       
        
        self.date_col = "PublishedDatesKey"
        
        
        for each in list(self.data):
            if "sentiment" in each.lower():
                self.sub_sent_col = each        
        
        
        for each in list(self.data):
            if "calculated sentiment subcategeory" in each.lower():
                self.sub_sent_col = each
        

        
                
        if self.content_col == 0:
            self.content_col = "Content"
        self.shape = self.data.shape
        self.data = self.data.fillna("NA")
        try:
            if self.ascii == True:
                self.data[self.content_col] = [ each.encode("ASCII","ignore").decode("utf-8") for each in self.data[self.content_col]]   
        except:
            pass
            
         
        
        if self.parent_config != 0:
            self.sub_sent_col = self.parent_config["sub_sent_col"]
            self.date_col = self.parent_config["date_col"]
            self.content_col = self.parent_config["content_col"]
            self.doc_col  = self.parent_config["doc_col"]
    
    def update_config(self):
        self.config = {}
        self.config["sub_sent_col"]= self.sub_sent_col
        self.config["date_col"]= self.date_col
        self.config["doc_col"]= self.doc_col
        self.config["content_col"]= self.content_col
        
        
    def __repr__(self):
        return str(self.data)
        
    def __str__(self):
        return str(self.data)        
        
    def convert_num(self,num):
   
        try:
            return int(num)
        except:
            pass
        
        try:
            return int(float(num))
        except:
            pass
        
        return num
            
            
    def csv(self,name= 0):
    
        self.name = name
        if self.name == 0:
            self.name = self.filename
        self.data.to_csv(self.name,index = None,encoding = "iso 8859-1")    
        
    def excel(self,name= 0):
    
        self.name = name
        if self.name == 0:
            self.name = self.filename
        self.data.to_excel(self.name,index = None,encoding = "iso 8859-1")
    
    def agg_sub(self,agg_name = 0,target = 0,col = 0,sent = 0,preprocess= True,print_all = True,write = False,return_agg = True):
        self.target = target
        self.agg_col = col
        self.write = write
        self.return_agg = return_agg
        self.preprocess = preprocess
        if self.agg_col ==0:
            self.agg_col = self.sub_sent_col
        self.agg_name = agg_name
        
        
        if self.agg_name == 0:
            self.agg_name = self.filename
        self.temp_data = self.data.copy()
        if self.preprocess == True:
            import re
            self.temp_data[self.content_col] = [re.sub("https://t.co/[a-zA-Z0-9_a ]{0,10}\S","",each) for each in self.temp_data[self.content_col]]
            self.temp_data[self.content_col] = [re.sub("RT @[a-zA-Z0-9_a]{0,30}\:\s","",each) for each in self.temp_data[self.content_col]]
            self.temp_data[self.content_col] = [re.sub("@[a-zA-Z0-9_a]{0,30}\s","",each) for each in self.temp_data[self.content_col]]
            # self.temp_data[self.content_col] = [re.sub("#[a-zA-Z0-9_a]{0,30}\s","",each) for each in self.temp_data[self.content_col]]
            self.temp_data[self.content_col] = [each.strip() for each in self.temp_data[self.content_col]]
             
        
        
        
        if sent!= 0:
            if sent in ["Very Positive","Very Negative","Neutral","Somewhat Positive","Somewhat Negative"]:
                self.temp_data = self.temp_data.loc[self.temp_data[self.sub_sent_col] == sent]
            elif sent == "Positive":
                self.temp_data = self.temp_data.loc[self.temp_data[self.sub_sent_col].isin(["Very Positive","Somewhat Positive"])]
            elif sent == "Negative":
                self.temp_data = self.temp_data.loc[self.temp_data[self.sub_sent_col].isin(["Very Negative","Somewhat Negative"])]
            
        if self.target != 0:
            if str(type(self.target)) != '<class \'tuple\'>':
                self.temp_data[self.date_col] = self.temp_data[self.date_col].astype(str)
                self.temp_data = self.temp_data.loc[self.temp_data[self.date_col] == str(target)]
            if str(type(self.target)) =='<class \'tuple\'>':
                self.dd = Newfile(data = self.temp_data)
                self.d1 = self.dd.filter(col = self.date_col,targ=[self.target[0]],value_filter = ">=")
                self.d2 = self.d1.filter(col = self.date_col,targ =[self.target[1]],value_filter= "<=")
                self.temp_data = self.d2.data
            
        self.agg_data =  pd.DataFrame((self.temp_data).groupby([self.content_col,self.agg_col])[self.doc_col].count())
        

        self.agg_data = self.agg_data.reset_index()
        self.agg_data = self.agg_data.sort_values(self.doc_col, ascending=False)
        if self.write == True:
            self.agg_data.to_csv("agg "+ str(self.agg_name),index = None)
        # print(self.agg_data.head(3))
        self.eg = self.agg_data[self.content_col].head(5).reset_index(drop = True)
        self.eg1 = self.agg_data[self.doc_col].head(5).reset_index(drop = True)
        self.print_all = print_all
        if self.print_all == True:
            for i,each in enumerate(self.eg):
                print(self.eg[i])
                print(self.eg1[i])
                print("XXXXXXXXXXXXXXXXXXXXXX")
        del self.preprocess
        if self.return_agg == True:
            return self.agg_data
        
    def agg(self,agg_name = 0,target = 0,col = 0,sent = 0,preprocess= True,print_all = True,return_agg = False,write = True):
        self.target = target
        self.agg_col = col
        self.preprocess = preprocess
        self.return_agg = return_agg
        self.write = write
        if self.agg_col ==0:
            self.agg_col = self.content_col
        self.agg_name = agg_name
        if self.agg_name == 0:
            self.agg_name = self.filename
        self.temp_data = self.data.copy()
        if self.preprocess == True:
            import re
            self.temp_data[self.agg_col] = [re.sub("https://t.co/[a-zA-Z0-9_a ]{0,10}\S","",each) for each in self.temp_data[self.agg_col]]
            self.temp_data[self.agg_col] = [re.sub("RT @[a-zA-Z0-9_a]{0,30}\:\s","",each) for each in self.temp_data[self.agg_col]]
            self.temp_data[self.agg_col] = [re.sub("@[a-zA-Z0-9_a]{0,30}\s","",each) for each in self.temp_data[self.agg_col]]
            # self.temp_data[self.content_col] = [re.sub("#[a-zA-Z0-9_a]{0,30}\s","",each) for each in self.temp_data[self.content_col]]
            self.temp_data[self.agg_col] = [each.strip() for each in self.temp_data[self.agg_col]]
             
        
        
        
        # if sent!= 0:
            # if sent in ["Very Positive","Very Negative","Neutral","Somewhat Positive","Somewhat Negative"]:
                # self.temp_data = self.temp_data.loc[self.temp_data[self.sub_sent_col] == sent]
            # elif sent == "Positive":
                # self.temp_data = self.temp_data.loc[self.temp_data[self.sub_sent_col].isin(["Very Positive","Somewhat Positive"])]
            # elif sent == "Negative":
                # self.temp_data = self.temp_data.loc[self.temp_data[self.sub_sent_col].isin(["Very Negative","Somewhat Negative"])]
            
        if self.target != 0:
            if str(type(self.target)) != '<class \'tuple\'>':
                self.temp_data[self.date_col] = self.temp_data[self.date_col].astype(str)
                self.temp_data = self.temp_data.loc[self.temp_data[self.date_col] == str(target)]
            if str(type(self.target)) =='<class \'tuple\'>':
                self.dd = Newfile(data = self.temp_data)
                self.d1 = self.dd.filter(col = self.date_col,targ=[self.target[0]],value_filter = ">=")
                self.d2 = self.d1.filter(col = self.date_col,targ =[self.target[1]],value_filter= "<=")
                self.temp_data = self.d2.data
            
        self.agg_data =  pd.DataFrame((self.temp_data).groupby([self.agg_col])[self.doc_col].count())
        

        self.agg_data = self.agg_data.reset_index()
        self.agg_data = self.agg_data.sort_values(self.doc_col, ascending=False)
        if self.write == True:
            self.agg_data.to_csv("agg "+ str(self.agg_name),index = None)
        # print(self.agg_data.head(3))
        self.eg = self.agg_data[self.agg_col].head(5).reset_index(drop = True)
        self.eg1 = self.agg_data[self.doc_col].head(5).reset_index(drop = True)
        self.print_all = print_all
        if self.print_all == True:
            for i,each in enumerate(self.eg):
                print(self.eg[i])
                print(self.eg1[i])
                print("XXXXXXXXXXXXXXXXXXXXXX")
        del self.preprocess
        if self.return_agg == True:
            return self.agg_data
    
    def retag(self,key,all_rt = False):
        self.start_time = time()
        self.m = pd.read_csv(key,encoding='iso-8859-1',error_bad_lines=False,index_col=False)
        self.m["count"] = 0
        self.temp2 = self.data.copy()
        self.temp2[self.content_col] = self.temp2[self.content_col].str.lower()
        self.temp2[self.content_col] = [each.replace("?¦","") for each in self.temp2[self.content_col]]
        self.rt_list = []
        self.all_rt = all_rt
        for each in range(0,len(self.m)):
            print(each)
            self.word = self.m["match"][each]
            self.word = str(self.word)
            self.word = self.word.lower()
            
            if self.m["regex"][each] == 1:
                self.word =r'%s'%self.word
                self.regex= True
            else:
                self.regex = False
                self.word = self.word.replace("?¦","")
            print(self.word)
            self.temp = self.temp2[((self.temp2[self.content_col]).str.contains(self.word,regex = self.regex,flags = re.IGNORECASE)) & self.temp2[self.sub_sent_col].str.contains(self.m["from"][each],flags = re.IGNORECASE)]
            [self.rt_list.append(each) for each in self.temp.index]
            self.count = 0
            self.data[self.sub_sent_col][self.data.index.isin(self.temp.index)] = self.m["to"][each]
            self.count = int(self.temp.shape[0])
            self.m["count"][each] = self.count
        
        self.rt_list = list(set(self.rt_list))
        if self.all_rt ==True:
            self.rt = self.data[self.data.index.isin(self.rt_list)]
            self.rt[[self.doc_col,"AuthorName","ChannelName",self.content_col,self.date_col,"SourceURL",self.sub_sent_col]].to_csv("All RT " + self.filename,index = None)
            
        self.end_time = time()
        print("Time took for retags == " + str(self.end_time - self.start_time) )
        self.m.to_csv(r'rt log ' +self.filename,index=None)
        self.filename = "RT "+ self.filename
        self.temp2 = 0
        self.update_config()
        return Newfile(filename = self.filename,data = self.data,content_col = self.content_col, sent_col = self.sent_col,config = self.config)


    def retag2(self,key,all_rt = False):
        self.start_time = time()
        self.m = pd.read_csv(key,encoding='iso-8859-1',error_bad_lines=False,index_col=False)
        self.m["count"] = 0
        self.temp2 = self.data.copy()
        self.temp2[self.content_col] = self.temp2[self.content_col].str.lower()
        self.temp2[self.content_col] = [each.replace("?¦","") for each in self.temp2[self.content_col]]
        self.rt_list = []
        self.all_rt = all_rt

        for each in range(0,len(self.m)):
            print(each)
            self.word = self.m["match"][each]
            self.word = str(self.word)
            self.word = self.word.lower()
            
            if self.m["regex"][each] == 1:
                self.word =r'%s'%self.word
                self.regex= True
            else:
                self.regex = False

                self.word = self.word.replace("?¦","")
            print(self.word)
            self.temp = self.temp2[((self.temp2[self.content_col]).str.contains(self.word,regex = self.regex,flags = re.IGNORECASE)) & self.temp2[self.sub_sent_col].str.contains(self.m["from"][each],flags = re.IGNORECASE)]
            [self.rt_list.append(each) for each in self.temp.index]

            self.count = 0
            self.temp2[self.sub_sent_col][self.temp2.index.isin(self.temp.index)] = self.m["to"][each]
            self.data[self.sub_sent_col][self.data.index.isin(self.temp.index)] = self.m["to"][each]
            self.count = int(self.temp.shape[0])
            self.m["count"][each] = self.count
        
        self.rt_list = list(set(self.rt_list))
        if self.all_rt ==True:
            self.rt = self.data[self.data.index.isin(self.rt_list)]
            self.rt[[self.doc_col,"AuthorName","ChannelName",self.content_col,self.date_col,"SourceURL",self.sub_sent_col]].to_csv("All RT " + self.filename,index = None)
            
        self.end_time = time()
        print("Time took for retags == " + str(self.end_time - self.start_time) )
        self.m.to_csv(r'rt log ' +self.filename,index=None)
        self.filename = "RT "+ self.filename
        self.temp2 = 0
        self.update_config()
        return Newfile(filename = self.filename,data = self.data,content_col = self.content_col, sent_col = self.sent_col,config = self.config)
        
        

        
    def sentiment_sub(self,target_col = 0,write = False,print_all = True):
        self.write = write
        self.target_col = target_col
        if self.target_col == 0:
            self.target_col = self.sub_sent_col
        

        self.print_all = print_all
        self.data[self.date_col] =  self.data[self.date_col].astype(str)
        
        self.d = pd.DataFrame(self.data.groupby([self.date_col])[self.target_col].count())
        # print(self.d)
        self.tx =pd.DataFrame()
        self.tx[self.date_col] = self.data[self.date_col]
        self.tx['sentiment'] = self.data[self.target_col]
        
        self.tp = self.tx
        self.tn = self.tx
        self.tt = self.tx
        
        self.filter_rows_p =[]
        self.filter_rows_ps =[]
        self.filter_rows_n =[]
        self.filter_rows_ns =[]
        self.filter_rows_t = []
    
        self.temp = self.tp[self.tp["sentiment"] == "Very Positive"]
        self.filter_rows_p = (self.temp.index)
        self.temp = self.tp[self.tp["sentiment"] == "Somewhat Positive"]
        self.filter_rows_ps = (self.temp.index)
        self.temp = self.tn[self.tn["sentiment"] == "Very Negative"]
        self.filter_rows_n= (self.temp.index)
        self.temp = self.tn[self.tn["sentiment"] == "Somewhat Negative"]
        self.filter_rows_ns= (self.temp.index)
        self.temp = self.tt[self.tt["sentiment"] == "Neutral"]
        self.filter_rows_t = (self.temp.index)
        
        self.cols = [self.date_col,"sentiment"]
        self.cols1 = [self.date_col,"Very Positive"]
        self.cols2 = [self.date_col,"Somewhat Positive"]
        self.cols3 = [self.date_col,"Very Negative"]
        self.cols4 = [self.date_col,"Somewhat Negative"]
        self.cols5 = [self.date_col,"Neutral"]
        
        self.tpx = pd.DataFrame(columns=self.cols1)
        self.tpsx = pd.DataFrame(columns=self.cols2)
        self.tnx = pd.DataFrame(columns=self.cols3)
        self.tnsx = pd.DataFrame(columns=self.cols4)
        self.ttx = pd.DataFrame(columns=self.cols5)
        
        self.tpx[self.date_col] = self.tp[self.date_col][self.tp.index.isin(self.filter_rows_p)]
        self.tpx["Very Positive"] = self.tp["sentiment"][self.tp.index.isin(self.filter_rows_p)]
        self.tpsx[self.date_col] = self.tp[self.date_col][self.tp.index.isin(self.filter_rows_ps)]
        self.tpsx["Somewhat Positive"] = self.tp["sentiment"][self.tp.index.isin(self.filter_rows_ps)]
        
        self.tnx[self.date_col] = self.tn[self.date_col][self.tn.index.isin(self.filter_rows_n)]
        self.tnx["Very Negative"] = self.tn["sentiment"][self.tn.index.isin(self.filter_rows_n)]
        self.tnsx[self.date_col] = self.tn[self.date_col][self.tn.index.isin(self.filter_rows_ns)]
        self.tnsx["Somewhat Negative"] = self.tn["sentiment"][self.tn.index.isin(self.filter_rows_ns)]
          
        self.ttx[self.date_col] = self.tt[self.date_col][self.tt.index.isin(self.filter_rows_t)]
        self.ttx["Neutral"] = self.tt["sentiment"][self.tt.index.isin(self.filter_rows_t)]
        
        if self.print_all == True:
            print("******")
            print("Sentiment taken from the column :",self.target_col)
            print("******")
            print("Very Positive " +  str(self.tpx.shape[0]) )
            print("Somewhat Positive " +  str(self.tpsx.shape[0]) )
            print("Very Negative " +  str(self.tnx.shape[0]) )
            print("Somewhat Negative " +  str(self.tnsx.shape[0]) )
            print("Neutral " +  str(self.ttx.shape[0]) )
        
        self.div = (self.tpx.shape[0]+ self.tpsx.shape[0] + self.tnx.shape[0]+self.tnsx.shape[0]+ self.ttx.shape[0])
        if self.div == 0:
            self.div = 0.000001
        self.sent_sum = {"Very Positive":str(round((self.tpx.shape[0]/self.div*100),2)),
                "Somewhat Positive":str(round((self.tpsx.shape[0]/self.div*100),2)),
                "Very Negative":str(round((self.tnx.shape[0]/self.div*100),2)),
                "Somewhat Negative":str(round((self.tnsx.shape[0]/self.div*100),2)),
                 "Neutral":   str(round((self.ttx.shape[0]/self.div*100),2)),
                 "XX POSITIVE": str(round(((round((self.tpsx.shape[0]/self.div*100),2)) + (round((self.tpx.shape[0]/self.div*100),2))),2)),
                 "XX NEGATIVE": str(round(((round((self.tnsx.shape[0]/self.div*100),2)) + (round((self.tnx.shape[0]/self.div*100),2))),2))}

        self.tpx = pd.DataFrame(self.tpx.groupby([self.date_col])["Very Positive"].count())
        self.tpsx = pd.DataFrame(self.tpsx.groupby([self.date_col])["Somewhat Positive"].count())
        self.tnx = pd.DataFrame(self.tnx.groupby([self.date_col])["Very Negative"].count())
        self.tnsx = pd.DataFrame(self.tnsx.groupby([self.date_col])["Somewhat Negative"].count())
        self.ttx = pd.DataFrame(self.ttx.groupby([self.date_col])["Neutral"].count())

        
        
        self.d = pd.concat([self.d, self.tnx["Very Negative"]], axis=1)
        self.d = pd.concat([self.d, self.tnsx["Somewhat Negative"]], axis=1)
        self.d = pd.concat([self.d, self.ttx["Neutral"]], axis=1)
        self.d = pd.concat([self.d, self.tpsx["Somewhat Positive"]], axis=1)
        self.d = pd.concat([self.d, self.tpx["Very Positive"]], axis=1)
        self.d = self.d.fillna(0)
        self.d[self.target_col] = self.d["Very Negative"] + self.d["Somewhat Negative"] + self.d["Neutral"] + self.d["Somewhat Positive"] +self.d["Very Positive"]
        

        self.d = self.d.reset_index()
        

        try:
            self.temp = [each for each in self.d["index"] if type(self.convert_num(each)) == int]
            self.d = self.d[self.d["index"].isin(self.temp)]
        except:
            pass
        if self.write == True:
            self.d.to_csv("sentiment_agg_SUB "+ self.filename,index=None)
        return self.sent_sum
                
    def generate_retag_template(self):
        self.new_retag_file = pd.DataFrame()
        self.new_retag_file["match"] = None
        self.new_retag_file["from"] = None
        self.new_retag_file["to"] = None
        self.new_retag_file["regex"] = None
        self.new_retag_file["month"] = None
        self.new_retag_file.to_csv("new_retag_template.csv",index = None)
        
    def generate_spam_template(self):
        self.new_spam_file = pd.DataFrame()
        self.new_spam_file["Keywords"] = None
        self.new_spam_file.to_csv("new_spam_template.csv",index = None)     
        
    def generate_theme_template(self):
        self.new_theme_file = pd.DataFrame()
        self.new_theme_file["Keywords"] = None
        self.new_theme_file["Theme_Name"] = None
        self.new_theme_file.to_csv("new_theme_template.csv",index = None)        
        
    def generate_config_template(self):
        self.new_theme_file = pd.DataFrame()
        self.new_theme_file["a"] = None
        self.new_theme_file["b"] = None
        self.new_theme_file.to_csv("new_config_template.csv",index = None)            
    
       
    def spam_remover(self,key):
        self.spam_key = key
        self.spam = pd.read_csv(self.spam_key,encoding='iso-8859-1',error_bad_lines=False,index_col=False)
        self.pp = self.data.copy()
        self.r = [str(each) for each in self.spam["Keywords"]]
        for each in self.r:
            self.pp = self.pp[self.pp[self.content_col].str.contains(each,flags=re.IGNORECASE, regex=True, na=False) == False]
        print("Number of Documents remaining " + str(self.pp.shape[0]))
        # self.original = self.data
        self.filename = "SPR "+self.filename
        return Newfile(data = self.pp,filename = self.filename,content_col = self.content_col, sent_col = self.sent_col)    
    
    def spark_spam_remover(self,key):
        import time
        from pyspark.sql import SQLContext  
        self.sqlContext = SQLContext(self.sc) 
        self.t1 = time.time()
        self.spam_key = key
        self.spam = pd.read_csv(self.spam_key,encoding='iso-8859-1',error_bad_lines=False,index_col=False)
        self.pp = self.data.copy()
        self.r = [str(each) for each in self.spam["Keywords"]]
        
        self.cols = list(self.pp)
        self.fields = [StructField(field_name, StringType(), True) for field_name in self.cols]
        self.schema = StructType(self.fields)
        
        for each in self.r:
            self.pp = self.sqlContext.createDataFrame(self.pp, self.schema)
            each = '(?i)' + each
            self.pp = self.pp.where(~self.pp[self.content_col].rlike(each)).toPandas()
            # self.pp = self.pp[self.pp[self.content_col].str.contains(each,flags=re.IGNORECASE, regex=True, na=False) == False]
        print("Number of Documents remaining " + str(self.pp.shape[0]))
        # self.original = self.data
        self.filename = "SPR "+self.filename
        self.sc.stop()
        self.t2 = time.time()
        print("Time Taken : " + str(self.t2-self.t1) )
        return Newfile(data = self.pp,filename = self.filename,content_col = self.content_col, sent_col = self.sent_col)
        
    def init_spark(self):
        import os
        import sys
        spark_home = os.environ.get('SPARK_HOME', None)
        import pyspark
        from pyspark import SparkContext, SparkConf
        from pyspark.sql import SparkSession
        self.conf = SparkConf()
        self.sc = SparkContext(conf=self.conf)
        # self.sc = SparkConf()
        self.spark = SparkSession.builder.config(conf=self.conf).getOrCreate()
       
            
    def theme(self,key,trim = False,uncovered = True):
        self.theme_key = key
        self.temp = self.data.copy()
        self.trim = trim
        self.start_time_overall = time()
        self.theme_data = pd.read_csv(self.theme_key,encoding='iso-8859-1',error_bad_lines=False,index_col=False)
        self.uncov_bool = uncovered
        self.covered_index = []
        if os.path.isdir("theme//") == False:
            os.makedirs("theme//")
        for each_kw in range(len(self.theme_data)):
            self.op = pd.DataFrame()
            self.r = str(self.theme_data['Keywords'][each_kw])
            self.t = str(self.theme_data['Theme_Name'][each_kw])

            self.op = self.temp[(self.temp[self.content_col].str.lower()).str.contains(self.r,flags=re.IGNORECASE, regex=True, na=False) == True].copy()
            (([self.covered_index.append(each) for each in self.op.index]))
            if self.trim == True:
                self.op[[self.doc_col,"AuthorName","ChannelName",self.content_col,self.date_col,"SourceURL",self.sub_sent_col]].to_csv(r'theme//'+ self.t +" "+self.mainname,index=None,encoding = "iso 8859-1")
            else:
                self.op.to_csv(r'theme//'+ self.t +" "+self.mainname,index=None,encoding = "iso 8859-1")
            
            print ("No. of Documents for theme -",self.t,"=", len(self.op))
        
        if self.uncov_bool == True:
            self.covered_index = [int(each) for each in self.covered_index]
            self.covered_index = list(set(self.covered_index))
            self.temp.index = [int(each) for each in self.temp.index]
            self.all_uncovered = self.temp[~self.temp.index.isin(self.covered_index)]
            
            print("The Uncovered Volume - " + str(self.all_uncovered.shape[0]))
            if self.trim == True:
                self.all_uncovered[[self.doc_col,"AuthorName","ChannelName",self.content_col,self.date_col,"SourceURL",self.sub_sent_col]].to_csv(r'theme//'+ "All Uncov - "+ self.theme_key + " " +self.mainname,index=None)
            else:
                self.all_uncovered.to_csv(r'theme//'+ "All Uncov - "+ self.theme_key + " " +self.mainname,index=None)
        self.end_time_overall = time()
        print ("Overall Running Time : " + str(self.end_time_overall - self.start_time_overall) + " secs")   

      

    def spark_theme(self,key,trim = False,uncovered = True):
        try:
            self.init_spark()
        except ValueError:
            pass
        
        self.theme_key = key
        self.temp = self.data.copy()
        self.trim = trim   
        self.uncov_bool = uncovered
        self.start_time_overall = time()
        self.theme_data = pd.read_csv(self.theme_key,encoding='iso-8859-1',error_bad_lines=False,index_col=False)
        from pyspark.sql import SQLContext
        self.sqlContext = SQLContext(self.sc)   
        self.covered_dkeys = []
        
        self.cols = list(self.temp)
        self.fields = [StructField(field_name, StringType(), True) for field_name in self.cols]
        self.schema = StructType(self.fields)
        self.ip = self.sqlContext.createDataFrame(self.temp, self.schema)
        if os.path.isdir("theme//") == False:
            os.makedirs("theme//")
        for i in range(len(self.theme_data)):
            self.t = str(self.theme_data['Theme_Name'][i])
            self.rx = '(?i)'+self.theme_data.Keywords[i]
            self.op = self.ip.where(self.ip[self.content_col].rlike(self.rx)).toPandas()
            self.op[self.content_col] = self.op[self.content_col].str.encode('iso-8859-1') 
            [self.covered_dkeys.append((each)) for each in self.op[self.doc_col]]
            if self.trim == True:
                self.op[[self.doc_col,"AuthorName","ChannelName",self.content_col,self.date_col,"SourceURL",self.sub_sent_col]].to_csv(r'theme//'+ self.t +" "+self.mainname,index=None,encoding = "iso 8859-1")
            else:
                self.op.to_csv(r'theme//'+ self.t +" "+self.mainname,index=None,encoding = "iso 8859-1")
            print("No. of Documents for theme -",self.theme_data.ix[i,"Theme_Name"],"=", len(self.op)) 
            
        if self.uncov_bool == True:
            self.covered_dkeys = [str(each) for each in self.covered_dkeys]
            self.covered_dkeys = list(set(self.covered_dkeys))
            self.temp[self.doc_col] = [str(each) for each in self.temp[self.doc_col]]
            self.all_uncovered = self.temp[~self.temp[self.doc_col].isin(self.covered_dkeys)]
            print("The Uncovered Volume - " + str(self.all_uncovered.shape[0]))
            if self.trim == True:
                self.all_uncovered[[self.doc_col,"AuthorName","ChannelName",self.content_col,self.date_col,"SourceURL",self.sub_sent_col]].to_csv(r'theme//'+ "All Uncov - "+ self.theme_key + " " +self.mainname,index=None)
            else:
                self.all_uncovered.to_csv(r'theme//'+ "All Uncov - "+ self.theme_key + " " +self.mainname,index=None)
                
        self.end_time_overall = time()
        self.sc.stop()
        print ("Overall Running Time : " + str(self.end_time_overall - self.start_time_overall) + " secs") 
    
            
    def copy(self):
        return(copy.deepcopy(self))
        
 
    def organic(self,write = False, official_handle = 0):
        self.official_handle = official_handle
        self.write = write

        print (self.mainname, "is the file currently being processed")
        
        def text_seggregate(df,query):
        	c1=df[df[self.content_col].str.contains(query,case=False)]
        	c2=df[~df[self.content_col].str.contains(query,case=False)]
        	c1=c1.drop_duplicates()
        	c2=c2.drop_duplicates()
        	return (c1,c2)
        	
        def text_seggregate_url(df,query):
        	df = df.fillna(0)
        	c1=df[df['SourceURL'].str.contains(query,case=False)]
        	c2=df[~df['SourceURL'].str.contains(query,case=False)]
        	c1=c1.drop_duplicates()
        	c2=c2.drop_duplicates()
        	return (c1,c2)
        
        if self.official_handle == 0:
            self.Official_handle_text=r"^(?!.*?(\/((apple|applesupport|applemusic|applemusices|applemusicjapan|beats1|applenews|applenewsAU|applenewsUK|appstore|appstoregames|appstoreES|appstoreJapan|beatsbydre|beatssupport|ibooks|itunes|itunesfestival|iTunesMovies|iTunesmusic|iTunesPodcasts|iTunesTrailers|iTunesTV|iTunesU|command_s_bot))\/)|giveaway|celloutfitter)"
        else:
            self.official_handle_text = self.official_handle
	
        self.Organic_Set1=r"^(?!.*?(^(I'|I\s|dear|\smy\s|\smine\s|we\s|us\s|you\s|they\s|it'|it\s|if\s|these\s|those\s|this\s|who\s|their\s|our\s|your\s|hey\s|fuck|have to say|also\s|so\s|wonder|yo\s|but\s|@|OMG|oh\s|ohmy|damn)|(great|awesome|amazing|incredible|good|nice|better|beautiful|wonderful).{0,3}job\b|my job|my new job|(get|getting|got) the job done|got the job|Steve\s?Jobs|(AAPL|Apple).{0,10}(Jobs)|(\bat\b|\bat my|went to|going to|\bin\b|I come to|I go to)\s?\@?Best\s?Buy\b|\@ Best Buy|i\'m going to check it out|will check it out|to win\s?10|(\d+\.?\d\+?\s?hrs?\b)))(?=.*?(i just finished|i just completed|microsoft solitaire|hour of code|i just earned|#free|#deal|#shopping|#dealofthemonth|help us reach \d+ followers|recruitment|#apprentice|#factoftheday|I won the|software test|job.{0,1}description|#anime|ebay|lego\s|gamestop|prize|amazon|tweepstakes|sweepstakes|\bOffers?\b|Deals|Promotion\b|Discount|Best.{0,2}Buy|Giv.{0,4}away|\bto win\b|follow to enter|fill out survey|Win LOADS|Retweet for a|Free.{0,2}Code.{0,2}Friday|Just retweet this|Follow.*?RT|RT.*?Follow|chance to.*?win|bid now|buy.{0,2}it.{0,2}now|auction|enter.{0,4}to win|factory sealed|free shipping|gift.{0,2}card|on sale.*?today|buy.{0,2}now|Wireless Controller With 3\.5mm Stereo Headset Jack|at Porn Site Flip store|follow for a chance|enter contest|multiple codes|microsoft.{0,3}xbox.{0,100}http|http.{0,100}microsoft.{0,3}xbox|console.{0,100}http|http.{0,100}console|better buy|home.{0,6}student|home.{0,6}business|bid now|systems analyst|#hiring|hiring|grab your free place|project manager|buy it now|msftswjobs|free download|webinar|student.{0,6}teacher|student & teacher|full download|register now|free ebook|furniture\s?today|free add-in|free addin|special edition|tablets shop|coupon|christmas special|christmas deal|check it out|shop now|happy hour|new sealed|\sunopened\s|limited edition|disc only|disk only|(job|\bhr\b|hire|hiring|required|needed|recruit|looking for).{0,100}(engineer|programmer|developer|java|c#|c\+\+)|jobrecruit|job recruit|jobsearch|jobalert|job alert|#job|\sjob\s|\sjobs\s|#HR|\shr\s|humanresources|#sex\s|#porn|TrueAchievement|exam dump|thanks for following|Thanks for the follow|#quotes|#trending|Send us a DM|we apologize.{5,20}inconvenience|read more|here\'s how|daily deals:|Free Microsoft Office Templates|rugged case|digital.{0,1}trends|(I have unlocked the).{15,45}(badge)|(I just earned).{10,30}(certification)|(I just got the).{5,25}(badge)|(I just completed).{10,30}(course|daily challenge)|(I just played).{10,30}(Microsoft Solitaire Collection)|excel secret|(\d+).{1,15}(truths|secrets|facts).{1,20}excel|(\d+).{1,15}excel.{1,5}(truths|secrets|facts)|like to hire|(is|i\'m|we\'re|we are|are you|are they).{0,5}hiring|reviewbonuscom|Apply now|how good are you at powerpoint|contact us|Microsoft.{0,1}Reward|MSFT.{0,1}Reward|(budget:).{5,20}(jobs:)))(?=.*?(http))"
	
        self.Organic_Set2=r"(^(I'|I\s|dear|\smy\s|\smine\s|we\s|us\s|you\s|they\s|it'|it\s|if\s|these\s|those\s|this\s|who\s|their\s|our\s|your\s|hey\s|fuck|have to say|also\s|so\s|wonder|yo\s|but\s|@|OMG|oh\s|ohmy|damn)|(^(?!.*?(^(I liked a @YouTube|I added a video to @youtube|I liked a @YouTube|check out|check it out|I added a video to a @youtube|read\s|in the news|must read|top news|new article|I've just posted a new blog|news article|shocking|breaking|new story|top story|new post|retweeted|RT|book now|video\s?:|technews|tech news|download|CVE|new|latest|tech|job|bigtech|test4pass|vacancy|\d+\s(ways|tips|thing|use|application|trick|tool|reason|awesome|fascinat|great|amazing|superb)|whoever stole|hotjobs|hot jobs|preview:|someone stole my|#rt|please rt|via\s|forbestech|forbes tech|mashabletech|mashable tech|techcrunch|wired|lifehacker|forbestech|forbes tech|fastcompany|fast company|gizmodo|engadget|thenextweb|the next web|Business insider|businessinsider|Verge|theverge|CNET|digital trends|digitaltrends|Venture Beat|venturebeat|Pcworld|pc world|TechRadar|tech radar|Tech Republic|tech republic|BGR|Thurrott|mustread|checkout|topnews|newarticle|new article|newsarticle|news|#breaking|#shocking|newstory|topstory|newpost|#checkout|#mustread|#topnews|#new|#newarticle|#latest|#tech|#job|#bigtech|#test4pass|#checkout|#mustread|#topnews|#newarticle|press release|this microsoft video|this microsoft app|you can now|it news)|(forbestech|forbes tech|#rt|exam dump|#news|mashabletech|mashable tech|techcrunch|wired|lifehacker|forbestech|forbes tech|fastcompany|fast company|gizmodo|engadget|thenextweb|the next web|Business insider|businessinsider|Verge|theverge|CNET|digital trends|digitaltrends|Ventrue Beat|venturebeat|Pcworld|pc world|TechRadar|techradar|Tech Republic|techrepublic|BGR|Thurrott|webinar|@youtube|https|webcast|brand new|collector.{0,4}edition|powerpointtips|exceltips|officetips|special article|hiring\s|jobs\s|#marketing|#maketing|microsoft world|periscope|resum.{0,70}(office|word|excel|spreadsheet|powerpoint)|(office|word|excel|spreadsheet|powerpoint).{0,70}resum|watch.{0,5}youtube|veeam|tips.{0,5}trick|#technologynews|#latestitnews|game cd|bitcoin|promo code| %off|% off|#development\s?#html\s?#css\s?#java|tech tip|techtip|read more|(excel|office|powerpoint|ppt|word|outlook|onenote|onedrive).{0,10}tips|tips.{0,10}(excel|office|powerpoint|ppt|word|outlook|onenote|onedrive)|this #?video shows|this #?infographic shows|\b#?MVP\b|Microsoft Innovative Educator|microsoftinnovativeeducator|need help))))|\b(:-\)|:\)|<3|;-\)|:-D|:D|;-D|;D|:\(|:-\(|:-P|:-\\|:'\(|:-\$|:-\[|:\||:-\||:-O|:O|B-\))\b)"
	
        self.Organic_Set3=r"(crash.{0,1}course|\brecommended \w*\s?setting|sleeve.{0,1}case|surface.{0,1}case|case maker|charity.{0,1}tuesday|nillkin|englon case|horse leather|#shopping|achieve?more|i just finished|i just completed|hour of code|i just earned|solitaire.{0,20}achiev|achiev.{0,20}solitaire|\b#anime\b|\bit.{0,1}pro.{0,1}portal\b|#advertis|#branding|#news|zdnet:|christian.{0,1}science.{0,1}monitor|training course|tips.{0,3}tricks|news18|#breakingnews|\bit.{0,1}news:\b|new video added|#freelance|#training|teaching workshop|internship|\btop\s\d+ reasons\b|#fakeheadlinebot|#makeatwitterbot|#stocks|tech.{0,1}news|#mondaymotivation|\bebay\b|\blego\b|\bgamestop\b|\bprize\b|\bamazon\b|\btweepstakes\b|\bsweepstakes\b|\bDeals\b|\bPromotion\b|\bDiscount\b|\bBest.{0,2}Buy\b|\bGiv.{0,4}away\b|\bto win\b|\bfollow to enter|fill out survey|Win LOADS|Retweet for a|Free.{0,2}Code.{0,2}Friday|Just retweet this|Follow for ?RT|chance to win|bid now|buy.{0,2}it.{0,2}now|\bauction\b|enter.{0,4}to win|factory sealed|free shipping|gift.{0,2}card|buy now|Wireless Controller With 3\.5mm Stereo Headset Jack|at Porn Site Flip store|enter contest|bid now|systems analyst|#hiring|\bhiring\b|grab your free place|project manager|buy it now|msftswjobs|free download|webinar|student.{0,6}teacher|student & teacher|full download|register now|free ebook|free add-in|free addin|tablets shop|\bcoupon\b|christmas special|christmas deal|shop now|happy hour|(\bjob\b|\bhr\b|\bhire\b|\bhiring\b|required|needed|recruit|looking for\b).{0,100}(engineer|programmer|developer|java|c#|c\+\+)|jobrecruit|job recruit|jobsearch|jobalert|job alert|#job|#HR|\bhr\b|humanresources|#sex\b|#porn|TrueAchievement|exam dump|thanks for following|Thanks for the follow|#quotes|#trending|Send us a DM|we apologize.{5,20}inconvenience|read more|here\'s how|daily deals:|Free Microsoft Office Templates|rugged case|digital.{0,1}trends|(I have unlocked the).{15,45}(badge)|(I just earned).{10,30}(certification)|(I just got the).{5,25}(badge)|(I just completed).{10,30}(course|daily challenge)|(I just played).{10,30}(Microsoft Solitaire Collection)|excel secret|(\d+).{1,15}(truths|secrets|facts).{1,20}excel|(\d+).{1,15}excel.{1,5}(truths|secrets|facts)|like to hire|(is|i\'m|we\'re|we are|are you|are they).{0,5}hiring|reviewbonuscom|Apply now|how good are you at powerpoint|contact us|Microsoft.{0,1}Reward|MSFT.{0,1}Reward|(budget:).{5,20}(jobs:)|I liked a @YouTube|I added a video to @youtube|I added a video to a @youtube|must read|top news|new article|I\'ve just posted a new blog|news article|top story|new post|retweeted|book now|technews|tech news|\bCVE\b|bigtech|test4pass|\b\d+\s(ways|tips|thing|use|application|trick|tool|reason|awesome|fascinat|great|amazing|superb)|whoever stole|hotjobs|hot jobs|preview:|someone stole my|#rt|please rt|forbestech|forbes tech|mashabletech|mashable tech|techcrunch|wired|lifehacker|forbestech|forbes tech|fastcompany|fast company|gizmodo|engadget|thenextweb|the next web|Business insider|businessinsider|theverge|CNET|digital trends|digitaltrends|Venture Beat|venturebeat|Pcworld|pc world|TechRadar|tech radar|Tech Republic|tech republic|\bBGR\b|Thurrott|mustread|newarticle|newstory|\b#tech\b|\b#bigtech\b|\A(press release|this microsoft video|this microsoft app|you can now|it news|dyk|did you know|how to)|powerpointtips|exceltips|officetips|special article|hiring\b|#marketing|#maketing|microsoft world|periscope|resum.{0,70}(office|word|excel|spreadsheet|powerpoint)|(office|word|excel|spreadsheet|powerpoint).{0,70}resum|veeam|tips.{0,5}trick|#technologynews|#latestitnews|promo code| %off|% off|#development\s?#html\s?#css\s?#java|tech tip|techtip|read more|(excel|office|powerpoint|ppt|word|outlook|onenote|onedrive).{0,10}tips|tips.{0,10}(excel|office|powerpoint|ppt|word|outlook|onenote|onedrive)|this #?video shows|this #?infographic shows|\bMVP\b|pro.{0,2}tip|Microsoft Innovative Educator|microsoftinnovativeeducator|need help|\d\d\-\d\d\d|smarturl|.*?@.*?@.*?@.*?@.*?@.*?@.*?|(:\s[a-zA-Z]{3,15})$)"
	
        self.Twitter_URL=r"(twitter\.com)"
        self.a1=pd.DataFrame()
        self.b1=pd.DataFrame()
        self.c1=pd.DataFrame()  
        
        self.a1 = self.data.copy()
        print ("Filtering to Twitter conversations\n")  
        self.a1 = self.a1.fillna(0)	
        self.b12=text_seggregate_url(self.a1,self.Twitter_URL)
        self.b1=self.b12[0]
        print ("Removing tweets by Microsoft's official handles\n")
        self.ofhandle=text_seggregate_url(self.b1,self.Official_handle_text)
        self.c1=self.ofhandle[0]
        self.c2=self.ofhandle[1]
        print ("Removal of Promotional Documents\n")   
        self.ip = self.c1
        self.op_promo=pd.DataFrame()
        self.op_non_promo=pd.DataFrame()
        self.promotion=text_seggregate(self.ip,self.Organic_Set1)  
        self.op_promo=self.promotion[0]
        self.op_non_promo=self.promotion[1]            
        print ("Organic Split")
        
        self.op_organic=pd.DataFrame()
        self.op_inorganic=pd.DataFrame()
        self.ipo4 = self.op_non_promo.copy()
        self.organic_data=text_seggregate(self.ipo4,self.Organic_Set2)
        self.op_organic=self.organic_data[0]
        self.op_inorganic=self.organic_data[1]
        
        self.ixp = self.op_organic
        self.final_promo=text_seggregate(self.ixp,self.Organic_Set3)
        self.oxp_promo=self.final_promo[0]
        self.oxp_non_promo=self.final_promo[1]
        
        if self.write == True:
            self.oxp_non_promo.to_csv("Organic "+self.filename,index =None)
        return Newfile(filename = "Organic_"+ self.filename,data = self.oxp_non_promo,content_col = self.content_col, sent_col = self.sent_col)
    
    def get_trend_data(self,name = 0,write = False):
        self.trend_data = pd.DataFrame(self.data.groupby([self.date_col])[self.content_col].count())
        self.trend_data = self.trend_data.reset_index()
               
        try:
            self.date_list = [i for i,each  in enumerate(self.trend_data["index"]) if type(self.convert_num(each)) == int]
            self.dates = [int(each) for i,each in enumerate(self.trend_data["index"]) if i in self.date_list]
        except:
            self.date_list = [i for i,each  in enumerate(self.trend_data[self.date_col]) if type(self.convert_num(each)) == int]
            self.dates = [int(each) for i,each in enumerate(self.trend_data[self.date_col]) if i in self.date_list]
            pass       
        self.trend_volume = self.trend_data[self.content_col]
        self.trend_volume = [each for i,each in enumerate(self.trend_volume) if i in self.date_list ]
        self.new_trend_data = pd.DataFrame()
        self.new_trend_data["Dates"] = self.dates
        self.new_trend_data["Count"] = self.trend_volume
        
        self.write_name = name
        if self.write_name == 0:
            self.write_name = self.filename
        
        self.write = write    
        if self.write == True:
            self.new_trend_data.to_csv("trend_"+self.write_name,index = None)
        
        return self.new_trend_data
            
    
    def generate_dates_key(self):
        import pandas as pd
        from dateutil.parser import parse
        from datetime import timedelta
        
        def parsing(inp):
            try:
                v =  parse(inp).strftime("%Y%m%d")
            except:
                v = inp
                pass
            return v
                
        self.data["p_date"] = [parsing(each) for each in self.data[self.date_col].astype(str)]
        self.date_col = "p_date"
    
    def get_trend_data2(self,sent = 0):
        self.sent = sent
        self.t_data = self.data.copy()
        if self.sent != 0:
            self.t_data = self.t_data[self.t_data[self.sub_sent_col]==self.sent]
        
        
        from dateutil.parser import parse
        from datetime import timedelta
        import pandas as pd
        self.x_temp = sorted(self.t_data["p_date"].tolist(),reverse= False)
        self.r_a = self.x_temp[0]
        self.r_b = self.x_temp[len(self.x_temp)-1]

        self.ranges =[]
        for each in range((parse(self.r_b)-parse(self.r_a)).days+1):
            self.ranges.append((parse(self.r_a)+timedelta(each)).strftime("%Y%m%d"))
            
        self.ranges_df = pd.DataFrame()
        self.ranges_df[self.date_col] = self.ranges
        self.date_agg_df = self.t_data.groupby([self.date_col])[self.doc_col].count().reset_index()
        self.trend = pd.merge(self.ranges_df, self.date_agg_df, on=self.date_col,how ="left")
        
        self.trend = self.trend.fillna(0)
        self.trend["trend_date"] = self.trend[self.date_col]
        self.trend_x =  self.trend["trend_date"].tolist()
        self.trend_y = self.trend[self.doc_col].astype(int).tolist()
        self.trend_x = [ parse(each).strftime("%d-%m-%Y")  for each in self.trend_x]
        return[self.trend_x,self.trend_y ]
        
    def get_trend_data3(self,sent = 0,formatted_date = True):
        try:
            self.sentiment_sub(write=False,print_all = False)
        except:
            self.tbl_score()
            self.sentiment_sub(write=False,print_all = False)
        self.sent = sent
        self.formatted_date = formatted_date
        # self.t_data = self.data.copy()
        # if self.sent != 0:
            # self.t_data = self.t_data[self.t_data[self.sub_sent_col]==self.sent]
        
        
        from dateutil.parser import parse
        from datetime import timedelta
        import pandas as pd
        self.x_temp = sorted(self.data["p_date"].tolist(),reverse= False)
        self.r_a = self.x_temp[0]
        self.r_b = self.x_temp[len(self.x_temp)-1]

        self.ranges =[]
        for each in range((parse(self.r_b)-parse(self.r_a)).days+1):
            self.ranges.append((parse(self.r_a)+timedelta(each)).strftime("%Y%m%d"))
            
        self.ranges_df = pd.DataFrame()
        self.ranges_df[self.date_col] = self.ranges
        
        self.d[self.date_col] = self.d[self.d.columns[:1]]
        self.date_agg_df = self.d
        self.trend = pd.merge(self.ranges_df, self.date_agg_df, on=self.date_col,how ="left")
        
        self.trend = self.trend.fillna(0)
        self.trend["trend_date"] = self.trend[self.date_col]
        self.trend_x =  self.trend["trend_date"].tolist()
        self.trend_overall = (self.trend["Very Positive"].astype(int)+ self.trend["Somewhat Positive"].astype(int)) + (self.trend["Very Negative"].astype(int) + self.trend["Somewhat Negative"].astype(int))+ self.trend["Neutral"].astype(int)
        self.trend_overall = self.trend_overall.tolist()
        self.trend_y_positive = (self.trend["Very Positive"].astype(int)+ self.trend["Somewhat Positive"].astype(int)).tolist()
        self.trend_y_negative = (self.trend["Very Negative"].astype(int) + self.trend["Somewhat Negative"].astype(int)).tolist()
        self.trend_y_neutral = self.trend["Neutral"].astype(int).tolist()
        if self.formatted_date == True:
            self.trend_x = [ parse(each).strftime("%d-%m-%Y")  for each in self.trend_x]
        return[self.trend_x,self.trend_overall ,self.trend_y_positive,self.trend_y_negative,self.trend_y_neutral]


            
    def plot_trend(self,target = 0):
        import matplotlib.pyplot as plt
        import numpy as np
        self.target = target
        if self.target == 0:
            self.target = self.sub_sent_col
        self.sentiment_sub()
        self.d = self.d.fillna(0)
        self.trend_data = self.d
        
        self.fig, self.ax = plt.subplots()
        
        if type(self.target) == list:
            self.color_code = {
            "Very Positive":"darkgreen",
            "Somewhat Positive":"lightgreen",
            "Neutral":"darkgrey",
            "Somewhat Negative":"orange",
            "Very Negative":"red",
            "Negative":"red",
            "Positive":"darkgreen"
            }
            for each in self.target:
                if each =="Positive":
                    self.y_trend = [each for each in (self.trend_data["Very Positive"]+self.trend_data["Somewhat Positive"])]
                elif each == "Negative":
                    self.y_trend = [each for each in (self.trend_data["Very Negative"]+self.trend_data["Somewhat Negative"])]
                else:
                    self.y_trend = [each for each in self.trend_data[each]]
                try:
                    self.x_list = [i for i,each  in enumerate(self.trend_data["index"]) if type(self.convert_num(each)) == int]
                    self.x_trend = [int(each) for i,each in enumerate(self.trend_data["index"]) if i in self.x_list]
                except:
                    self.x_list = [i for i,each  in enumerate(self.trend_data[self.date_col]) if type(self.convert_num(each)) == int]
                    self.x_trend = [int(each) for i,each in enumerate(self.trend_data[self.date_col]) if i in self.x_list]
                    pass
                
                self.y_trend = [each for i,each in enumerate(self.y_trend) if i in self.x_list]
                self.y_trend = [str(each) for each in self.y_trend]
                self.ax.plot(self.y_trend,color = self.color_code[each])
                plt.xticks([each for each in range(len(self.x_trend))],self.x_trend)
                
        else:
                   
            self.y_trend = [each for each in self.trend_data[self.target]]
            try:
                self.x_list = [i for i,each  in enumerate(self.trend_data["index"]) if type(self.convert_num(each)) == int]
                self.x_trend = [int(each) for i,each in enumerate(self.trend_data["index"]) if i in self.x_list]
            except:
                self.x_list = [i for i,each  in enumerate(self.trend_data[self.date_col]) if type(self.convert_num(each)) == int]
                self.x_trend = [int(each) for i,each in enumerate(self.trend_data[self.date_col]) if i in self.x_list]
                pass
            self.y_trend = [each for i,each in enumerate(self.y_trend) if i in self.x_list]
            self.y_average = [sum(self.y_trend)/len(self.y_trend) for each in self.y_trend]
            
            self.ax.plot(self.y_trend)
               
            self.ax.plot(self.y_average)
            plt.xticks([each for each in range(len(self.x_trend))],self.x_trend)

        plt.xlabel("TIME",fontsize = 10)
        plt.xticks(fontsize=8, rotation=90)
        plt.ylim(ymin=0)
        plt.show()
        
    

        
    def filter(self, col = 0,targ = 0, condition = "or",regex = False, value_filter = 0,name = 0,write = False,trim = False,return_uncov = False,exact_match = False):
        self.trim = trim
        self.temp = self.data.copy()
        self.uncov_fil = self.data.copy()
        self.regex = regex
        self.tar_col = col
        self.list_flag = 0
        self.exact_match = exact_match
        if type(self.tar_col)==list:
            self.list_flag = 1
        self.return_uncov = return_uncov
        if self.tar_col == 0:
            self.tar_col = self.content_col
        

        self.targets = targ
        self.condition = condition
        self.targets = [str(e).lower() for e in self.targets]
        self.value_filter = value_filter
        self.write = write
        self.temp_name = name

        
        
        if self.condition == "and":
            for i,each in enumerate(self.targets):
                if str(each)[0]=="~":
                    each = str(each)[1:]
                    self.check = False
                else:
                    self.check = True
                if self.regex == True:
                    each = r'%s'%each
                if self.list_flag == 1:
                    if self.exact_match == True:
                        self.temp = self.temp[(self.temp[self.tar_col[i]].str.lower())== each.lower() ]
                    else:
                        self.temp = self.temp[(self.temp[self.tar_col[i]].str.lower()).str.contains(each,flags=re.IGNORECASE, regex=self.regex, na=False) == self.check ]
                else:
                    if self.exact_match == True:
                        self.temp = self.temp[(self.temp[self.tar_col].str.lower())== each.lower() ]
                    else:
                        self.temp = self.temp[(self.temp[self.tar_col].str.lower()).str.contains(each,flags=re.IGNORECASE, regex=self.regex, na=False) == self.check ]
                        
                    
                    
                    
                    
                    
            if self.return_uncov == True:
                self.uncov_fil = self.uncov_fil[~self.uncov_fil.index.isin(self.temp.index)]
        if self.condition == "or":
            
            if self.value_filter == 0:
                self.temp = pd.DataFrame()
                for each in self.targets:
                    if str(each)[0]=="~":
                        each = str(each)[1:]
                        self.check = False
                    else:
                        self.check = True
                    if self.regex == True:
                        each = r'%s'%each
                        # print(each)
                    if self.list_flag == 1:
                        if self.exact_match == True:
                            self.temp2 = self.data[(self.data[self.tar_col[i]].str.lower())== each.lower()]
                        else:
                            self.temp2 = self.data[(self.data[self.tar_col[i]].str.lower()).str.contains(each,flags=re.IGNORECASE, regex=self.regex, na=False) == self.check]
                    else:
                        if self.exact_match == True:
                            self.temp2 = self.data[(self.data[self.tar_col].str.lower())==each.lower()]
                        else:
                            self.temp2 = self.data[(self.data[self.tar_col].str.lower()).str.contains(each,flags=re.IGNORECASE, regex=self.regex, na=False) == self.check]
                    self.temp = pd.concat([self.temp,self.temp2])
                        
                self.temp = self.temp.drop_duplicates()
                if self.return_uncov == True:
                    self.uncov_fil = self.uncov_fil[~self.uncov_fil.index.isin(self.temp.index)]

        if self.value_filter != 0:
            self.temp[self.tar_col] = self.temp[self.tar_col].apply(self.convert_num)
            self.temp[self.tar_col] = self.temp[self.tar_col].astype(str)
            self.target = self.targets[0]
            self.test = (self.temp[self.tar_col])
            
            self.v_list = list(set([self.convert_num(each) for each in self.temp[self.tar_col] if type(self.convert_num(each)) == int]))
            if type(self.target) == str:
                self.target = self.convert_num(self.target)
            
            if self.value_filter == "<":
                self.v_list = [str(each) for each in self.v_list if self.convert_num(each) < self.target]
            elif self.value_filter == ">":
                self.v_list = [str(each) for each in self.v_list if self.convert_num(each) > self.target]
            elif self.value_filter == ">=":
                self.v_list = [str(each) for each in self.v_list if self.convert_num(each) >= self.target]    
            elif self.value_filter == "<=":
                self.v_list = [str(each) for each in self.v_list if self.convert_num(each) <= self.target]            
            
            
            self.temp = self.temp[self.temp[self.tar_col].isin(self.v_list)]
            if self.return_uncov == True:
                self.uncov_fil = self.uncov_fil[~self.uncov_fil.index.isin(self.temp.index)]
        if self.temp_name == 0:
            self.temp_name = "filtered_"+ self.filename
        
        self.update_config()
        if self.trim == True:
            if self.return_uncov == True:
                return Newfile(filename = self.temp_name,config = self.config, data = self.temp[[self.doc_col,"AuthorName","ChannelName",self.content_col,self.date_col,"SourceURL",self.sub_sent_col]], content_col = self.content_col, sent_col = self.sent_col), Newfile(filename = self.temp_name, data = self.uncov_fil[[self.doc_col,"AuthorName","ChannelName",self.content_col,self.date_col,"SourceURL",self.sub_sent_col]], content_col = self.content_col, sent_col = self.sent_col)
            else:
                return Newfile(filename = self.temp_name,config = self.config, data = self.temp[[self.doc_col,"AuthorName","ChannelName",self.content_col,self.date_col,"SourceURL",self.sub_sent_col]], content_col = self.content_col, sent_col = self.sent_col)
        else:
            if self.return_uncov== True:
                return Newfile(filename = self.temp_name,config = self.config, data = self.temp, content_col = self.content_col, sent_col = self.sent_col), Newfile(filename = self.temp_name, data = self.uncov_fil, content_col = self.content_col, sent_col = self.sent_col)
            else:
                return Newfile(filename = self.temp_name,config = self.config, data = self.temp, content_col = self.content_col, sent_col = self.sent_col)
        
        if self.write == True:
            self.temp.to_csv("Filtered "+self.filename,index = None,encoding = "iso 8859-1")
   
    def sp_filter(self, col = 0,targ = 0, condition = "or",regex = False, name = 0,write = False,trim = False,return_uncov = False):
        try:
            self.init_spark()
        except ValueError:
            pass
        self.trim = trim
        self.temp = self.data.copy()
        self.uncov_fil = self.data.copy()
        self.regex = regex
        self.tar_col = col
        self.list_flag = 0
        from pyspark.sql import SQLContext
        self.sqlContext = SQLContext(self.sc)  
        self.cols = list(self.temp)
        if type(self.tar_col)==list:
            self.list_flag = 1
        self.return_uncov = return_uncov
        if self.tar_col == 0:
            self.tar_col = self.content_col
            
        self.temp3 = self.data.copy()
        

        self.targets = targ
        self.condition = condition
        self.targets = [str(e).lower() for e in self.targets]
        self.write = write
        self.temp_name = name
        
        
        if self.condition == "and":
            for i,each in enumerate(self.targets):
            
                self.cols = list(self.temp)
                self.fields = [StructField(field_name, StringType(), True) for field_name in self.cols]
                self.schema = StructType(self.fields)
                self.temp = self.sqlContext.createDataFrame(self.temp, self.schema)
                
                if str(each)[0]=="~":
                    each = str(each)[1:]
                    self.check = False
                else:
                    self.check = True
                if self.regex == True:
                    each = r'%s'%each
                    each = '(?i)'+each
                if self.list_flag == 1:
                    if self.check == True:
                        self.temp = self.temp.where(self.temp[self.tar_col[i]].rlike(each)).toPandas()
                    else:
                        self.temp = self.temp.where(~self.temp[self.tar_col[i]].rlike(each)).toPandas()
                else:
                    if self.check == True:
                        self.temp = self.temp.where(self.temp[self.tar_col].rlike(each)).toPandas()
                    else:
                        self.temp = self.temp.where(~self.temp[self.tar_col].rlike(each)).toPandas()

            if self.return_uncov == True:
                self.uncov_fil = self.uncov_fil[~self.uncov_fil.index.isin(self.temp.index)]
                
                
        if self.condition == "or":
            self.cols = list(self.temp3)
            self.fields = [StructField(field_name, StringType(), True) for field_name in self.cols]
            self.schema = StructType(self.fields)
            self.temp3 = self.sqlContext.createDataFrame(self.temp3, self.schema)   
            self.temp = pd.DataFrame()
            for each in self.targets:
             
                if str(each)[0]=="~":
                    each = str(each)[1:]
                    self.check = False
                else:
                    self.check = True
                if self.regex == True:
                    each = r'%s'%each
                    each = '(?i)'+each
                if self.list_flag == 1:
                    if self.check == True:
                        self.temp2 = self.temp3.where(self.temp3[self.tar_col[i]].rlike(each)).toPandas()
                    else:
                        self.temp2 = self.temp3.where(~self.temp3[self.tar_col[i]].rlike(each)).toPandas()
                else:
                    if self.check == True:
                        self.temp2 = self.temp3.where(self.temp3[self.tar_col].rlike(each)).toPandas()
                    else:
                        self.temp2 = self.temp3.where(~self.temp3[self.tar_col].rlike(each)).toPandas()
                self.temp = pd.concat([self.temp,self.temp2])
                    
            self.temp = self.temp.drop_duplicates()
            if self.return_uncov == True:
                self.uncov_fil = self.uncov_fil[~self.uncov_fil.index.isin(self.temp.index)]


        if self.temp_name == 0:
            self.temp_name = "filtered_"+ self.filename
        
        if self.trim == True:
            if self.return_uncov == True:
                return Newfile(filename = self.temp_name, data = self.temp[[self.doc_col,"AuthorName","ChannelName",self.content_col,self.date_col,"SourceURL",self.sub_sent_col]], content_col = self.content_col, sent_col = self.sent_col), Newfile(filename = self.temp_name, data = self.uncov_fil[[self.doc_col,"AuthorName","ChannelName",self.content_col,self.date_col,"SourceURL",self.sub_sent_col]], content_col = self.content_col, sent_col = self.sent_col)
            else:
                return Newfile(filename = self.temp_name, data = self.temp[[self.doc_col,"AuthorName","ChannelName",self.content_col,self.date_col,"SourceURL",self.sub_sent_col]], content_col = self.content_col, sent_col = self.sent_col)
        else:
            if self.return_uncov== True:
                return Newfile(filename = self.temp_name, data = self.temp, content_col = self.content_col, sent_col = self.sent_col), Newfile(filename = self.temp_name, data = self.uncov_fil, content_col = self.content_col, sent_col = self.sent_col)
            else:
                return Newfile(filename = self.temp_name, data = self.temp, content_col = self.content_col, sent_col = self.sent_col)
                
        if self.write == True:
            self.temp.to_csv("Filtered "+self.filename,index = None,encoding = "iso 8859-1")                
                
                
                
    def append(self,object =0,df=0):
        self.object = object
        self.df = df
        try:
            self.temp = pd.concat([self.data,object.data])
        except:
            self.temp = pd.concat([self.data,df])        
            
        self.temp = self.temp.drop_duplicates()
        return Newfile(data = self.temp)    
        
    def remaining(self,master_object =0,master_df=0):
        self.master_object = master_object
        self.master_df = master_df
        self.data[self.doc_col] = [str(each) for each in self.data[self.doc_col]]
        self.d_keys = self.data[self.doc_col]
        
        try:
            self.master_object.data[self.doc_col] = [str(each) for each in self.master_object.data[self.doc_col]]
            self.temp = self.master_object.data[~self.master_object.data[self.doc_col].isin(self.d_keys)] 
        except:
            self.master_df[self.doc_col] = [str(each) for each in self.master_df[self.doc_col]]
            self.temp = self.master_df[~self.master_df[self.doc_col].isin(self.d_keys)]    
            
        return Newfile(data = self.temp)
        

        
    def chunks(self,target_loc = 0,num_chunk= 50,name = 0):
        import numpy as np
        self.target_loc = target_loc
        
        self.num_chunk = num_chunk
        self.base_name = name
        if self.base_name == 0:
            self.base_name = self.filename
        if os.path.isdir(str(self.target_loc)+"//") == False:
                  
            try:
                os.makedirs(self.target_loc + "//")
            except:
                pass
                
        self.tot_row =self.data.shape[0]
        self.c_size = (self.tot_row/num_chunk)
        import math
        self.c_size= math.floor(self.c_size)
        self.ini = 0
        for each in range(self.num_chunk+2):
            self.chunk = self.data[self.ini:self.ini +self.c_size ]
            self.ini = self.ini + self.c_size
            self.chunk.to_csv(self.target_loc + "\\"+ self.base_name + "_"+ str(each) +".csv",index = None)
            print(self.chunk.shape)
            
    def remove_stopwords(self,text):
        from nltk.tokenize import TweetTokenizer
        self.tknzr = TweetTokenizer(reduce_len = True)
        self.text = text
        self.sp = ["ourselves", "hers", "between", "yourself", "but", "again", "there", "about", "once", "during", "out", "very", "having", "with", "they", "own", "an", "be", "some", "for", "do", "its", "yours", "such", "into", "of", "most", "itself", "other", "off", "is", "s", "am", "or", "who", "as", "from", "him", "each", "the", "themselves", "until", "below", "are", "we", "these", "your", "his", "through", "don", "nor", "me", "were", "her", "more", "himself", "this", "down", "should", "our", "their", "while", "above", "both", "up", "to", "ours", "had", "she", "all", "no", "when", "at", "any", "before", "them", "same", "and", "been", "have", "in", "will", "on", "does", "yourselves", "then", "that", "because", "what", "over", "why", "so", "can", "did", "not", "now", "under", "he", "you", "herself", "has", "just", "where", "too", "only", "myself", "which", "those", "i", "after", "few", "whom", "t", "being", "if", "theirs", "my", "against", "a", "by", "doing", "it", "how", "further", "was", "here", "than","ourselves", "hers", "between", "yourself", "but", "again", "there", "about", "once", "during", "out", "very", "having", "with", "they", "own", "an", "be", "some", "for", "do", "its", "yours", "such", "into", "of", "most", "itself", "other", "off", "is", "s", "am", "or", "who", "as", "from", "him", "each", "the", "themselves", "until", "below", "are", "we", "these", "your", "his", "through", "don", "nor", "me", "were", "her", "more", "himself", "this", "down", "should", "our", "their", "while", "above", "both", "up", "to", "ours", "had", "she", "all", "no", "when", "at", "any", "before", "them", "same", "and", "been", "have", "in", "will", "on", "does", "yourselves", "then", "that", "because", "what", "over", "why", "so", "can", "did", "not", "now", "under", "he", "you", "herself", "has", "just", "where", "too", "only", "myself", "which", "those", "i", "after", "few", "whom", "t", "being", "if", "theirs", "my", "against", "a", "by", "doing", "it", "how", "further", "was", "here", "than"]
        self.text = self.text.lower()
        self.text = re.sub(r'[^\w\s]','',self.text)
        self.text = self.tknzr.tokenize(self.text)
        self.new_text = [each for each in self.text if each not in self.sp]
        # self.new_text = [each for each in self.text.split(" ") if each not in self.sp]
        return self.new_text                

        
    def ngrams(self,range = (1,2),num = 150,write = True,remove_handles = True,print_all = True,vectorizer_parameters = 0,target_col = 0):
        self.ng_vectorizer = vectorizer_parameters
        self.print_all = print_all
        from nltk.tokenize import TweetTokenizer
        self.tknzr = TweetTokenizer(reduce_len = True)
        self.remove_handles = remove_handles
        # from time import time
        self.t1 = time()
        self.write = write
        self.ngram_range = range
        self.ngram_count = num
        self.target_col = target_col
        if self.target_col == 0:
            self.target_col = self.content_col
        
            
        from sklearn.feature_extraction.text import CountVectorizer
        import re
        import string
        
        self.ngram_data = self.data.copy()
        self.ngram_data["s_content"] = self.ngram_data[self.target_col]
        self.ngram_data["s_content"] = [re.sub("https://t.co/[a-zA-Z0-9_a ]{0,10}\S","",each) for each in self.ngram_data["s_content"]]
        self.ngram_data["s_content"] = [re.sub("RT @[a-zA-Z0-9_a]{0,30}\:\s","",each) for each in self.ngram_data["s_content"]]
        
        if self.remove_handles == True:
            self.ngram_data["s_content"] = [re.sub("@[a-zA-Z0-9_a]{0,30}\s","",each) for each in self.ngram_data["s_content"]]
        
        self.ngram_data["s_content"] = [(" ".join(self.remove_stopwords(each))).lower() for each in self.ngram_data["s_content"]]
        if self.ng_vectorizer == 0:
            self.vectorizer = CountVectorizer(ngram_range =self.ngram_range,max_features  = self.ngram_count)
        else:
            self.vectorizer = self.ng_vectorizer
        
        # CountVectorizer(analyzer='word', binary=False, decode_error='strict',
        # dtype=<class 'numpy.int64'>, encoding='utf-8', input='content',
        # lowercase=True, max_df=1.0, max_features=None, min_df=0.0,
        # ngram_range=(1, 1), preprocessor=None, stop_words=None,
        # strip_accents=None, token_pattern='(?u)\\b\\w\\w+\\b',
        # tokenizer=None, vocabulary=None)
        
                
        self.X = self.vectorizer.fit_transform(self.ngram_data["s_content"])
        self.x = self.vectorizer.get_feature_names()[:self.ngram_count]
        # print(len(self.x))
        
        self.ngrams_result = pd.DataFrame()
        self.matrix =  pd.DataFrame(self.X.A, columns=self.vectorizer.get_feature_names())
        
        self.ngrams_result["ngram"] = self.x
        
        self.ngrams_result["count"] = [self.matrix[each].sum() for each in self.x]
        self.ngrams_result = self.ngrams_result.sort_values("count",ascending = False)
        self.ngrams_result= self.ngrams_result.reset_index(drop = True)
        if self.write == True:
            self.ngrams_result.to_csv(self.filename + " ngrams.csv",index = None)
        self.t2 = time()
        if self.print_all == True:
            print("Time Taken : "+ str(self.t2-self.t1))
        return self.ngrams_result
        
            
        
    def birch_cluster(self, num_clusters = 10,birch = 0,vectorizer=0):
        from sklearn.feature_extraction.text import CountVectorizer
        from sklearn.cluster import Birch
        self.num_clusters = num_clusters
        self.birch_parameter = birch
        self.vectorizer_parameter = vectorizer
        self.t1 = time()
        
        self.td = self.data.copy()
        self.td = self.td[self.td["ChannelName"]== "TWITTER/MICROBLOG"]
        self.td = self.td.reset_index(drop=True)
        self.td["t_content"] = [re.sub("https://t.co/[a-zA-Z0-9_a ]{0,10}\S","",each) for each in self.td[self.content_col]]
        self.td["t_content"] = [re.sub("RT @[a-zA-Z0-9_a]{0,30}\:\s","",each) for each in self.td["t_content"]]
        
        
        print("Preprocessing complete - Now fitting the vectorizer")
        if self.vectorizer_parameter == 0:
            self.vector = CountVectorizer(ngram_range = (2,4),max_df = 0.8,min_df = 0.01,stop_words = "english")
        else:
            self.vector = self.vectorizer_parameter
        self.xv = self.vector.fit_transform(self.td["t_content"])
        self.features = self.vector.get_feature_names()
        print("Number of features - " + str(len(self.features)))
        
        print("Applying Clustering - ")
        if self.birch_parameter == 0:
            self.brc = Birch(branching_factor=50, n_clusters=self.num_clusters, threshold=0.5,compute_labels=True)
        else:
            self.brc = self.birch_parameter
        self.brc.fit(self.xv)
        self.c = self.brc.predict(self.xv)
        self.td["cluster"] = self.c
        print("Clustering is complete")
        self.dic = { each:0 for each in set(self.c)}
        for each in self.c:
            self.dic[each] = self.dic[each] + 1
        print("Cluster distribution -")
        print(self.dic)
        
        print("Now returning clusters with Ngrams and agg data")
        self.ng_dim = pd.DataFrame()
        self.ng_dim["cluster"] = 0
        self.ng_dim["ngrams"] = 0
        self.clus = []
        self.ngs = []
        self.all_nf =[]
        
        for each in self.dic:
            self.clus.append(each)
            self.temp = self.td[self.td["cluster"] == each]
            self.all_nf.append(Newfile(data = self.temp))
            self.all_nf[each].filename = "Birch "+str(each) + ".csv"
            self.all_nf[each].agg_sub(print_all = False)
            self.temp = Newfile(data = self.temp)
            self.ng = self.temp.ngrams(range = (2,3),num = 6,remove_handles = False,print_all = False)
            self.xng = self.ng["ngram"].tolist()
            self.xng = self.xng[:20]
            self.ngs.append(self.xng)
        self.ng_dim["cluster"] = self.clus
        self.ng_dim["ngrams"] = self.ngs
        print(self.ng_dim)
        print(self.dic)
        self.t2 = time()
        print("Time Taken : "+ str(self.t2-self.t1))
        return self.all_nf
        
    def dv(self,X,num_clusters = 10,birch = 0):
        self.t1 = time()
        from sklearn.feature_extraction.text import CountVectorizer
        from sklearn.cluster import Birch
        self.td = self.data.copy()
        self.X = X
        self.num_clusters = num_clusters
        self.birch_parameter = birch
        
        if self.birch_parameter == 0:
            self.brc = Birch(branching_factor=50, n_clusters=self.num_clusters, threshold=0.2,compute_labels=True)
        else:
            self.brc = self.birch_parameter
            
            
        self.brc.fit(self.X)
        self.c = self.brc.predict(self.X)
        self.td["cluster"] = self.c
        print("Clustering is complete")
        self.dic = { each:0 for each in set(self.c)}
        for each in self.c:
            self.dic[each] = self.dic[each] + 1
        print("Cluster distribution -")
        print(self.dic)
        
        print("Now returning clusters with Ngrams and agg data")
        self.ng_dim = pd.DataFrame()
        self.ng_dim["cluster"] = 0
        self.ng_dim["ngrams"] = 0
        self.clus = []
        self.ngs = []
        self.all_nf =[]
        
        for each in self.dic:
            self.clus.append(each)
            self.temp = self.td[self.td["cluster"] == each]
            self.all_nf.append(Newfile(data = self.temp))
            self.all_nf[each].filename = "Birch "+str(each) + ".csv"
            self.all_nf[each].agg(print_all = False)
            self.temp = Newfile(data = self.temp)
            self.ng = self.temp.ngrams(range = (2,3),num = 6,remove_handles = False,print_all = False)
            self.xng = self.ng["ngram"].tolist()
            self.xng = self.xng[:20]
            self.ngs.append(self.xng)
        self.ng_dim["cluster"] = self.clus
        self.ng_dim["ngrams"] = self.ngs
        print(self.ng_dim)
        print(self.dic)
        self.t2 = time()
        print("Time Taken : "+ str(self.t2-self.t1))
        return self.all_nf        
        
        
    def preprocess(self,lemmatize = False,remove_stopwords = True,phraser = False):
        def lemmatize(text):
            text = text.split(" ")
            text = [wordnet_lemmatizer.lemmatize(each) for each in text]
            text = " ".join(text)
            return text
        
        self.data["p_content"] = [str(each) for each in self.data[self.content_col]]
        self.data["p_content"] = [re.sub("https://t.co/[a-zA-Z0-9_a ]{0,10}\S","",each) for each in self.data["p_content"]]
        # self.data["p_content"] = [re.sub("RT @[a-zA-Z0-9_a]{0,30}\:\s","",each) for each in self.data[  "p_content"]]
        # self.data["p_content"] = [re.sub("@[a-zA-Z0-9_a]{0,30}\s","",each) for each in self.data["p_content"]]
        self.data["p_content"] = [re.sub("@","",each) for each in self.data["p_content"]]
        self.data["p_content"] = [re.sub(r'[^\w\s]','',each) for each in self.data["p_content"]]
        # self.data["p_content"] = [(" ".join(self.remove_stopwords(each))).lower() for each in self.data["p_content"]]
        self.data["p_content"] = [(" ".join(each.split(" "))).lower() for each in self.data["p_content"]]
        
        if remove_stopwords == True:
            self.data["p_content"] = [ " ".join(self.remove_stopwords(each)) for each in self.data["p_content"]]
            
        
        if lemmatize == True:
            self.data["p_content"]= [ lemmatize(each) for each in self.data["p_content"]]
        self.data["p_content"] = [ each.encode("ASCII","ignore").decode("utf-8") for each in self.data["p_content"]]      

        if phraser == True:
            from gensim.test.utils import common_texts
            from gensim.models import Phrases
            from gensim.test.utils import datapath
            from gensim.models import Word2Vec
            from gensim.models.phrases import Phrases, Phraser
            self.l = [ each.split(" ") for each in self.data["p_content"]]
            self.phrases = Phrases(self.l, min_count=50, threshold=7) 
            self.bigram = Phraser(self.phrases)
            self.data["p_content"] = [" ".join(self.bigram[each]) for each in self.l]
            
        
        
###################### THEME CLUSTER HELPER FUNCTIONS ###########################################        
    def sum_feature_wv(self,text,word_vec):
        import numpy as np
        self.text = text
        self.text = self.text.split(" ")
        self.t_word_dic = word_vec
        self.t=  [self.t_word_dic[each] for each in self.text if each in self.t_word_dic]
        if self.t == []:
            return np.zeros([self.word_vec_dim,])
#        del self.t_word_dic
        return np.sum(self.t,axis = 0)/len(self.t)
    
    def average_wd(self,text):
        import numpy as np
        self.text = text
        self.total = [self.X_dic[each] for each in self.vf if each in self.text]
        self.l = len(self.total)
        if self.l == 0:
    #        print(text)
            return np.zeros([self.word_vec_dim,])
            # return "XX"
        else:
            self.fin = (np.sum(self.total,axis = 0))/self.l
     
        return self.fin
        
        
    def average_wd2(self,text):
        import numpy as np
        self.text = text
        self.total = [self.X_dic[each] for each in self.vf if each in self.text]
        self.l = len(self.total)
        if self.l == 0:
    #        print(text)
            # return np.zeros([self.word_vec_dim,])
            return "XX"
        else:
            self.fin = (np.sum(self.total,axis = 0))/self.l
     
        return self.fin

        
    def average_wd3(self,text):
        import numpy as np
        self.text = text
        self.total = [self.word_vec[each] for each in self.text if each in self.word_vec]
        self.l = len(self.total)
        if self.l == 0:
    #        print(text)
            # return np.zeros([self.word_vec_dim,])
            return "XX"
        else:
            self.fin = (np.sum(self.total,axis = 0))/self.l

        return self.fin
        
    def features_present(self,text):
        self.text = text
        self.feats_present = list(set((" ".join([each for each in self.vf if each in self.text])).split(" ")))
        self.res = self.average_wd3(self.feats_present)
        return self.res
        
#################################################################################################        
    
    def get_word_vec(self,word_vec_dim = 100):
        import numpy as np
        from gensim.models import Word2Vec
        self.word_vec_dim = word_vec_dim
#        self.td = 
        try:
#            self.data["p_content"] = self.data["p_content"]
            self.w_data= self.data["p_content"]
        except:
            self.preprocess()
            self.w_data = self.data["p_content"]
        
        self.word_list = [each.split(" ") for each in self.w_data]
        self.wv_model = Word2Vec(self.word_list, min_count=1,size= self.word_vec_dim,seed = 24,sg = 1,iter = 10)
        self.X = self.wv_model[self.wv_model.wv.vocab]
        self.words = list(self.wv_model.wv.vocab)
        self.word_dic = {each:(self.X[i]) for i,each in enumerate(self.words)}
        return self.word_dic
        
    def theme_cluster3(self,cluster_parameters=0,vectorizer_parameters=0,word_vec=0,num_clusters = 15,add_features = [],use_features =[],output_filename = "",print_all = True,write = True):
        self.write = write
        from gensim.summarization import summarize  
        from sklearn.cluster import Birch
        from sklearn.feature_extraction.text import CountVectorizer
        import numpy as np
        self.update_config()
        self.print_all = print_all
        
        self.label_name = []
        self.add_features = add_features
        self.use_features = use_features
        self.output_filename = output_filename
        if self.output_filename == "":
            self.output_filename = "Birch "
        self.t1 = time()
        self.cluster_model = cluster_parameters
        self.vectorizer = vectorizer_parameters
        self.word_vec = word_vec
        self.num_clusters = num_clusters
        
        if self.cluster_model ==0:
            self.cluster_model = Birch(branching_factor=50, n_clusters=self.num_clusters , threshold=0.5,compute_labels=True)
        if self.vectorizer == 0:
            self.vectorizer = CountVectorizer(ngram_range = (2,4),max_df = 0.8,min_df = 0.01,stop_words = {'english'})
        if self.word_vec == 0:
            self.word_vec = self.get_word_vec()
        
        self.word_vec_dim = [len(value) for key,value in self.word_vec.items()][0]
            
        try:
            self.w_data= self.data["p_content"]
        except:
            self.preprocess()
            self.w_data = self.data["p_content"]    
            
        self.data_c = self.data.copy()
        
        
        if self.use_features == []:
            self.X = self.vectorizer.fit(self.w_data)
            self.vf = self.vectorizer.get_feature_names()
            self.vf = self.vf + self.add_features
        else:
            self.vf = self.use_features
            
        self.vf = list(set(self.vf))
        print("Features Count "+str(len(self.vf)))
        # self.X_vec = [self.sum_feature_wv(each,word_vec = self.word_vec) for each in self.vf]
        # self.X_vec = [self.sum_feature_wv(each,word_vec = self.word_vec) for each in self.vf]
        # self.X_dic = {each:self.X_vec[i] for i,each in enumerate(self.vf)}
        self.data_c["Vec"] = [self.features_present(each) for each in self.w_data]
        self.index_no_feature = [i for i,each in enumerate(self.data_c["Vec"]) if each == "XX"]
        self.sparse_cluster = self.data_c[self.data_c.index.isin(self.index_no_feature)]
        self.data_c = self.data_c[~self.data_c.index.isin(self.index_no_feature)]
        
        self.X = ([(each) for each in self.data_c["Vec"]])
        print("Vectors ready")
        print(len(self.X))
        self.c = self.cluster_model.fit_predict(self.X)
        self.data_c["cluster"] = self.c
        print("Clustering is complete")
        self.dic = { each.tolist():0 for each in set(self.c)}
        for each in self.c:
            self.dic[each] = self.dic[each] + 1
        print("Cluster distribution -")
        print(self.dic)
        
        print("Now returning clusters with Ngrams and agg data")
        self.ng_dim = pd.DataFrame()
        self.ng_dim["cluster"] = 0
        self.ng_dim["ngrams"] = 0
        self.clus = []
        self.ngs = []
        self.all_nf =[]
        self.score = {}
        self.cluster_agg = []
            
        for i,each in enumerate(self.dic):
            self.clus.append(each)
            print(len(self.dic))
            self.temp = self.data_c[self.data_c["cluster"] == each]
            print(self.temp.shape)
            self.all_nf.append(Newfile(data = self.temp,config = self.config))
            print(each)
            print(len(self.all_nf))
            
            self.all_nf[i].filename = self.output_filename+str(i) + ".csv"
            self.agg_temp = self.all_nf[i].agg(print_all = False,return_agg = True,write = False)
            if self.write == True:
                self.agg_temp.to_csv(self.all_nf[i].filename)
            self.cluster_agg.append(self.agg_temp)
            self.temp = Newfile(data = self.temp,config = self.config)
            # self.cluster_average = np.sum(self.temp.data["Vec"])/len(self.temp.data)
            # self.cluster_variance = (np.sum([np.square(self.cluster_average - each) for each in self.temp.data["Vec"]]))/len(self.temp.data)
            # self.cluster_score = 1/(self.cluster_variance+0.01)
            # self.score[i]= (self.cluster_score)
            
            print("Cluster Number : "+str(i))
            try: ## some clusters data would have only one words
                # self.vp = CountVectorizer(range(1,3))
                self.ng = self.temp.ngrams(range = (1,3),num = 6,remove_handles = False,print_all = False,vectorizer_parameters = self.vectorizer,target_col = "p_content",write = False)
            except:
                self.ng = self.temp.ngrams(range = (1,4),num = 6,remove_handles = False,print_all = False,vectorizer_parameters = 0,write = False)
            self.xng = self.ng["ngram"].tolist()
            self.xng = self.xng[:20]
            
            self.ngs.append(self.xng)
            if self.print_all == True:
                print(" ")
                print(self.xng)
                # print(self.temp.vector_variance_summarizer())
                print("++++++++++++++++++++++++++++++++")
            self.label_name.append(self.temp.vector_variance_summarizer())

                    
        self.ng_dim["cluster"] = self.clus
        self.ng_dim["ngrams"] = self.ngs
        print(self.ng_dim)
        print(self.dic)
        self.t2 = time()
        print("Time Taken : "+ str(self.t2-self.t1))
        return self.all_nf            

        
        
    def theme_cluster(self,cluster_parameters=0,vectorizer_parameters=0,word_vec=0,num_clusters = 15,add_features = [],use_features =[],output_filename = ""):
        from gensim.summarization import summarize  
        from sklearn.cluster import Birch
        from sklearn.feature_extraction.text import CountVectorizer
        import numpy as np
        self.update_config()
        self.label_name = []
        self.add_features = add_features
        self.use_features = use_features
        self.output_filename = output_filename
        if self.output_filename == "":
            self.output_filename = "Birch "
        self.t1 = time()
        self.cluster_model = cluster_parameters
        self.vectorizer = vectorizer_parameters
        self.word_vec = word_vec
        self.num_clusters = num_clusters
        
        if self.cluster_model ==0:
            self.cluster_model = Birch(branching_factor=50, n_clusters=self.num_clusters , threshold=0.5,compute_labels=True)
        if self.vectorizer == 0:
            self.vectorizer = CountVectorizer(ngram_range = (2,4),max_df = 0.8,min_df = 0.01)
        if self.word_vec == 0:
            self.word_vec = self.get_word_vec()
        
        self.word_vec_dim = [len(value) for key,value in self.word_vec.items()][0]
            
        try:
            self.w_data= self.data["p_content"]
        except:
            self.preprocess()
            self.w_data = self.data["p_content"]    
        
        
        if self.use_features == []:
            self.X = self.vectorizer.fit(self.w_data)
            self.vf = self.vectorizer.get_feature_names()
            self.vf = self.vf + self.add_features
        else:
            self.vf = self.use_features
            
        self.vf = list(set(self.vf))
        print("Features Count "+str(len(self.vf)))
        self.X_vec = [self.sum_feature_wv(each,word_vec = self.word_vec) for each in self.vf]
        self.X_dic = {each:self.X_vec[i] for i,each in enumerate(self.vf)}
        self.data["Vec"] = [self.average_wd(each) for each in self.w_data]
        
        self.X = [(each) for each in self.data["Vec"]]
        print("Vectors ready")
        self.c = self.cluster_model.fit_predict(self.X)
        self.data["cluster"] = self.c
        print("Clustering is complete")
        self.dic = { each.tolist():0 for each in set(self.c)}
        for each in self.c:
            self.dic[each] = self.dic[each] + 1
        print("Cluster distribution -")
        print(self.dic)
        
        print("Now returning clusters with Ngrams and agg data")
        self.ng_dim = pd.DataFrame()
        self.ng_dim["cluster"] = 0
        self.ng_dim["ngrams"] = 0
        self.clus = []
        self.ngs = []
        self.all_nf =[]
        self.score = {}
        self.cluster_agg = []
        
        for i,each in enumerate(self.dic):
            self.clus.append(each)
            self.temp = self.data[self.data["cluster"] == each]
            self.all_nf.append(Newfile(data = self.temp))
            self.all_nf[each].filename = self.output_filename+str(each) + ".csv"
            self.agg_temp = self.all_nf[each].agg(print_all = False,return_agg = True,write = True)
            self.cluster_agg.append(self.agg_temp)
            self.temp = Newfile(data = self.temp)
            self.cluster_average = np.sum(self.temp.data["Vec"])/len(self.temp.data)
            self.cluster_variance = (np.sum([np.square(self.cluster_average - each) for each in self.temp.data["Vec"]]))/len(self.temp.data)
            self.cluster_score = 1/(self.cluster_variance+0.01)
            self.score[i]= (self.cluster_score)
            
            print("Cluster Number : "+str(i))
            self.ng = self.temp.ngrams(range = (2,4),num = 6,remove_handles = False,print_all = False,vectorizer_parameters = 0)
            self.xng = self.ng["ngram"].tolist()
            self.xng = self.xng[:20]
            self.ngs.append(self.xng)
            
            if self.temp.shape[0]>=300:       
                print(self.temp.shape[0])
                print(self.xng)
                # self.label_name.append(self.xng[:3])
                self.label_name.append("")
                print("=========================================")
            else:
                self.tng = [e for e in self.temp.data["p_content"]]
                self.tng = ". ".join(self.tng)
                try:
                    self.tng = summarize(self.tng,word_count =15)
                except:
                    self.tng = self.tng
                # self.tng = sumy(self.tng)
                if self.tng!= "":
                    print(self.temp.shape[0])
                    print(self.tng)
                    self.label_name.append(self.tng)
                    print("=========================================")
                else:
                    print(self.temp.shape[0])
                    print(self.xng)
                    # self.label_name.append(self.xng[:3])
                    self.label_name.append("")
                    print("=========================================")
                    
        self.ng_dim["cluster"] = self.clus
        self.ng_dim["ngrams"] = self.ngs
        print(self.ng_dim)
        print(self.dic)
        self.t2 = time()
        print("Time Taken : "+ str(self.t2-self.t1))
        return self.all_nf        

    def theme_cluster2(self,cluster_parameters=0,vectorizer_parameters=0,word_vec=0,num_clusters = 15,add_features = [],use_features =[],output_filename = "",print_all = True,write = True):
        from gensim.summarization import summarize  
        from sklearn.cluster import Birch
        from sklearn.feature_extraction.text import CountVectorizer
        import numpy as np
        self.update_config()
        self.print_all = print_all
        self.write = write
        
        self.label_name = []
        self.add_features = add_features
        self.use_features = use_features
        self.output_filename = output_filename
        if self.output_filename == "":
            self.output_filename = "Birch "
        self.t1 = time()
        self.cluster_model = cluster_parameters
        self.vectorizer = vectorizer_parameters
        self.word_vec = word_vec
        self.num_clusters = num_clusters
        
        if self.cluster_model ==0:
            self.cluster_model = Birch(branching_factor=50, n_clusters=self.num_clusters , threshold=0.5,compute_labels=True)
        if self.vectorizer == 0:
            self.vectorizer = CountVectorizer(ngram_range = (2,4),max_df = 0.8,min_df = 0.01)
        if self.word_vec == 0:
            self.word_vec = self.get_word_vec()
        
        self.word_vec_dim = [len(value) for key,value in self.word_vec.items()][0]
            
        try:
            self.w_data= self.data["p_content"]
        except:
            self.preprocess()
            self.w_data = self.data["p_content"]    
            
        self.data_c = self.data.copy()
        
        
        if self.use_features == []:
            try:
                self.X = self.vectorizer.fit(self.w_data)
            except:
                print("no max df")
                self.vectorizer = CountVectorizer(ngram_range = (1,3))
                self.X = self.vectorizer.fit(self.w_data)
            self.vf = self.vectorizer.get_feature_names()
            self.vf = self.vf + self.add_features
        else:
            self.vf = self.use_features
            
        self.vf = list(set(self.vf))
        print("Features Count "+str(len(self.vf)))
        self.X_vec = [self.sum_feature_wv(each,word_vec = self.word_vec) for each in self.vf]
        # self.X_vec = [self.sum_feature_wv(each,word_vec = self.word_vec) for each in self.vf]
        self.X_dic = {each:self.X_vec[i] for i,each in enumerate(self.vf)}
        self.data_c["Vec"] = [self.average_wd2(each) for each in self.w_data]
        self.index_no_feature = [i for i,each in enumerate(self.data_c["Vec"]) if each == "XX"]
        self.sparse_cluster = self.data_c[self.data_c.index.isin(self.index_no_feature)]
        self.data_c = self.data_c[~self.data_c.index.isin(self.index_no_feature)]
        
        self.X = ([(each) for each in self.data_c["Vec"]])
        print("Vectors ready")
        print(len(self.X))
        self.c = self.cluster_model.fit_predict(self.X)
        self.data_c["cluster"] = self.c
        print("Clustering is complete")
        self.dic = { each.tolist():0 for each in set(self.c)}
        for each in self.c:
            self.dic[each] = self.dic[each] + 1
        print("Cluster distribution -")
        print(self.dic)
        
        print("Now returning clusters with Ngrams and agg data")
        self.ng_dim = pd.DataFrame()
        self.ng_dim["cluster"] = 0
        self.ng_dim["ngrams"] = 0
        self.clus = []
        self.ngs = []
        self.all_nf =[]
        self.score = {}
        self.cluster_agg = []
        
        for i,each in enumerate(self.dic):
            self.clus.append(each)
            print(len(self.dic))
            self.temp = self.data_c[self.data_c["cluster"] == each]
            print(self.temp.shape)
            self.all_nf.append(Newfile(data = self.temp,config = self.config))
            print(each)
            print(len(self.all_nf))
            
            self.all_nf[i].filename = self.output_filename+str(i) + ".csv"
            self.agg_temp = self.all_nf[i].agg(print_all = False,return_agg = True,write = False)
            self.cluster_agg.append(self.agg_temp)
            self.temp = Newfile(data = self.temp,config = self.config)
            # self.cluster_average = np.sum(self.temp.data["Vec"])/len(self.temp.data)
            # self.cluster_variance = (np.sum([np.square(self.cluster_average - each) for each in self.temp.data["Vec"]]))/len(self.temp.data)
            # self.cluster_score = 1/(self.cluster_variance+0.01)
            # self.score[i]= (self.cluster_score)
            
            print("Cluster Number : "+str(i))
            try: ## some clusters data would have only one words
                self.ng = self.temp.ngrams(range = (2,4),num = 6,remove_handles = False,print_all = False,vectorizer_parameters = 0,write = False)
            except:
                self.ng = self.temp.ngrams(range = (1,4),num = 6,remove_handles = False,print_all = False,vectorizer_parameters = 0,write = False)
            self.xng = self.ng["ngram"].tolist()
            self.xng = self.xng[:20]
            
            self.ngs.append(self.xng)
            if self.print_all == True:
                print(" ")
                print(self.xng)
                # print(self.temp.vector_variance_summarizer())
                print("++++++++++++++++++++++++++++++++")
            self.label_name.append(self.temp.vector_variance_summarizer())
         

                    
        self.ng_dim["cluster"] = self.clus
        self.ng_dim["ngrams"] = self.ngs
        print(self.ng_dim)
        print(self.dic)
        self.t2 = time()
        print("Time Taken : "+ str(self.t2-self.t1))
        return self.all_nf       

        
    def vector_variance_summarizer(self):
        # try:
            # self.data.p_content
        # except:
            # self.preprocess()
        import numpy as np
        from gensim.summarization import summarize  
        try:
            self.sqd = (np.square(self.data["Vec"]-self.data["Vec"].mean()))
            self.sqd = self.sqd.apply(lambda a: sum(a))
            self.sqd_i = self.sqd.sort_values().index[:300]
            self.sqd_t = self.data[self.data.index.isin(self.sqd_i)]
            self.sqd_t = self.sqd_t["p_content"]            
            self.smzr_feed = ". ".join([e for e in self.sqd_t])
            return summarize(self.smzr_feed,word_count =15)
        except:
            return "00"    
 

    def theme_cluster4(self,cluster_parameters=0,vectorizer_parameters=0,word_vec=0,num_clusters = 15,add_features = [],use_features =[],output_filename = "",print_all = True,write = True):   #with BLOGS
        from gensim.summarization import summarize  
        from sklearn.cluster import Birch
        from sklearn.feature_extraction.text import CountVectorizer
        from sklearn.cluster import MiniBatchKMeans
        import numpy as np
        self.update_config()
        self.print_all = print_all
        self.write = write
        
        self.label_name = []
        self.add_features = add_features
        self.use_features = use_features
        self.output_filename = output_filename
        if self.output_filename == "":
            self.output_filename = "Birch "
        self.t1 = time()
        self.cluster_model = cluster_parameters
        self.vectorizer = vectorizer_parameters
        self.word_vec = word_vec
        self.num_clusters = num_clusters
        
        if self.cluster_model ==0:
            # self.cluster_model = Birch(branching_factor=50, n_clusters=self.num_clusters , threshold=0.5,compute_labels=True)
            self.cluster_model = MiniBatchKMeans(n_clusters =int(self.num_clusters),random_state = 10,batch_size = 2000)
        if self.vectorizer == 0:
            self.vectorizer = CountVectorizer(ngram_range = (2,4),max_df = 0.8,min_df = 0.01)
        if self.word_vec == 0:
            self.word_vec = self.get_word_vec()
        
        self.word_vec_dim = [len(value) for key,value in self.word_vec.items()][0]
            
        try:
            self.w_data= self.data["p_content"]
        except:
            self.preprocess()
            self.w_data = self.data["p_content"]    
            
        self.data_c = self.data.copy()
        
        
        if self.use_features == []:
            try:
                self.X = self.vectorizer.fit(self.w_data)
            except:
                print("no max df")
                self.vectorizer = CountVectorizer(ngram_range = (1,3))
                self.X = self.vectorizer.fit(self.w_data)
            self.vf = self.vectorizer.get_feature_names()
            self.vf = self.vf + self.add_features
        else:
            self.vf = self.use_features
            
        self.vf = list(set(self.vf))
        print("Features Count "+str(len(self.vf)))
        self.X_vec = [self.sum_feature_wv(each,word_vec = self.word_vec) for each in self.vf]
        self.X_vec = [self.sum_feature_wv(each,word_vec = self.word_vec) for each in self.vf]
        self.X_dic = {each:self.X_vec[i] for i,each in enumerate(self.vf)}
        self.data_c["Vec"] = [self.average_wd2(each) for each in self.w_data]
        self.index_no_feature = [i for i,each in enumerate(self.data_c["Vec"]) if each == "XX"]
        self.sparse_cluster = self.data_c[self.data_c.index.isin(self.index_no_feature)]
        self.data_c = self.data_c[~self.data_c.index.isin(self.index_no_feature)]
        
        self.X = ([(each) for each in self.data_c["Vec"]])
        print("Vectors ready")
        print(len(self.X))
        self.c = self.cluster_model.fit_predict(self.X)
        self.data_c["cluster"] = self.c
        print("Clustering is complete")
        self.dic = { each.tolist():0 for each in set(self.c)}
        for each in self.c:
            self.dic[each] = self.dic[each] + 1
        print("Cluster distribution -")
        print(self.dic)
        
        print("Now returning clusters with Ngrams and agg data")
        self.ng_dim = pd.DataFrame()
        self.ng_dim["cluster"] = 0
        self.ng_dim["ngrams"] = 0
        self.clus = []
        self.ngs = []
        self.all_nf =[]
        self.score = {}
        self.cluster_agg = []
        
        for i,each in enumerate(self.dic):
            self.clus.append(each)
            print(len(self.dic))
            self.temp = self.data_c[self.data_c["cluster"] == each]
            print(self.temp.shape)
            self.all_nf.append(Newfile(data = self.temp,config = self.config))
            print(each)
            print(len(self.all_nf))
            
            self.all_nf[i].filename = self.output_filename+str(i) + ".csv"
            self.agg_temp = self.all_nf[i].agg(print_all = False,return_agg = True,write = False)
            self.cluster_agg.append(self.agg_temp)
            self.temp = Newfile(data = self.temp,config = self.config)
            # self.cluster_average = np.sum(self.temp.data["Vec"])/len(self.temp.data)
            # self.cluster_variance = (np.sum([np.square(self.cluster_average - each) for each in self.temp.data["Vec"]]))/len(self.temp.data)
            # self.cluster_score = 1/(self.cluster_variance+0.01)
            # self.score[i]= (self.cluster_score)
            
            print("Cluster Number : "+str(i))
            self.ng = self.temp.ngrams(range = (2,4),num = 6,remove_handles = False,print_all = False,vectorizer_parameters = 0,write = False)
            self.xng = self.ng["ngram"].tolist()
            self.xng = self.xng[:20]
            
            self.ngs.append(self.xng)
            if self.print_all == True:
                print(" ")
                print(self.xng)
                # print(self.temp.vector_variance_summarizer())
                print("++++++++++++++++++++++++++++++++")
            self.label_name.append(self.temp.vector_variance_summarizer())
         

                    
        self.ng_dim["cluster"] = self.clus
        self.ng_dim["ngrams"] = self.ngs
        print(self.ng_dim)
        print(self.dic)
        self.t2 = time()
        print("Time Taken : "+ str(self.t2-self.t1))
        return self.all_nf,self.data_c            
    def vector_variance_summarizer(self):
        # try:
            # self.data.p_content
        # except:
            # self.preprocess()
        import numpy as np
        from gensim.summarization import summarize  
        try:
            self.sqd = (np.square(self.data["Vec"]-self.data["Vec"].mean()))
            self.sqd = self.sqd.apply(lambda a: sum(a))
            self.sqd_i = self.sqd.sort_values().index[:300]
            self.sqd_t = self.data[self.data.index.isin(self.sqd_i)]
            self.sqd_t = self.sqd_t["p_content"]            
            self.smzr_feed = ". ".join([e for e in self.sqd_t])
            return summarize(self.smzr_feed,word_count =15)
        except:
            return "00"    
 
  
			
			
    def t_sentiment_function(self,text):
        from textblob import TextBlob
        try:
            sent_score = TextBlob(text).sentiment.polarity
        except:
            sent_score = TextBlob(str(text)).sentiment.polarity
        # sent_score =  sent_text.sentiment.polarity
        tag = 0
        if sent_score < 0.3 and sent_score >= 0:
            tag = "Neutral"
        elif sent_score >= 0.3 and sent_score < 0.7:
            tag = "Somewhat Positive"
        elif sent_score >= 0.7:
            tag = "Very Positive"
        elif sent_score < 0 and sent_score > -0.7:
            tag = "Somewhat Negative"
        else:
            tag = "Very Negative"
        return tag
    
    
    def tbl_score(self):
        
        self.data["t_sentiment"] = [self.t_sentiment_function(each) for each in self.data[self.content_col] ]
        
        self.sub_sent_col = "t_sentiment"
        self.update_config()
        
    def cnn_score(self,glv_file,model= 0,loaded_model = 0):
        from Package.Models import load_glove_model,cnn_sent,simpleCNN,Flatten
        self.glv = (glv_file)
        self.sent_model = model
        self.loaded_model = loaded_model
        try:
            self.data["p_content"]
        except:
            self.preprocess()
        if self.loaded_model == 0:
            self.data["t_sentiment"] = cnn_sent(self.data["p_content"],glv_name = self.glv,trained_model = self.sent_model)
        else:
            self.data["t_sentiment"] = cnn_sent(self.data["p_content"],glv_name = self.glv,trained_model = 0,loaded_model = self.loaded_model)
        self.sub_sent_col = "t_sentiment"
        self.update_config()
        
    # def cnn_score(self,trained_model,glv_name = r"D:/SENTIMENT CLASSIFIER/glove.6B.50d.txt" ):
        # self.preprocess()
        # self.glv = self.loadGloveModel(glv_name)
        # self.cnn_data = self.data.copy()
        # from nltk.tokenize import TweetTokenizer
        # from Package import Newfile as nf
        # import torch
        # from torch import nn
        # from torch.nn.modules.padding import ConstantPad1d,ReflectionPad1d
        # from torch.nn.utils.rnn import pack_padded_sequence, pad_packed_sequence
        # import time
        # import pandas as pd
        # from sklearn.model_selection import train_test_split
        # import time
        # import numpy as np
        # from Package import Newfile as nf
        # import pandas as pd
        # import numpy as np
        
        
        # class Flatten(nn.Module):
            # def forward(self, x):
                # return x.contiguous().view(x.numel())    
        # class simpleCNN(torch.nn.Module):
            # def __init__(self):
                # super(simpleCNN,self).__init__()
                # self.conv1 = nn.Sequential(),  
                # self.flat = Flatten()
                # self.fc = nn.Sequential()
                # self.conv1 = self.conv1.double()
                # self.fc = self.fc.double()
                    
            # def forward(self,x):
                # cf = self.conv1(x)
                # c = self.flat.forward(cf)
                # ans = self.fc(c)
                # return ans
        
        # def get_vectors(text):
            # all_vectors = [glv[each] for each in text.split(" ") if each in glv]
            # temp = np.zeros((50))
            # all_vectors = all_vectors[:250]
            
            # if len(all_vectors) <250:
                # temp_range = 250-len(all_vectors)
                # temp_list = temp.tolist()
                # [all_vectors.append(temp_list)for each in range(temp_range)]
            
            # all_vectors = np.array(all_vectors)
            # return all_vectors
        # self.trained_model = self.simpleCNN()
            
        # self.trained_model = torch.load(trained_model)
        # self.cnn_data["p_content"] = [str(each) for each in self.cnn_data["p_content"]]
        # self.cnn_sent = []
        # self.t1 = time.time()
        # for i,each in enumerate(self.cnn_data["p_content"]):
            # self.x = get_vectors(each)
            # self.X = torch.from_numpy(self.x.T)[:,None,:]
            # self.pred = self.trained_model.forward(self.X)
            # self.cnn_sent.append(self.pred.item())
        # print("TT "+str(int(time.time()-self.t1)))
        # #self.cnn_data["cnn_sentiment_score"] = self.cnn_sent
            
        # self.data["cnn_sentiment_score"] = self.cnn_sent
                
        
    # def loadGloveModel(self,gloveFile):
        # import numpy as np
        # print ("Loading Glove Model")
        # self.f = open(gloveFile,'r',encoding="utf8")
        # self.glv = {}
        # for line in self.f:
            # self.splitLine = line.split()
            # self.word = self.splitLine[0]
            # self.embedding = np.array([float(val) for val in self.splitLine[1:]])
            # self.glv[self.word] = self.embedding
        # print ("Done.",len(self.glv)," words loaded!")
        # return self.glv
        
    def apply_config(self,config):
        self.c = Newfile(config)
        self.c = self.c.data
        self.change_list = {each:self.c.a[i] for i,each in enumerate(self.c.b) if self.c.a[i] != "nil" }
        self.change_list = {each:self.change_list[each] for each in self.change_list if each in self.data.columns}
        # print(self.change_list)
        self.data = self.data.rename(columns = self.change_list)
        # print(list(self.data))
        self.nil_list = [ self.c.a[i] for i,each in enumerate(self.c.b) if each == "nil"]
        if "AuthorName" in self.nil_list:
            self.data["AuthorName"] = "noname"
        if "DocumentsKey" in self.nil_list:
            self.data["DocumentsKey"] = [ each for each in range(0,self.data.shape[0])]
        if "SourceURL" in self.nil_list:
            self.data["SourceURL"] = "unavailable"
        if "PublishedDatesKey" in self.nil_list:
            self.data["PublishedDatesKey"] = "20180101"
        if "ChannelName" in self.nil_list:
            self.data["ChannelName"] = "Twitter"
        self.doc_col = "DocumentsKey"
        self.date_col = "PublishedDatesKey"
        # self.date_col = ""
    
        