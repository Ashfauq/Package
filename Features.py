import pandas as pd
import time
import re
import os
import warnings
import pickle
warnings.filterwarnings("ignore")


def concat_folder(target_loc = 0,files = 0):
    old_dir = os.getcwd()
    if files ==0:
        target_loc = target_loc
        files = os.listdir(target_loc)    
    
    if target_loc == 0:
        op = pd.read_csv(files[0],encoding='iso-8859-1',error_bad_lines=False,index_col=False)
    else:
        os.chdir(target_loc)
        op =  pd.read_csv(files[0],encoding='iso-8859-1',error_bad_lines=False,index_col=False)
        os.chdir(old_dir)
    for i,each in enumerate(files):
        print(each)
        if i !=0:
            if target_loc==0:
                ip = pd.read_csv(each,encoding='iso-8859-1',error_bad_lines=False,index_col=False)
            else:
                os.chdir(target_loc)
                ip = pd.read_csv(each,encoding='iso-8859-1',error_bad_lines=False,index_col=False)
                os.chdir(old_dir)
            op = pd.concat([op,ip])
            print(op.shape)
    op = op.reset_index(drop = True)
    os.chdir(old_dir)
    return op
    
def save(var,name):
    with open(name,"wb") as f:
        pickle.dump(var,f)
        
def load(name):
    with open(name,"rb") as f:
        loaded = pickle.load(f)
        
    return loaded

def error_handle(dir = 0,files = []):
    for each in files:
        columns = pd.read_csv(str(each),encoding='iso-8859-1',nrows= 1).columns
        columns = columns.tolist()
        df = pd.read_csv(str(each), encoding='iso-8859-1',usecols=columns)
        kw1 = df.ix[:,:15]
        kw2 = df.ix[:,26:]
        i = 0
        kw2_headers = list(kw2)
        sent = ["Very Positive","Somewhat Positive" ,"Very Negative","Somewhat Negative","Neutral" ]
        kw1["sentiment"] = ''
        
        kw1["sentiment"] = [list((kw2.iloc[row,col]) for col,e in enumerate(kw2_headers) if kw2.iloc[row,col] in sent) for row in range(0,kw2.shape[0])]
        
        kw1["sentiment"] =[ each[0] if each!=[] else "NA" for each in kw1["sentiment"]  ]
        
        na = [each for each in kw1.sentiment if each == "NA"]
        
        print("Number of rows with no sentiment = "+str(len(na)))
        
        kw1.to_csv("Error handled "+each,index=None)
        
def theme_multi(loc,key,uncovered=True,content_col = "Content"):
    import time
    import os
    t1 = time.time()
    files = os.listdir(loc)
    theme_data = pd.read_csv(key,encoding='iso-8859-1',error_bad_lines=False,index_col=False)
    master = { each:pd.DataFrame() for each in theme_data["Theme_Name"]}
    if os.path.isdir("theme//") == False:
        os.makedirs("theme//")
    
    for file in files:
        print("Processing - "+str(file))
        temp = pd.read_csv(loc+"\\"+file,encoding='iso-8859-1',error_bad_lines=False,index_col=False)
        for each in range(len(theme_data)):
            op = pd.DataFrame()
            r = str(theme_data['Keywords'][each])
            t = str(theme_data['Theme_Name'][each])
            op = temp[(temp[content_col].str.lower()).str.contains(r,flags=re.IGNORECASE, regex=True, na=False) == True]
            # print(master[t].shape)
            if master[t].shape[0] == 0:
                master[t] = op
            else:
                master[t] = pd.concat([master[t],op])
    for each in master:
        print(str(each) +" = " + str(master[each].shape[0]))
        master[each].to_csv(r'theme//'+each + ".csv",index=None)
    t2 = time.time()
    print("Time Taken :" + str(t2-t1))
    return master
        
def spark_theme_multi(loc,key,uncovered=True,content_col = "Content"):
    import os
    import time
    t1 = time.time()
    files = os.listdir(loc)
    theme_data = pd.read_csv(key,encoding='iso-8859-1',error_bad_lines=False,index_col=False)
    try:
        theme_data[content_col] = [ each.encode("ASCII","ignore").decode("utf-8") for each in theme_data[content_col]]   
    except:
        pass
    master = { each:pd.DataFrame() for each in theme_data["Theme_Name"]}
    if os.path.isdir("theme//") == False:
        os.makedirs("theme//")
        
    import sys
    spark_home = os.environ.get('SPARK_HOME', None)
    import pyspark
    from pyspark import SparkContext, SparkConf
    from pyspark.sql import SparkSession
    conf = SparkConf()
    sc = SparkContext(conf=conf)
    spark = SparkSession.builder.config(conf=conf).getOrCreate()        

    
    for file in files:
        print("Processing - "+str(file))
        temp = spark.read.format("csv").option("inferSchema","true").option("header","true").load(loc+"\\"+file)
        for each in range(len(theme_data)):
            op = pd.DataFrame()
            r = '(?i)'+ str(theme_data['Keywords'][each])
            t = str(theme_data['Theme_Name'][each])
            op = temp.where(temp[content_col].rlike(r)).toPandas()
            # print(master[t].shape)
            if master[t].shape[0] == 0:
                master[t] = op
            else:
                master[t] = pd.concat([master[t],op])
    for each in master:
        print(str(each) +" = " + str(master[each].shape[0]))
        master[each].to_csv(r'theme//'+each + ".csv",index=None)    
    sc.stop()
    t2 = time.time()
    print("Time Taken :" + str(t2-t1))
    return master
    
def load_glove_model(gloveFile):
    import numpy as np
    print ("Loading Glove Model")
    f = open(gloveFile,'r',encoding="utf8")
    model = {}
    for line in f:
        splitLine = line.split()
        word = splitLine[0]
        embedding = np.array([float(val) for val in splitLine[1:]])
        model[word] = embedding
    print ("Done.",len(model)," words loaded!")
    return model
    
    
def get_vectors(text,glv,pad = 250,get_pos = False):
    import nltk
    import numpy as np
    all_vectors = [glv[each] for each in text.split(" ") if each in glv]
    temp = np.zeros((50))
    
    if pad != None:
        all_vectors = all_vectors[:pad]
        if len(all_vectors) <pad:
            temp_range = pad-len(all_vectors)
            temp_list = temp.tolist()
            [all_vectors.append(temp_list)for each in range(temp_range)]
    if len(all_vectors)== 0:
        all_vectors.append(temp)
    
    all_vectors = np.array(all_vectors)
    
    if get_pos == True:
        pos_vector = get_pos_tag(text,glv)
        if len(all_vectors)!= len(pos_vector):
            print("Pos Error detected")
        all_vectors = [np.concatenate((each,pos_vector[1])) for i,each in enumerate(all_vectors) if len(all_vectors)== len(pos_vector)]
        
    
    return all_vectors
    
def get_pos_tag(text,glv):
    import nltk
    import numpy as np
    
    all_vectors = [glv[each] for each in text.split(" ") if each in glv]
    all_words = [each for each in text.split(" ") if each in glv]
    pos_tag = nltk.pos_tag(all_words)
    tags = ['CC','CD','DT','EX','FW','IN','JJ','LS','MD','NN','RB','RP','$','TO','UH','WP','VB','VBD','VBG','VBN','VBP','VBZ','WDT','WP$','WRB','JJR','JJS','PDT' ,'POS' ,'PRP' ,'RBR' ,'RBS' ,'NNS' ,'NNP' ,'NNPS','PRP$']
    # print(text)
    # print("==")
    tag_map = [ [i for i,e in enumerate(tags) if each[1] == e] for each in pos_tag]
    # len(all_vectors)
    # len(pos_tag)
    # len(tag_map)
    pos_vector = []
    for each in tag_map:
        temp = np.zeros(len(tags))
        try:
            temp[each[0]] = 1
        except:
            pass
        pos_vector.append(temp)
        
    return pos_vector


    
    