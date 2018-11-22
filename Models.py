# def create_model_instance():
from Package import Newfile as nf
import torch
from torch import nn
from torch.nn.modules.padding import ConstantPad1d,ReflectionPad1d
from torch.nn.utils.rnn import pack_padded_sequence, pad_packed_sequence
import time
import pandas as pd
from sklearn.model_selection import train_test_split
import time
import numpy as np
from Package import Newfile as nf
import pandas as pd
import numpy as np

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
    
    
def get_vectors(text,glv,pad = 250):
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
    return all_vectors
    
    
class Flatten(nn.Module):
    def forward(self, x):
        return x.contiguous().view(x.numel())    
class simpleCNN(torch.nn.Module):
    def __init__(self):
        super(simpleCNN,self).__init__()
        self.conv1 = nn.Sequential(),  
        self.flat = Flatten()
        self.fc = nn.Sequential()
        self.conv1 = self.conv1.double()
        self.fc = self.fc.double()
            
    def forward(self,x):
        cf = self.conv1(x)
        c = self.flat.forward(cf)
        ans = self.fc(c)
        return ans

# def hel():
    # print(__name__)
    # if __name__ == "__main__":
        # print("heleleleeleelelelele")
    
if __name__ == "__main__":
    hel

def main():
    a = 3
    x = __name__
    return x
def hel():
    v = main()
    return v
    
    
def main2():
    print(__name__)
    if __name__ == "__main__":       
        print("HUE")
    
def main1():
    print(__name__)
    if __name__ == "__main__":
        print("PUE")
        
        
    


def cnn_sent(text_list,trained_model,glv_name = r"D:/SENTIMENT CLASSIFIER/glove.6B.50d.txt",loaded_model = 0):
    from nltk.tokenize import TweetTokenizer
    from Package import Newfile as nf
    import torch
    from torch import nn
    from torch.nn.modules.padding import ConstantPad1d,ReflectionPad1d
    from torch.nn.utils.rnn import pack_padded_sequence, pad_packed_sequence
    import time
    import pandas as pd
    from sklearn.model_selection import train_test_split
    import time
    import numpy as np
    from Package import Newfile as nf
    import pandas as pd
    import numpy as np
    # create_model_instance()
    
    class simpleCNN(torch.nn.Module):
        def __init__(self):
            super(simpleCNN,self).__init__()
            self.conv1 = nn.Sequential(),  
            self.flat = Flatten()
            self.fc = nn.Sequential()
            self.conv1 = self.conv1.double()
            self.fc = self.fc.double()
                
        def forward(self,x):
            cf = self.conv1(x)
            c = self.flat.forward(cf)
            ans = self.fc(c)
            return ans
    
    glv = load_glove_model(glv_name)
    print("Under Models -- ")
    print(__name__)
    # print(__main__)
    if loaded_model == 0:
        trained_model = torch.load(trained_model, map_location='cpu')
    else:
        trained_model = loaded_model
    cnn_sent =[]
    t1 = time.time()
    cnn_sent = [  trained_model.forward(torch.from_numpy((get_vectors(str(each),glv)).T)[:,None,:]).item()  for i,each in enumerate(text_list)]
    print("TT "+str(int(time.time()-t1)))
    
    def cnn_sentiment(score):
        if score <=0.2:
            return "Very Negative"           
        if score >0.2 and score <= 0.4:
            return "Somewhat Negative"        
        if score >0.4 and score<=0.65:
            return "Neutral"        
        if score >0.65 and score<=0.8:
            return "Somewhat Positive"
        if score >0.8:
            return "Very Positive"
            
    cnn_sent = [ cnn_sentiment(each) for each in cnn_sent]
    
    return cnn_sent
    # evals.data["cnn_sentiment_score"] = cnn_sent

    # print(evals.data.cnn_sentiment_score)
    # evals.data.to_csv("checking.csv",encoding = "utf-8")