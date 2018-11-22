
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

trained_model = torch.load(trained_model, map_location='cpu')