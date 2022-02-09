

class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(4096)

    def has_capacity(self):
        if self.num_records == 512 : #4096 bytes divided by the 8 bytes == 512
            return False
        else:
            return True
    def Thas_capacity(self,column): #This is for the Tail Page
        if self.num_records > 512 - column:
            return False
        else:
            return True
    def read(self, position):
    #I don't need to error check
        arr =  self.data[position*8 : position*8 +7]
        num = 0
        num.from_bytes(arr, 'big')
        return(num)

    def write(self, value):
        if self.has_capacity():
            if value == None:
                count = 0
                zero = 0
                arr = zero.to_bytes(8, 'big')
                self.data.extend(arr)
                return(True)
            arr = value.to_bytes(8, 'big')
            self.data.extend(arr) # change due to verying size
            self.num_records += 1
            return(True)
        else:
            return False
    def write2(self, value, position):
        if value == None:
            count = 0
            while count < 8:
                self.data[position*8+count] = 0
                count += 1
             #self.data[position*8 : position*8+7] = 0 #come back to for changing RID to 0/NULL
            return(True)
        arr = value.to_bytes(8, 'big')
        self.data[position*8 : position*8+7] = arr
        return(True)

    

class BP:
    def __init__(self,columns):
        self.columns = columns
        self.hold = []#Physiical holder
        self.counter = 0
        self.updates = 0
        for i in range(0, self.columns+4):
            x0 = Page()
            self.hold.append(x0)
        
    
        #def I_PP(self):
        # if self.has_capacity:
    


    #def has_capacity(self):
    #   if self.columns == columns

    def write(self,value, column):
        page = column
        position = self.counter%512 #Not needed for now
        if self.updates%self.columns == 0:
            self.counter += 1
        self.updates +=1
        self.hold[page].write(value)
        return(self.counter)
    def write2(self, value, column, position):
        page = column
        position2 = position%512
        self.hold[page].write2(value, position2)
        return (True)
            

    def read(self, position, column):
        page = ((position//512)-1)*self.columns+column
        schemaPage = ((position//512)-1)*self.columns+4
        indirectionPage = ((position//512)-1)*self.columns+1
        position2 = position%512
        bit = self.hold[schemaPage].read(position2)%pow(2, 7-column)
        bit = bit//pow(2, 8-column) 
        if bit == 1: 
            encoding = self.hold[indirectionPage].read(position2)
            return(False, encoding)
        return(True, self.hold[page].read(position2))#Potentially wrong


