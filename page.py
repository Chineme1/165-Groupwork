from lstore.table import Table

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
        return self.num_records[position*8 : position*8 +7]

    def write(self, value):
        if self.has_capacity:
            self.num_records[self.num_records*8 : self.num_records*8+7] = value
            self.num_records += 1
        else:
            return False
    def write2(self, value, position):
        self.num_records[position*8 : position*8+7] = value
        return(True)

class PageRange:
    def __init__(self,columns):
        self.columns = columns
        self.TP = 1
        self.BP = 0
        #TP1 = TP(columns) #necessary?
        #BP1 = BP(columns) #necessary?
        self.hold = [None*16]
    
    def I_TP(self,value):
        x1 = Page()
        self.hold.append(x1) #change to insert
        self.TP +=1

    def I_BP(self):
        if self.has_capacity:
            np= Page()
            self.hold[self.BP]=np
            self.BP +=1
        else:
            return False

    def has_capacity(self):
        if self.BP == 16:
            return False
        else:
            return True
    def read(self,position,column):
        page = position//8192
        position2 = position%8192
        return(self.hold[page].read(position2))
    

class BP:
    def __init__(self,columns):
        self.columns = columns
        self.hold = [None* columns] #Physiical holder
        self.counter = 0
    
    
        #def I_PP(self):
        # if self.has_capacity:
    


    #def has_capacity(self):
    #   if self.columns == columns

    def write(self,value, column):
        page = (self.counter//512)*self.columns+column
        position = self.counter%512 #Not needed for now
        if position == 0:
            self.counter +=1
            x0 = Page()
            self.hold[page] = x0
            x0.write(value)
        else:
            self.counter +=1
            self.hold[page].write(value)
    def write2(self, value, column, position):
        page = (self.counter//512)*position+column
        position2 = position%512
        self.hold[page].write2(value, position2)
        return (True)
            

    def read(self, position, column):
        page = ((position//512)-1)*self.columns+column
        schemaPage = ((position//512)-1)*self.columns+page
        position2 = position%512
        if self.hold[schemaPage].read(position2)[column] == 1:
            return(False)
        return(self.hold[page].read(position2))#Potentially wrong


