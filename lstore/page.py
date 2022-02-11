


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
        arr =  self.data[position*8 : position*8 + 8]
        num = 0
        num = int.from_bytes(arr, 'big')
        return(num)

    def write(self, value):
        if self.has_capacity():
            if value == None:
                count = 0
                zero = 0
                arr = zero.to_bytes(8, 'big')
                self.data[self.num_records*8 : self.num_records*8+8] = arr 
                self.num_records += 1
                return(True)
            arr = value.to_bytes(8, 'big')
            self.data[self.num_records*8 : self.num_records*8+8] = arr #check for signed ints
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
        self.data[position*8 : position*8+8] = arr
        return(True)
        
    def pagePrint(self):
        print(self.data)

    

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
        position = self.counter%512 
        ret = self.counter
        if self.updates%(self.columns+4) == self.columns+4:
            self.counter += 1
        self.updates +=1
        self.hold[page].write(value)
        return(ret)
    def write2(self, value, column, position):
        page = column
        self.hold[page].write2(value, position)
        return (True)
            

    def read(self, position, column):
        page = column
        schemaPage = 3
        indirectionPage = 0
        bit = self.hold[schemaPage].read(position)
        bit = bit%pow(2, 9-column)
        bit = bit//pow(2, 8-column) 
        if bit == 1: 
            indirection = self.hold[indirectionPage].read(position)
            #print("the indirection col = ", indirection)
            return(False, indirection)
        return(True, self.hold[page].read(position))


# x0 = BP(5)
# for i in range(0, 5):
    # x0.write(4294967295, i)
    # x0.write(None, i)
    # x0.write(4294967293, i)
    # x0.write(4294967292, i)

# print(x0.read(0, 0))
# print(x0.read(1, 0))
# print(x0.read(2, 0))
# print(x0.read(3, 0))

# x0.write2(None, 0, 0)
# print(x0.read(0, 0))