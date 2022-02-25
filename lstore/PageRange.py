from .basepage import BasePage
from time import process_time


class PageRange:
    
    # """
        # :param num_columns: int             #Number of Columns: all columns are integer
        # :param pr_key: int                  #identification of page range,like 0 means the first page range
        # :param key: int                     #Index of page range key in columns, indicating which column store the key column
        # """
    
    def __init__(self,num_columns,pr_key,key):
        self.num_columns = num_columns
        self.base_pages = [None]* 16
        self.tail_pages = []
        self.count_base_pages = 0 #This is the counter for base pages
        self.count_tail_pages = 0
        self.num_base_record = 0
        self.num_tail_record = 0
        self.pr_key = pr_key
        self.key = key

    def BaseRead(self,position,column): #This is Base READ *...*
        base_page = (position-1) // 512                 #index of base page
        base_page_position = ((position-1) % 512)+1        #position inside base page we are reading
        ret = self.base_pages[base_page].read(base_page_position,column)
        if ret[0] == False:
            return(self.TailRead(ret[1], column))
        else:
            return(ret[1])
    
    def BaseWrite(self,columns,position):#value we insert
    #Columns is the array of all values in a record
    #Position Use??
        if position == None:
            base_page = (self.num_base_record) // 512
            base_page_position = self.num_base_record % 512
            if base_page_position == 0:
                x0 = BasePage(self.num_columns)#Does it need the column and/or positon
                self.base_pages[self.count_base_pages]  = x0 # Might be self.count_base_pages + 1 or just
                self.count_base_pages += 1
                self.num_base_record += 1
                return x0.write(columns, base_page_position+1)
            else:
                self.num_base_record += 1
                return self.base_pages[self.count_base_pages-1].write(columns,base_page_position+1) #To be tested#TBT
        else:#Write to the position specified
            base_page = (position-1)// 512
            base_page_position = ((position-1) % 512 +1)
            #self.num_base_record += 1 #Not needed ??
            return self.base_pages[base_page].write(columns, base_page_position)

    
    #def write2(self,value,column,position):
    #   base_page = (position//512)
    # position2 = position%512
    # self.base_pages[base_page].write2(value, column, position2)
# return(True)

    def TailRead(self,position,column):
        tail_page = (position-1) // 512                 #index of base page
        tail_page_position = ((position-1) % 512 +1)         #position inside base page we are reading
        ret = self.tail_pages[tail_page].read(tail_page_position,column)
        if ret[0] == False:
                self.TailRead(ret[1], column)
        else:
            return(ret[1])

    def TailWrite(self,columns,position):
        tail_page = self.num_tail_record // 512
        tail_page_position = self.num_tail_record % 512
        if position == None:
            tail_page = self.num_tail_record // 512
            tail_page_position = self.num_tail_record % 512
            if tail_page_position == 0:
                x0 = BasePage(self.num_columns)#Does it need the column and/or positon
                self.tail_pages.append(x0) # Might be self.count_base_pages + 1 or just
                self.count_tail_pages += 1
                self.num_tail_record += 1
                return x0.write(columns, tail_page_position+1)
            else:
                self.num_tail_record += 1
                return self.tail_pages[self.count_tail_pages-1].write(columns,tail_page_position+1) #To be tested#TBT
        else:#Write to the position specified
            tail_page = (position-1)// 512
            tail_page_position = ((position-1) % 512 +1)
            #self.num_base_record += 1 #Not needed ??
            return self.tail_pages[tail_page].write(columns, tail_page_position)

    def has_capacity(self):
    #Append to the list in the Page range
    #CShould check the 16th page range
        if self.count_base_pages == 16:
            if self.base_pages[15].has_capacity: #This is fine \ - _ - /  ???
                return True
            else:
                return False
        if (self.count_base_pages < 16):
                return(True)
        return(False)
           
           
    def Delete(self,position):
           array = [None]*self.num_columns
           array[1] = 0
           self.BaseWrite(array,position)
           indirection = self.BaseRead(position,0)
           while indirection != 0:#Might lead to an infinte bug #CHECK!!!!!!!!!!!!!!!
                self.TailWrite(array,indirection)
                indirection = self.TailRead(indirection,0)
     
    def Update(self, position, columns, BA):#BA = bit array of updated columns
           #AB = BA #Converting the Bit array
           num = 0
           CurrSchema = self.BaseRead(position,3)
           CurrSchema = [CurrSchema >> i & 1 for i in range(self.num_columns-1,-1,-1)]
           for i in range(0, self.num_columns):#Doesn't need 4 potentially
                if BA[i] == 1 or CurrSchema[i] == 1:
                    num += pow(2,self.num_columns -i -1)
           array = [None]*self.num_columns
           array[3] = num
           self.BaseWrite(array,position)
           metadata = [None]*4
           metadata[0] = self.BaseRead(position,0)
           metadata[1] = self.num_tail_record+1
           metadata[2] = int(process_time())      #To be fixed/Checked
           metadata[3] = 0 #Nott being used noW
           data= list(columns)         
           for i in range(4, self.num_columns):
                if data[i-4] == None:
                    data[i-4] = self.BaseRead(position, i)
           data  = metadata + data         
           array[3] = None
           array[0] = self.num_tail_record+1
           #print("array",array)
           self.BaseWrite(array, position)#To be checked
           self.TailWrite(data, None)
           
           
    def merge(self):
        new = PageRange(self.num_columns, self.pr_key, self.key)
        for i in range(1, self.num_base_record+1):
            columns = []
            for j in range(0, self.num_columns):
                columns.append(self.BaseRead(i, j))
            new.BaseWrite(columns, None)
        return(new)
           
# x0 = PageRange(5, 0, 0)
# for i in range(1, 10):
#     arr = [0, i, 0, 0, i]
#     print(x0.BaseWrite(arr, None))
# print("break")
# for i in range(1, 10):
#     print(x0.BaseRead(i, 4))
# x0.Update(5, [1], [0, 0, 0, 0, 1])
# print(x0.BaseRead(5, 4))
# x0.Update(5, [3], [0, 0, 0, 0, 1])
# print(x0.BaseRead(5, 4))

# print(x0.BaseRead(5, 1))
# print(x0.TailRead(1, 1))
# print(x0.TailRead(2, 1))
# x0.Delete(5)
# print(x0.BaseRead(5, 1))
# print(x0.TailRead(1, 1))
# print(x0.TailRead(2, 1))