from asyncio.windows_events import NULL
from msilib.schema import tables
from lstore.index import Index
from lstore.page import BP
from time import time

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3

class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

class PageRange:

    """
    :param num_columns: int             #Number of Columns: all columns are integer
    :param pr_key: int                  #identification of page range,like 0 means the first page range
    :param key: int                     #Index of page range key in columns, indicating which column store the key column
    """

    def __init__(self,num_columns,pr_key,key):
        self.num_columns = num_columns
        self.base_pages = [None]* 16
        self.tail_pages = []
        self.count_base_pages = 0
        self.count_tail_pages = 0 
        self.num_base_record = 0
        self.num_tail_record = 0
        self.base_updates = 0 # make sure we are not increasing num_base_record each time we update a column
        self.tail_update = 0  # same as above but for tail record
        self.pr_key = pr_key
        self.key = key
    
    """
    # read the column of specified position(RID) in base page
    # :param position:
    # :param column:  
    """
    def read(self,position,column):
        base_page = position // 512                 #index of base page
        base_page_position = position % 512         #position inside base page we are reading
        boolean, value = self.base_pages[base_page].read(base_page_position,column)
        if boolean == True:
            return(True, value)
        else:
            return(self.tail_read(value, column))


    """
    # write value to the specified column of the last base page
    # :param value:
    # :param column:  
    """
    def write(self,value,column):
        base_page = self.num_base_record // 512
        base_page_position = self.num_base_record % 512
        if base_page_position == 0 and self.base_updates == 0:
            #if self.base_updates == self.num_columns:
            #    self.num_base_record += 1
            self.base_updates = 1
            #self.base_updates += 1
            self.count_base_pages += 1
            x0 = BP(self.num_columns)
            self.base_pages[base_page] = x0
            x0.write(value,column)
        else:
            if self.base_updates == self.num_columns+3:
                self.num_base_record += 1
                self.base_updates = 0
            else:
                self.base_updates += 1
            self.base_pages[base_page].write(value,column)
        return(True)

    """
    # Update the value of column in position of the base page
    # :param value:
    # :param position:  
    # :param column:  
    """
    def write2(self,value,column,position):
        base_page = (position//512)
        position2 = position%512
        self.base_pages[base_page].write2(value, column, position2)
        return(True)
    
    """
    # Read tail page of column of base page in position, return value of this column in tail page
    # :param position:  
    # :param column:  
    """
    def tail_read(self,position,column):
        #print("position = " , position)
        tail_page = position // 512
        tail_page_location = position % 512
        true_false, value = self.tail_pages[tail_page].read(tail_page_location,column)
        return (True, value)


    """
    # Write value of column of base page in position, return the rid of created tail page 
    # :param value: value to be appended to the tail page
    # :param column: specified column where the value belongs to
    # :param position: the RID of the base record being updated 
    """
    def tail_write(self,value,column,position):
        #print("num tail records = " , self.num_tail_record)
        #print("num tail pages = " , self.count_tail_pages)
        tail_page = self.num_tail_record // 512
        tail_page_position = self.num_tail_record % 512
        #print("tail page position = ", tail_page_position)
        #print("tail page update = ", self.tail_update)
        if tail_page_position == 0 and self.tail_update == 0:
            self.tail_update += 1
            x0 = BP(self.num_columns)
            self.tail_pages.append(x0)
            RID = self.tail_pages[self.count_tail_pages].write(value,column)
            self.count_tail_pages += 1
        else:
            if self.tail_update == self.num_columns + 3:
                self.num_tail_record += 1
                self.tail_update = 0
            else:
                self.tail_update += 1
            RID = self.tail_pages[tail_page].write(value,column)
        self.write2(RID, 0, position)
        return(RID)       # return the RID of the update in tail page


    """
    # Update the value in the tailpage with specified column and position
    # :param value: updated value
    # :param column: specified column to modify
    # :param position: the RID of the tail page record being updated 
    """
    def tail_write2(self,value,column,position):
        tail_page = (self.num_tail_record//512) * position + column
        position2 = position%512
        self.tail_pages[tail_page].write2(value, column, position2)
        return(True)


    """
    # delete a base record, if indirection column is not zero then delete the following tail pages
    # :param position:  
    """
    def delete(self,position):
        base_page = position // 512
        base_page_position = position % 512
        schema = self.base_pages[base_page].read(base_page_position,3)
        for i in range(4, self.num_columns+4): #should work?
            bit = schema[1]%pow(2, 8-i)
            bit = bit//pow(2, 7-i)
            check = 0
            if bit == 1:
                new_RID = self.base_pages[base_page].read(base_page_position,0)[0]
                check += 1
                break
            else:
                check += 0
        if check == 0:
            self.base_pages[base_page].write2(None,1,base_page_position)
            return(True)
        # set rid of tail pages to null
        else:
            tail_page = new_RID // 512
            tail_page_position = new_RID % 512
            while self.tail_pages[tail_page].read(tail_page_position,0)[0] != 0 :
                new_RID = self.tail_pages[tail_page].read(tail_page_position,0)[0]
                self.tail_pages[tail_page].write2(None,1,tail_page_position)
                tail_page = new_RID // 512
                tail_page_position = new_RID % 512
            self.tail_pages[tail_page].write2(None,1,tail_page_position)    
            return(True)

    # def create_tail_page(self):
        # tail_page_index = self.count_tail_pages
        # self.count_tail_pages += 1
        # new_tail_page = BP(self.num_columns+4)
        # self.tail_pages.append(new_tail_page)
        # return tail_page_index


    def has_capacity(self):   # check if there's enough capacity for adding new base page in the page range
        if (self.base_pages <= 15):
            return(True)
        return(False)


class Table:

    """
    :param name: string                 #Table name
    :param num_columns: int             #Number of Columns: all columns are integer
    :param page_ranges_num: int         #Number of the created page ranges
    :param page_ranges: PageRange       #List of page ranges
    :param key: int                     #Index of table key in columns
    """
    def __init__(self, name, num_columns, key):
        self.name = name
        self.num_columns = num_columns
        self.key = key
        self.page_ranges_num = 0
        self.num_base_record = 0 
        self.num_tail_record = 0
        self.num_table_record = 0 #what is this for
        self.base_update = 0 # make sure we are not increasing num_base_record each time we update a column
        self.tail_update = 0 # same as above
        self.page_ranges = []
        self.index = Index(self)
        pass
    
    """
    This function creat page ranges, and return the pr_key of the created page range

    """
    # def create_page_range(self): is this being used anywhere?
        # self.page_ranges_num += 1
        # pr_index = self.page_ranges_num
        # new_page_range = PageRange(self.num_columns, pr_index, self.key)
        # self.page_ranges.append(new_page_range)        
        # return pr_index

    def page_directory(self, RID):
        position_page_range = RID//8192              #index of page range
        position_base_page = (RID%8192)         #index of base page
        return position_page_range, position_base_page

    
    # read the column in rid in base page
    def read(self,RID,column):
        RID = RID-1 #due to rid = 0 being reserved but we still want to use position 0 in our pages
        position_page_range, position_base_page = self.page_directory(RID)
        return self.page_ranges[position_page_range].read(position_base_page,column)[1]
    
     # write value to column of the last base page
    def write(self,value,column):
        page_range = self.num_base_record // 8192
        page_range_position = self.num_base_record % 8192
        if page_range_position == 0 and self.base_update == 0:
            self.page_ranges_num += 1
            if self.base_update == self.num_columns + 3:
                self.num_base_record += 1
                self.base_update = 0
            else:
                self.base_update += 1
            x0 = PageRange(self.num_columns, self.page_ranges_num, 0)
            self.page_ranges.append(x0)
            x0.write(value,column)
        else:
            if self.base_update == self.num_columns + 3:
                self.num_base_record += 1
                self.base_update = 0
            else:
                self.base_update += 1
            self.page_ranges[page_range].write(value,column)
        return (True)

    def write2(self,value,column,position):
        page_range = (position//8192)
        position2 = position % 8192
        self.page_ranges[page_range].write2(value,column,position2)
        return(True)


    def tail_read(self,RID,column):
        RID = RID-1 #make sure to change is deleted RID is changed to -1
        position_page_range, position_base_page = self.page_directory(RID)
        return self.page_ranges[position_page_range].tail_read(position_base_page,column)

    def tail_write(self,value,column,RID):
        page_range, page_range_position = self.page_directory(RID)
        if self.tail_update == self.num_columns + 3:
                self.num_tail_record += 1
        self.tail_update += 1
        self.page_ranges[page_range].tail_write(value,column,page_range_position)
        return(True)


    def tail_write2(self,value,column,RID,position):
        page_range = (self.num_tail_record//8192) * position + column
        position2 = self.num_tail_record % 8192
        self.page_ranges[page_range].tail_write2(value,column,position2)
        return(True)


    def delete(self,RID):
        position_page_range, position_base_page = self.page_directory(RID)
        self.page_ranges[position_page_range].delete(position_base_page)
        return(True)
        
    def __merge(self):
        print("merge is happening")
        pass
 

