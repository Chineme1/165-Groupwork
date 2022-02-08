from msilib.schema import tables
from lstore.index import Index
from lstore.page import BP
from time import time

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3
KEY_COLUMN = 4


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
        self.num_colums = num_columns
        self.base_pages = [None]* 16
        self.tail_pages = [None]
        self.count_base_pages = 0
        self.count_tail_pages = 0 
        self.num_base_record = 0
        self.num_tail_record = 0
        self.pr_key = pr_key
        self.key = key
    
    def read(self,position,column):

        base_page = position // 512                 #index of base page
        base_page_position = position % 512         #position inside base page we are reading
        
        return(self.base_pages[base_page].read(base_page_position,column))

    def write(self,value,column):
        base_page = self.num_base_record // 512
        base_page_position = self.num_base_record % 512
        if base_page_position == 0:
            self.num_base_record += 1
            self.count_base_pages += 1
            x0 = BP()
            self.base_pages[base_page] = x0
            x0.write(value,column)
        else:
            self.num_base_record += 1
            self.base_pages[base_page].write(value,column)

    def tail_read(self,position,column):
        tail_page = position // 512
        tail_page_location = position % 512
        true_false, encoding = self.tail_pages[tail_page].read(tail_page_location,column)
        return encoding

    def tail_write(self,value,column)
        tail_page = self.num_tail_record // 512
        tail_page_position = self.num_tail_record % 512
        if tail_page_position == 0:
            self.num_tail_record += 1
            self.count_tail_pages += 1
            x0 = self.creat_tail_page()
            self.tail_pages[tail_page] = x0
            x0.write(value,column)
        else:
            self.num_tail_record += 1
            self.tail_pages[tail_page].write(value,column)

    def creat_tail_page(self):
        self.count_tail_pages += 1
        tail_page_index = self.count_tail_pages
        new_tail_page = BP()
        self.tail_pages.append(new_tail_page)
        return tail_page_index

    def has_capacity(self): 
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
        self.page_directory = {} #total data in page range
        self.page_ranges_num = 0
        self.num_base_record = 0 
        self.num_tail_record = 0
        self.page_ranges = []
        self.index = Index(self)
        pass
    
    """
    This function creat page ranges, and return the pr_key of the created page range

    """
    def creat_page_range(self):

        self.page_ranges_num += 1
        pr_index = self.page_ranges_num
        new_page_range = PageRange(self.num_columns, pr_index, self.key)
        self.page_ranges.append(new_page_range)        
        return pr_index

    def page_directory(RID):

        num_page_range = RID//8192              #index of page range
        num_base_page = (RID%8192)//512         #index of base page
        
        return num_page_range, num_base_page

    def read(self,RID,column):
        num_page_range, num_base_page = self.page_directory(RID)
        return self.page_ranges[num_page_range].read(num_base_page,column)
    
    def write(self,value,column):
        page_range = self.num_base_record // 8192
        page_range_position = self.num_base_record % 8192
        if page_range_position == 0:
            self.num_base_record += 1
            self.page_ranges_num += 1
            x0 = PageRange()
            self.page_ranges[page_range] = x0
            x0.write(value,column)
        else:
            self.num_base_record += 1
            self.page_ranges[page_range].write(value,column)

        

    def __merge(self):
        print("merge is happening")
        pass
