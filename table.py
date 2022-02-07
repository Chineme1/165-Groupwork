from msilib.schema import tables
from lstore.index import Index
from lstore.page import Page
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
        self.num_colums = num_columns
        self.base_pages = []
        self.count_base_pages = 0
        self.pr_key = pr_key
        self.key = key
    
    def read(self,num_page):
        return self.base_pages[num_page]

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
        self.num_record = 0 
        self.page_ranges = []
        self.index = Index(self)
        pass
    
    """
    This function creat page ranges, and return the pr_key of the created page range

    """
    def creat_page_range(self):

        pr_index = len(self.page_ranges_num)
        new_page_range = PageRange(self.num_columns, pr_index, self.key)
        self.page_ranges.append(new_page_range)        
        self.page_ranges_num += 1
        return pr_index

    def page_directory(RID):

        num_page_range = RID//8192
        num_page = (RID%8192)//512
        return num_page_range, num_page

    def read(self,RID,column):
        num_page_range, num_page = self.table.page_directory(RID)
        return Page().read(PageRange().read(self.page_ranges[num_page_range],num_page),column*8:column*8+7))
    
    def write(self,value):

        
    
    

    def __merge(self):
        print("merge is happening")
        pass
 

