from lstore.index import Index
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
    :param table_key: int       #Name of the table which the page range belongs
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Key of page range // I'm not too sure, maybe just using key as identification of page range
    """

    def __init__(self,num_columns,table_key,pr_key):
        self.num_colums = num_columns
        self.table_key = table_key
        self.key = pr_key


class Table:

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param unsigned_columns     #Number of columns that need to create page range to store
    :param page_ranges_num      #Number of the created page ranges
    :param page_ranges:         #List of page ranges
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.unsigned_columns = num_columns
        self.page_directory = {}
        self.page_ranges_num = 0
        self.page_ranges = []
        self.index = Index(self)
        pass
    
    """
    This function creat page ranges, and return the pr_key of the created page range

    """
    def creat_page_range(self):

        if self.unsigned_columns > 16:
            num_columns = 16
            self.unsigned_columns -= 16
        else 
            num_columns = self.unsigned_columns
            self.unsigned_columns = 0

        pr_index = len(self.page_ranges_num)
        new_page_range = PageRange( num_columns, self.key, pr_index)
        self.page_ranges.append(new_page_range)        
        self.page_ranges_num += 1
        return pr_index

    def has_capacity(self): 
        if (self.num_columns%self.page_ranges_num <= 15):
            return(True)
        return(False)

    def __merge(self):
        print("merge is happening")
        pass
 
