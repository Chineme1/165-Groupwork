from asyncio.windows_events import NULL
from msilib.schema import tables
from lstore.index import Index
from lstore.PageRange import PageRange
from lstore.basepage import BasePage
from time import time

# define the metadata columns
INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3


class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns


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
        self.num_columns = num_columns   # number of columns besides the metadata columns
        self.key = key
        self.page_ranges_num = 0    # number of page range in the table
        self.num_base_record = 0    # number of base record in total
        self.num_tail_record = 0    # number of tail record in total
        self.page_ranges = []   # list of all page ranges
        self.index = Index(self)


    """
    # Returns corresponding location of the given RID
    """
    def page_directory(self, RID):
        position_page_range = (RID-1)//8192              #index of page range
        position_base_page = ((RID-1)%8192)+1        #index of base page
        return position_page_range, position_base_page


    """
    # Read the data with given RID and specified column index, return a single value in the record
    # :param RID: RID of the desired record in base page
    # :param column: index of the desired column
    """
    def readValue(self,baseRID,column):
        position_page_range, position_base_page = self.page_directory(baseRID)  # find the page range the given record is at
        return self.page_ranges[position_page_range].BaseRead(position_base_page,column)
    
    """
    # Read the entire record with given RID
    # :param RID: RID of the desired record in base page
    """
    def readRecord(self, baseRID):
        record = []
        for i in range(self.num_columns+4):  # read value in every column
            record.append(self.readValue(baseRID,i))
        return record

    """
    # Append a new record to the base page if RID not given, otherwise edit the existing record
    # :param columns: a list of new values corresponding to each column
    # :param RID: RID of the record to be updated
    """
    def write(self,columns,position):
        if position == None:#Write to the end
            page_range = self.num_base_record // 8192
            page_range_position = self.num_base_record % 8192
            if page_range_position == 0:    # when page range is full
                self.page_ranges_num += 1
                x0 = PageRange(self.num_columns+4, self.page_ranges_num, 0)  # create a new page range
                self.page_ranges.append(x0)
                self.num_base_record += 1
                return x0.BaseWrite(columns,None)
            else:
                self.num_base_record += 1
                return self.page_ranges[page_range].BaseWrite(columns,None)
        else: #Write to the position
            page_range = position // 8192
            page_range_position = position % 8192
            return self.page_ranges[page_range].BaseWrite(columns,page_range_position)


    """
    # Read the tail page data with given RID and specified column index, return a single value in the record
    # :param RID: RID of the desired tail record
    # :param column: index of the desired column
    """
    def tail_read(self,tailRID,column):
        position_page_range, position_base_page = self.page_directory(tailRID)  # TODO: page directory may not work for tail page
        return self.page_ranges[position_page_range].TailRead(tailRID,column)


    """
    # Update the base record
    # :param baseRID: RID of the base record to update
    # :param columns: array of updated columns
    # :param BA: bit array of columns being updated
    """
    def update(self,baseRID,columns,BA):
        page_range, page_range_position = self.page_directory(baseRID)
        self.page_ranges[page_range].Update(page_range_position,columns,BA)
        if self.num_tail_record%(self.num_base_record*0.2) == 0:
        # merge the tail records with the base record when the number of tail records = 20% of the base records
            self.__merge()
        return(True)


    """
    # Delete a record
    # :param RID: RID of the record to be deleted
    """
    def delete(self,RID):
        position_page_range, position_base_page = self.page_directory(RID)   # find the location of the given record
        self.page_ranges[position_page_range].Delete(position_base_page)
        return(True)
        

    """
    # Merge tail records with corresponding base records
    """
    def __merge(self):
        # load a copy of all base pages of the selected range into memory
        # iterate the tail records in reverse order and apply them to the copied base pages
        # to find the corresponding base record for tail record, need a BaseRID column to track (may use schema encoding?)
        # TPS
        pass
 

