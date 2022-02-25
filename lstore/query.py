from .table import Table, Record
from .index import Index
from .page import Page
from .bufferpool import BufferPool
import time # for timestamp


class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """
    def __init__(self, table):
        self.table = table
        update = 0
        pass



    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """
    def delete(self, primary_key):
        if not self.table.page_directory(primary_key):
            return False
        output = []
        out = self.table.index.indices[0].find(primary_key, self.table.index.indices[0].root, output)
        RID = output[0]
        self.table.bufferpool.delete(RID)
        return(True)
            
        
        
    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """
    def insert(self, *columns):     #create metadata
        
        indirection = None
        rid = self.table.num_base_record +1
        ts = int(time.time())
        schema_encoding = 0
        data = [indirection, rid, ts, schema_encoding]
        #append the insert data into the metadata array
        for i in range(0, self.table.num_columns):
            if self.table.index.indices[i] != None:
                self.table.index.indices[i].insert(columns[i], rid, self.table.index.indices[i].root)
        for page in columns:
            data.append(page)
        self.table.bufferpool.writeValue(data, rid)
        self.table.num_base_record += 1
        return(True)



    """
    # Read a record with specified key
    # :param index_value: the value of index you want to search
    # :param index_column: the column number of index you want to search based on
    # :param query_columns: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select(self, index_value, index_column, query_columns):
        output = []
        self.table.index.indices[index_column].find(index_value, self.table.index.indices[index_column].root, output) # find the RID with the filter parameters
        RID = output[0]
        numCols = len(query_columns)
        arr = []
        for i in range (0, numCols):
            if query_columns[i] == 1:   # check which values in the query_columns are 1
                arr.append(self.table.bufferpool.readValue(RID, i+4))  # read the data in the desired columns and append it to the list
        ret = [Record(RID, index_value, arr)]
        return(ret)



    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
    def update(self, primary_key, *columns):
        # check if exists
        if not self.table.page_directory(primary_key):
            return False
        # get RID
        output = []
        #print("primary_key",primary_key)
        self.table.index.indices[0].find(primary_key, self.table.index.indices[0].root, output)

        RID = output[0]
        # update column
        ba_updated = [0] *(len(columns)+4)
        numCols = len(columns)
        for i in range(0, numCols):
            if(columns[i] == None):
                ba_updated[i+4] = 0
            else:
                ba_updated[i+4] = 1
        self.table.bufferpool.update(RID, columns, ba_updated)
        update += 1
        if update == 100:
            update = 0
            self.table.bufferpool.merge()
        return (True)



    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum(self, start_range, end_range, aggregate_column_index):
        # Get the range of records using B-Tree's findRange function
        output = []
        self.table.index.indices[0].findRange(start_range, end_range, self.table.index.indices[0].root, output)
        # Loop through the range and add all the read values
        num = 0
        for i in output:
            num += self.table.bufferpool.readValue(i, aggregate_column_index+4)
        return(num)



    """
    incremenets one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """
    def increment(self, key, column):
        r = self.select(key, self.table.key, [1] * self.table.num_columns)[0]
        if r is not False:
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r[column] + 1
            u = self.update(key, *updated_columns)
            return u
        return False
