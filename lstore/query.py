from lstore.table import Table, Record
from lstore.index import Index
from lstore.page import Page
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
        pass



    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """
    def delete(self, primary_key):
        output = []
        out = self.table.index.indices[0].find(primary_key, self.table.index.indices[0].root, output)
        try:
            RID = output[0]
        except:
            return False
        self.table.delete(RID)
        return(True)
            
        
        


    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """

    def insert(self, *columns):
        #Creating a metadata array before adding data
        try:
            self.table.num_table_record += 1
            indirection = None
            rid = self.table.num_table_record
            ts = time.time()
            schema_encoding = 0
            meta = [indirection, rid, ts, schema_encoding]
            # adds record, with the first element being the key
            self.table.index.indices[0].insert(columns[0], rid, self.table.index.indices[0].root)
            numCol = self.table.num_columns + 4
            for i in range(4, numCol):
                self.table.write(columns[i - 4], i)
            return (True)
        except:
            return False




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
        out = self.table.index.indices[index_column].find(index_value, self.table.index.indices[index_column].root, output) # find the RID with the filter parameters
        RID = output[0]
        numCols = len(query_columns)
        arr = []
        for i in range (0, numCols):
            if query_columns[i] == 1:   # check which values in the query_columns are 1
                arr.append(self.table.read(RID, i))  # read the data in the desired columns and append it to the list
        return(arr)


    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """

    def update(self, primary_key, *columns):
        output = []
        out = self.table.index.indices[0].find(primary_key, self.table.index.indices[0].root, output)
        try:
            RID = output[0]
        except:
            return False
        numCols = len(columns)
        Indirection = self.table.read(RID, 0)
        rid = 0
        ts = int(time.time())
        schema_encoding = 0
        self.table.tail_write(Indirection, 0, RID)
        self.table.tail_write(rid, 1, RID)
        self.table.tail_write(ts, 2, RID)
        self.table.tail_write(schema_encoding, 3, RID)
        for i in range(0, numCols):
            if columns[i] == None:
                val = self.table.read(RID, i)
                self.table.tail_write(val, 3, RID)
            else:
                self.table.tail_write(columns[i], i+4, RID)
                self.table.write2(1, 3, RID)
            
        


        """       
        if not self.table.page_directory(primary_key):
            return False
        output = []
        RID = self.table.index.indices[0].find(primary_key, self.table.index.indices[0].root, output) #find RID of the record we want to update
        indirectionRID = self.table.read(RID, 0)   # find the value in the indirection column of the record
        for i in range(columns):
            if columns(i):
                self.table.tail_write(columns(i), i+4, RID)   # write the given updated value to the tail page (tail_write edits the indirection column)
            if not columns(i):
                unupdated_value = self.table.read(RID, i+4)  # get the data that stays the same
                self.table.tail_write(unupdated_value, i+4, RID)  # write the unupdated value to the tail page

        if indirectionRID:  # check if it's the first update
            lasted_update_RID = self.table.read(RID, 0)   # find the RID stored in the indirection column, which points to the lastest update in the tail page
            self.table.tail_write2(indirectionRID, 0, lasted_update_RID)  # change the RID of the previous update in the indirection column of the latest update
        return True
        """



    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """

    def sum(self, start_range, end_range, aggregate_column_index):
        output = []
        self.table.index.indices[0].findRange(start_range, end_range, self.table.index.indices[0].root, output)
        num = 0
        try:
            for i in output:
                num += self.table.read(i, aggregate_column_index)
            return (num)
        except:
            return False

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
