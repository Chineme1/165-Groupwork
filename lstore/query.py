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
        self.table.num_table_record += 1
        indirection = None
        rid = self.table.num_table_record+1 #as rid = 0 is reserved
        ts = int(time.time())
        schema_encoding = 0
        meta = [indirection, rid, ts, schema_encoding]
        # adds record, with the first element being the key
        self.table.index.indices[0].insert(columns[0], rid, self.table.index.indices[0].root)
        self.table.write(indirection, 0)
        self.table.write(rid, 1)
        self.table.write(ts, 2)
        self.table.write(schema_encoding, 3)
        for i in range(self.table.num_columns):
            self.table.write(columns[i], i+4)
        return (True)




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
        arr = [[None for i in range(numCols)] for j in range(1)]
        for i in range (0, numCols):
            if query_columns[i] == 1:   # check which values in the query_columns are 1
                arr[0][i] = (self.table.read(RID, i+4))  # read the data in the desired columns and append it to the list
        ret = []
        ret.append(Record(RID, index_value, arr))
        return(ret)


    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """

    def update(self, primary_key, *columns):
        output = []
        out = self.table.index.indices[0].find(primary_key, self.table.index.indices[0].root, output)
        try:
            RID = output[0]  # find the RID of the record we want to update
        except:
            return False
        numCols = len(columns)
        Indirection = self.table.read(RID, 0)     # find the value in the indirectionn column of given record
        ts = int(time.time())
        schema_encoding = 0
        update_RID = self.table.tail_write(Indirection, 0, RID)  # store the RID of the previous update in the indirection column of the latest update
        # tail_write() stores the RID of the latest update in the indirection column of the base record and returns it
        self.table.tail_write(update_RID, 1, RID)
        self.table.tail_write(ts, 2, RID)
        self.table.tail_write(schema_encoding, 3, RID)
        for i in range(0, numCols):
            if columns[i] == None:   # if the value of columns[i] is not updated
                unupdated_val = self.table.read(RID, i+4)   # read the value from the record in the base page
                self.table.tail_write(unupdated_val, i+4, RID)
            else:
                self.table.tail_write(columns[i], i+4, RID)
                self.table.write2(1, 3, RID)
        return True




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


