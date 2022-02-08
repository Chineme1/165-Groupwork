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
        if not self.table.page_directory(primary_key):
            return False
        output = []
        RID = self.table.index.indices[0].find(primary_key, self.table.index.indices[0].root, output)
        self.table.delete(RID)
        return(True)
            
        
        


    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """

    def insert(self, *columns):
        #Creating a metadata array before adding data
        #Indirection -- ?? i forgot
        rid = self.table.num_records
        ts = time.time()
        schema_encoding = 0
        meta = [indirection, rid, ts, schema_encoding]
        #adds record, with the first element being the key
        data = meta.append(columns)
        numCol = len(data)
        for i in range (0, numCol):
            self.table.write(data[i], i)
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
        RID = self.table.index.indices[index_column].find(index_value, self.table.index.indices[index_column].root, output) # find the RID with the filter parameters
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
        # select 
        # append to tail pages
        # indirection column link to base page or previous update
        # change schema coding from 0 to 1

        pass

    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """

    def sum(self, start_range, end_range, aggregate_column_index):
        def sum(self, start_range, end_range, aggregate_column_index):
        output = []
        self.table.index.indices[0].findRange(start_range, end_range, self.table.index.indices[0].root, output)
        num = 0
        for i in output:
            num += self.table.read(i, aggregate_column_index)
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