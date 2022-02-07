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
    # overloading [] operator 
    
    def __getitem__(self, key):
        return self.table
    """


    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """
    def delete(self, primary_key):
        if not self.table.page_directory(primary_key):
            return False
        page_range, page_number = self.table.page_directory(primary_key)
        return True


    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """

    def insert(self, *columns):
        #Creating a metadata array before adding data
        #Indirection -- ?? i forgot
        rid = self.tables.num_records
        ts = time.time()
        schema_encoding = 0
        meta = [indirection, rid, ts, schema_encoding]
        #adds record, with the first element being the key
        data = meta.append(columns)



        #check if theres enough space in page/page range
        #then write to base page
        for i, value in enumerate(data):
            page =  #find the page -- how are we finding the page. will return page range and position
            #check if full using the page's has_capacity function
            #check if page range is full
            page.write(value)
            self.tables.num_records += 1
        pass


    """
    # Read a record with specified RID and returns it
    # :param RID: the RID of the record we want to select
    """
    def selectWithRID(self, RID):
        pass



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
        pass



    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """

    def update(self, primary_key, *columns):
        # remember to change the schema coding from 0 to 1
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
