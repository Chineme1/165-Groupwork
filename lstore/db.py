from importlib.metadata import metadata
from lstore.table import Table
from lstore.bufferpool import BufferPool
import os
import csv

class Database():

    def __init__(self):
        self.tables = []
        pass

    def open(self, path):
        if not os.path.exists(path):
            os.mkdir(path)
        with open('./ECS165/database.csv', 'w') as csvfile:
            writer = csv.writer(csvfile)
            metadata_header = ['Indirection', 'RID', 'Timestamp', 'Schema Encoding', 'Data']
            writer.writerow(metadata_header)
    
    """
        lookup a specific row:

        def find_record(RID):
            with open('./ECS165/database.csv', 'r') as csvfile:
                reader = csv.reader(csvfile)
                find_RID = 0
                for row in reader:
                    if find_RID == RID:
                        return row
    """

    def close(self):
        BufferPool.flush()
        pass

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key_index):
        table = Table(name, num_columns, key_index)
        self.tables.append(table)   # Append the newly created table to the table list
        return table

    """
    # Deletes the specified table
    """
    def drop_table(self, name):
        for i in self.tables:
            if i.name == name:
                self.tables.remove(name)
                return True
        return False


    """
    # Returns table with the passed name
    """
    def get_table(self, name):
        for i in self.tables:
            if i.name == name:
                return i
        return False
