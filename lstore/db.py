from lstore.table import Table

class Database():

    def __init__(self):
        self.tables = []
        pass

    # Not required for milestone1
    #Optional
    def open(self, path):
        pass
    #Optional
    def close(self):
        pass

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key_index):
        table = Table(name, num_columns, key_index)
        self.tables.append(table)  # Append the newly created table to the table list
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
