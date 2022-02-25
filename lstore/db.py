from .table import Table
import os

class Database():

    def __init__(self):
        self.tables = []
        path = None
        pass


    def open(self, path):
        self.path = path
        if os.path.isdir(path) == False:
            os.mkdir(path, mode = 0o777)
        elif os.path.exists(path + "/tables"): 
            with open(path + "/tables") as file:
                while(1):
                    line = file.readline()
                    if line == '':
                        break
                    line = line[:-1]
                    with open(path + "/%s"%line + "/%s.txt"%line) as table:
                        name = table.readline()
                        name = name[:-1]
                        num_cols = int(table.readline())
                        key = int(table.readline())
                        temp = self.create_table(name, num_cols, key)
                        num = int(table.readline())
                        for i in range(0, num):
                            with open(path + "/%s"%line + "/%s.txt"%i) as f:
                                while(1):
                                    test = f.readline()
                                    if test == '':
                                        break
                                    rid = int(f.readline())
                                    f.readline()
                                    f.readline()
                                    key = int(f.readline())
                                    for i in range(0, num_cols-1):
                                        f.readline()
                                    temp.index.indices[0].insert(key, rid, temp.index.indices[0].root)

    def close(self):
        with open(self.path + "/tables" , 'w') as txt_file:
            for i in self.tables:
                i.bufferpool.evict_all()
                txt_file.write(i.name)
                txt_file.write('\n')
                with open(self.path + "/%s"%i.name + "/%s.txt"%i.name, 'w') as txt_file:
                    txt_file.write(i.name)
                    txt_file.write('\n')
                    txt_file.write(str(i.num_columns))
                    txt_file.write('\n')
                    txt_file.write(str(i.key))
                    txt_file.write('\n')
                    txt_file.write(str(i.bufferpool.size2))

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key_index):
        path = self.path + "/%s"%name
        if os.path.isdir(path) == False:
            os.mkdir(path, mode = 0o777)
        table = Table(name, num_columns, key_index, path)
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
