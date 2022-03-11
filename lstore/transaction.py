from lstore.table import Table, Record
from lstore.index import Index

class Transaction:

    """
    # Creates a transaction object.
    """
    def __init__(self):
        self.queries = []
        self.num = 0
        self.listLocks = []
        self.inserts = []
        self.listRID = []

    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, grades_table, 0, *[None, 1, None, 2, None])
    """
    def add_query(self, query, table, *args):
        self.queries.append((query, args))
        # use grades_table for aborting

    # If you choose to implement this differently this method must still return True if transaction commits or False on abort
    def run(self):
        for query, args in self.queries:
                success = self.lock(query, args, self.num)
                #print(query.__name__)
                #print(success)
                if success == False:
                    return self.abort()
        return self.commit()

    def abort(self):
        self.unlock()
        return False

    def commit(self):
        for query, args in self.queries:
            query(*args)
        self.unlock()
        return True
        
    def lock(self, query, args, num):
        if query.__name__ == 'insert':
            self.inserts.append(args[0])
            return(True)
        elif query.__name__ == 'delete':
            if args[0] in self.inserts:
                return(True)
            output = []
            query.__self__.table.index.indices[0].find(args[0], query.__self__.table.index.indices[0].root, output)
            self.listLocks.append(query)
            self.listRID.append(output[0])
            return(query.__self__.table.lockmanager.GetExclusive(self.num, output[0]))
        elif query.__name__ == 'select':
            if args[0] in self.inserts:
                return(True)
            output = []
            query.__self__.table.index.indices[0].find(args[0], query.__self__.table.index.indices[0].root, output)
            self.listLocks.append(query)
            self.listRID.append(output[0])
            return(query.__self__.table.lockmanager.GetShared(self.num, output[0]))
        elif query.__name__ == 'update':
            if args[0] in self.inserts:
                return(True)
            output = []
            query.__self__.table.index.indices[0].find(args[0], query.__self__.table.index.indices[0].root, output)
            self.listLocks.append(query)
            self.listRID.append(output[0])
            return(query.__self__.table.lockmanager.GetExclusive(self.num, output[0]))
        elif query.__name__ == 'sum':
            pass
        else:
            print("invalid function name?")

    def unlock(self):
        for i in range(0, len(self.listLocks)):
            self.listLocks[i].__self__.table.lockmanager.unlock(self.num, self.listRID[i])
        self.listLocks = []
        self.listRID = []
    
    # def locked(self, query, args, num):
        # if query.__name__ == 'insert':
            # return(query.table.lockamanger.locked(self.num, args[0]))
        # elif query.__name__ == 'delete':
            # for i in self.listLocks:
                # if i == args[0]:
                    # return(False)
            # else:
                # return(query.table.locked(args[0], 0))
        # elif query.__name__ == 'select':
            # return(False)
        # elif query.__name__ == 'update':
            # for i in self.listLocks:
                # if i == args[1][0]:
                    # return(False)
            # else:
                # return(query.table.locked(args[1][0], 0))
        # elif query.__name__ == 'sum':
            # return(False)
        # else:
            # print("invalid function name?")
    

#sum function
#check if lock is present in unlock function
#add lockamanger to table
#using key or rid?