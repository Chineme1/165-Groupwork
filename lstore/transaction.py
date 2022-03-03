from lstore.table import Table, Record
from lstore.index import Index

class Transaction:

    """
    # Creates a transaction object.
    """
    def __init__(self):
        self.queries = []
        self.num = 0
        self.listInserts = []
        self.listLocks = []

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
            if self.locked(query, args, self.num) == True:
                return self.abort()
            else:
                self.lock(query, args, self.num)
        return self.commit()

    def abort(self):
        return False

    def commit(self):
        for query, args in self.queries:
            query(*args)
        self.unlock()
        return True
        
    def lock(self, query, args, num):
        if query.__name__ == 'insert':
            self.listInserts.append(args[0])
        elif query.__name__ == 'delete':
            query.table.lock(args[0], 0)
            self.listLocks.append(args[0])
        elif query.__name__ == 'select':
            pass
        elif query.__name__ == 'update':
            query.table.lock(args[1][0], self.num)
            self.listLocks.append(args[1][0])
        elif query.__name__ == 'sum':
            pass
        else:
            print("invalid function name?")

    def unlock(self):
        for i in self.listLocks:
            query.table.unlock(i)
    
    def locked(self, query, args, num):
        if query.__name__ == 'insert':
            return(False)
        elif query.__name__ == 'delete':
            for i in self.listLocks:
                if i == args[0]:
                    return(False)
            else:
                return(query.table.locked(args[0], 0))
        elif query.__name__ == 'select':
            return(False)
        elif query.__name__ == 'update':
            for i in self.listLocks:
                if i == args[1][0]:
                    return(False)
            else:
                return(query.table.locked(args[1][0], 0))
        elif query.__name__ == 'sum':
            return(False)
        else:
            print("invalid function name?")
    


#make sure to swap number of threads
#functions in transaction worker will need threading implemented