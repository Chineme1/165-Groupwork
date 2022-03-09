from lstore.table import Table, Record
from lstore.index import Index
import time
import threading
class TransactionWorker:

    """
    # Creates a transaction worker object.
    """
    def __init__(self, transactions = []):
        self.stats = []
        self.transactions = transactions
        self.result = 0
        pass

    """
    Appends t to transactions
    """
    def add_transaction(self, t):
        t.num = len(self.transactions)+1
        self.transactions.append(t)
        

    """
    Runs all transaction as a thread
    """
    def run(self):
        for i in self.transactions:
            i.run()
    

    """
    Waits for the worker to finish
    """
    def join(self):
        time.sleep(2)


    def __run(self):
        for transaction in self.transactions:
            # each transaction returns True if committed or False if aborted
            self.stats.append(transaction.run())
        # stores the number of transactions that committed
        self.result = len(list(filter(lambda x: x, self.stats)))

