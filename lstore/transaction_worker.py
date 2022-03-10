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
        x= threading.Thread()#This is an instance of the thread
            #this should add the transactions to threads
        x(self.__run()) #I don't know what this does or do we put it into the Thread.// Not sure#Why is it calliing itself. Is it meant to be recursive
        x.start()

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

