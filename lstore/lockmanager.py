import threading
from typing_extensions import Self
from transaction import Transaction

import threading, time, random
 
mutex = threading.Lock()


class Lock:
    def __init__(self):
        shared = []
        exlcusive = None


class LockManager:

    def __init__(self):
        self.d = {1:Lock(1)}
        pass

    #should be guarde by mutex（one transaction accessing, no more can access)(do this first)
    def GetShared(self,transaction,rid):
        global mutex
        mutex.acquire()
        #check if the transaction already has this lock for all functions

        #first check if exist in d
        if self.d[rid]==None:
           self.d[rid]=Lock()
        if self.d[rid].exclusive != None:
            mutex.release()
            return False
        self.d[rid].shared.append(transaction)
        mutex.release()
        return True


    def GetExlusive(self,transaction,rid):
        global mutex
        mutex.acquire()
        if len(self.d[rid].shared) != 0:
            mutex.release()
            return False
        if self.d[rid].exclusive != None:
            mutex.release()
            return False
        if len(self.d[rid].shared)==1 and self.d[rid].shared[0] == transaction and self.d[rid].exclusive == None:
            self.d[rid].exclusive = transaction
            mutex.release()
            return True
        if len(self.d[rid].shared)==0 and self.d[rid].exclusive == None:
            self.d[rid].exclusive = transaction
            mutex.release()
            return True
        if len(self.d[rid].shared)==0 and self.d[rid].exclusive == transaction:
            mutex.release()
            return True
        return False

    def unlock(self,transaction,rid):
        #delete the entry when delete the lock
        global mutex
        mutex.acquire()
        if rid in self.d:
            if self.d[rid].exclusive == transaction:
                self.d[rid].exclusive = None
            for i in self.d[rid].shared:
                if i == transaction:
                    i = None
            if self.d[rid].exclusive==None and len(self.d[rid].shared)==0:
                del self.d[rid]
            #if no exlusive and shared lock then delete the entry
        mutex.release()
        return True
