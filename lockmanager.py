import threading

 
mutex = threading.Lock()


class Lock:
    def __init__(self):
        self.shared = []
        self.exclusive = None


class LockManager:

    def __init__(self):
        self.d = {}
        pass

    #should be guarde by mutex（one transaction accessing, no more can access)(do this first)
    def GetShared(self,transaction,rid):
        global mutex
        mutex.acquire()
        #check if the transaction already has this lock for all functions
        #first check if exist in d
        if self.d.get(rid) is None:
           self.d[rid]=Lock()

        if self.d.get(rid).exclusive != None and self.d.get(rid).exclusive != transaction:
            mutex.release()
            
            return False

        duplicate = False
        for i in self.d.get(rid).shared:
            if i == transaction:
                duplicate = True
        if  duplicate == False:
            self.d.get(rid).shared.append(transaction)
            mutex.release()
            
            return True
        else:
            mutex.release()
            
            return True
        


    def GetExclusive(self,transaction,rid):
        global mutex
        mutex.acquire()
        if self.d.get(rid) is None:
           self.d[rid]=Lock()

        if len(self.d.get(rid).shared)==1 and self.d.get(rid).shared[0] == transaction and self.d.get(rid).exclusive == None:
            self.d.get(rid).exclusive = transaction
            mutex.release()

            return True

        if len(self.d.get(rid).shared)==1 and self.d.get(rid).shared[0] == transaction and self.d.get(rid).exclusive == transaction:
            mutex.release()

            return True

        if len(self.d.get(rid).shared) >=1:
            mutex.release()

            return False
        if self.d.get(rid).exclusive is not None and self.d.get(rid).exclusive != transaction:
            mutex.release()

            return False
        if len(self.d.get(rid).shared) ==0 and self.d.get(rid).exclusive == None:
            self.d.get(rid).exclusive = transaction
            mutex.release()

            return True
        if len(self.d.get(rid).shared) ==0  and self.d.get(rid).exclusive == transaction:
            mutex.release()

            return True
        mutex.release()

        return False

    def unlock(self,transaction,rid):
        #delete the entry when delete the lock
        global mutex
        mutex.acquire()
        if rid in self.d:
            if self.d.get(rid).exclusive == transaction:
                self.d.get(rid).exclusive = None
            for i in self.d.get(rid).shared:
                if i == transaction:
                    i = None
            if self.d.get(rid).exclusive==None and len(self.d.get(rid).shared)==0:
                self.d.pop(rid)
            #if no exlusive and shared lock then delete the entry
        mutex.release()
        return True
