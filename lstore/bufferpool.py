
from typing import Counter
from lstore.PageRange import PageRange
from lstore.table import Table

class Node:
    def __init__(self,num_columns,pr_key,key):
        self.index = pr_key     #store the index of the node，format:pagerange*10000 + pagerange position
        self.data =PageRange(num_columns,pr_key,key) #store the data of the pagerange containing rid
        self.next = None
        self.isdirt = 0 #dirt when it's 1



class LinkedList:
    def __init__(self):
        self.headnode = None
        self.endnode = None
    

class BufferPool:

    def __init__(self, num_pages,num_columns,key):
        self.dirtydata = []           #list that keeps track of all dirty pagerange
        self.bufferpool = LinkedList()   #actually store the data
        self.pagerange_index = []         # index of pagerange stored format: pagerange*10000 + pagerange position
        self.size = num_pages       # size of the bufferpool
        self.counter = 0           #count the num of pagerange
        self.num_columns = num_columns
        self.key = key

    def read_record(self,RID):
        page_range_index = (RID-1)//8192 
        index = page_range_index
        find = 0
        for bp_index in self.pagerange_index:
            if index == bp_index:
                find =1
        if(find==0):
            self.bufferpool.write_record(RID,self.num_columns,self.key)
        else: 
            node = Node(index)
            self.bufferpool.last.next = node #let previous node point to node
            self.bufferpool.endnode=node  #add node to the tail
        
        return self.bufferpool.node(index).data

    def create_node(self,RID,num_columns,key):
        page_range_index = (RID-1)//8192 
        pr_key = page_range_index
        node = Node(num_columns,pr_key,key)


        if self.bufferpool.first == None:  #check the situation for creating first node
            self.bufferpool.headnode = node
    
        self.bufferpool.last.next = node #let previous node point to node
        self.bufferpool.endnode = node  #add node to the tail
        self.pagerange_index.append(pr_key) #add index to basepage index list
    
    def write_record(self,RID):
        if self.have_capacity():
            self.create_node(RID,self.num_columns,self.key)
            self.counter + 1 
        else:
            #check if the data is dirty first, didn't implement
            #remove first node
            self.create_node(RID,self.num_columns,self.key)

    def evict_page(self):
        self.bufferPool.remove(self.bufferPool.headnode)

    def have_capacity(self):
        if self.counter< self.size:
            return True
        else:
            return False



