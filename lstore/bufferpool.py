from lstore.PageRange import PageRange

class Node:
    def __init__(self, pageRange):   
        self.data = pageRange #store the data of the pagerange containing rid
        self.child = None
        self.isdirt = 0 #dirt when it's 1



class LinkedList:
    def __init__(self):
        self.headnode = None
        self.endnode = None
    
        
class BufferPool:            
            
    def __init__(self, num_pages,num_columns, key,  file):
        self.bufferpool = LinkedList()   #actually store the data       
        self.size = num_pages       # size of the bufferpool
        self.counter = 0           #count the num of pagerange
        self.num_columns = num_columns+4
        self.key = key
        self.file = file
        self.size2 = 0
            
    def readValue(self,RID, column):
        page_range_index = (RID-1)//8192
        position = ((RID-1)%8192)+1 
        find = self.find_page(page_range_index)
        if find == None:
            self.evict_page()
            find = self.file.readPageRange(self.num_columns, page_range_index, self.key)
            self.add_page(find)
        return(find.BaseRead(position, column))
        
    def update(self,RID, columns, updated):
        page_range_index = (RID-1)//8192
        position = ((RID-1)%8192)+1 
        find = self.find_page(page_range_index)
        if find == None:
            self.evict_page()
            find = self.file.readPageRange(self.num_columns, page_range_index, self.key)
            self.add_page(find)
        find.Update(position, columns, updated)
        
    def writeValue(self,data, RID):
        page_range_index = (RID-1)//8192
        position = ((RID-1)%8192)+1 
        find = self.find_page(page_range_index)
        if position == 1:
            self.size2 += 1
            find = PageRange(self.num_columns, page_range_index, self.key)
            self.evict_page()
            self.add_page(find)
        elif find == None:
            self.evict_page()
            find = self.file.readPageRange(self.num_columns, page_range_index, self.key)
            self.add_page(find)
        find.BaseWrite(data, None)
        
    def delete(self,RID):
        page_range_index = (RID-1)//8192
        position = ((RID-1)%8192)+1 
        find = self.find_page(page_range_index)
        if find == None:
            self.evict_page()
            find = self.file.readPageRange(self.num_columns, page_range_index, self.key)
            self.add_page(find)
        find.Delete(position)
        
        
    def evict_page(self):
        if self.counter == self.size:
            self.file.writePageRange(self.bufferpool.headnode.data)
            self.bufferpool.headnode = self.bufferpool.headnode.child
            self.counter -= 1
    
    def evict_all(self):
        while self.counter != 0:
            self.file.writePageRange(self.bufferpool.headnode.data)
            self.bufferpool.headnode = self.bufferpool.headnode.child
            self.counter -= 1
        
    def merge(self):
        nex = self.bufferpool.headnode
        while nex != None:
            nex.data.merge()
            nex = nex.child
        
    def add_page(self,pageRange):
        new = Node(pageRange)
        if self.bufferpool.headnode == None:
            self.bufferpool.headnode = new
            self.bufferpool.endnode = new
        else:
            self.bufferpool.endnode.child = new
        self.counter += 1
        
    def find_page(self,index):
        node = self.bufferpool.headnode
        while node != None:
            if node.data.pr_key == index:
                return(node.data)
            node = node.child
        return(None)
        