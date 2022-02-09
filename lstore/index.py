"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""
# may need to change to allow insertions of equal keys. seems like it kinda works?
from random import choice, randrange, randint
from time import process_time

class Index:

    def __init__(self, table):
        # One index for each table. All our empty initially.
        self.indices = [None] *  table.num_columns
        self.indices[0] = BTree()
        

    """
    # returns the location of all records with the given value on column "column"
    """

    def locate(self, column, value):
        output = []
        return(find(value, self.indices[column].root, output))

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column):
        output = []
        return(findRange(begin, end, self.indices[column].root, output))

    """
    # optional: Create index on specific column
    """

    def create_index(self, column_number):
        pass

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        pass

class BTreeEntry:
    def __init__(self, key, RID):
        self.key = key
        self.RID = RID       

class BTreeNode:
    def __init__(self, entry, leaf):
        self.leaf = leaf      
        self.leftChild = None
        self.middleChild = None
        self.rightChild = None
        self.parent = None
        self.entries = [entry]
        
        
    def sort(self):
        entries = len(self.entries)
        done = 0
        while done == 0:
            done = 1
            for i in range(0, entries-1):
                if self.entries[i].key > self.entries[i+1].key:
                    temp = self.entries[i]
                    self.entries[i] = self.entries[i+1]
                    self.entries[i+1] = temp

class BTree:
    def __init__(self):
        self.root = None
    

    def insert(self, key, RID, node):
        temp = BTreeEntry(key, RID)
        if self.root == None:
            self.root = BTreeNode(temp, True)
            return(self.root)
        if node == None:
            return(BTreeNode(temp, True))
        if node.leftChild != None or node.rightChild != None or node.middleChild != None:
            if key < node.entries[0].key:
                self.insert(key, RID, node.leftChild)
            elif len(node.entries) == 1 or key > node.entries[1].key:
                self.insert(key, RID, node.rightChild)
            else:
                self.insert(key, RID, node.middleChild)
            return(node)
        if len(node.entries) < 2:
            node.entries.append(temp)
            node.sort()
            return(node)
        if len(node.entries) >= 2:
            node.entries.append(temp)
            node.sort()
            left = BTreeNode(node.entries[0], True)
            right = BTreeNode(node.entries[2], True)
            entry = node.entries[1]
            node.entries = [entry]
            node.leftChild = left
            node.leftChild.parent = node
            node.rightChild = right
            node.rightChild.parent = node
            self.push(node)
            return(node)

    def push(self, node):
        if node.parent == None:
            self.root = node
            return(True)
        if len(node.parent.entries) == 2:
            if node.parent.leftChild == node:
                left = node
                right = BTreeNode(node.parent.entries[0], False)
                middle = BTreeNode(node.parent.entries[0], False)
                middle.parent = node.parent.parent
                if node.parent.parent != None:
                    node.parent.parent.leftChild = middle
                left.leftChild = node.leftChild
                left.leftChild.parent = left
                left.rightChild = node.rightChild
                left.rightChild.parent = left
                right.leftChild = node.parent.middleChild
                right.leftChild.parent = right
                right.rightChild = node.parent.rightChild
                right.rightChild.parent = right
                middle.leftChild = left
                middle.leftChild.parent = middle
                middle.rightChild = right
                middle.rightChild.parent = middle
                middle.parent = node.parent.parent
                self.push(middle)
            elif node.parent.rightChild == node:
                right = node
                middle = BTreeNode(node.parent.entries[1], False)
                middle.parent = node.parent.parent
                if node.parent.parent != None:
                    node.parent.parent.rightChild = middle
                left = BTreeNode(node.parent.entries[0], False)
                right.leftChild = node.leftChild
                right.leftChild.parent = right
                right.rightChild = node.rightChild
                right.rightChild.parent = right
                left.leftChild = node.parent.leftChild
                left.leftChild.parent = left
                left.rightChild = node.parent.middleChild
                left.rightChild.parent = left
                middle.leftChild = left
                middle.leftChild.parent = middle
                middle.rightChild = right
                middle.rightChild.parent = middle
                self.push(middle)
            else:
                left = BTreeNode(node.parent.entries[0], False)
                right = BTreeNode(node.parent.entries[1], False)
                middle = node
                middle.leftChild = node.leftChild
                middle.leftChild.parent = middle
                middle.rightChild = node.rightChild
                middle.rightChild.parent = middle
                right.leftChild = node.leftChild
                right.leftChild.parent = right
                right.rightChild = node.parent.rightChild
                right.rightChild.parent = right
                left.leftChild = node.parent.leftChild
                left.leftChild.parent = left
                left.rightChild = node.leftChild
                left.rightChild.parent = left
                middle.parent = node.parent.parent
                if node.parent != None:
                    node.parent.middleChild = middle
                self.push(middle)
        else:
            if node.parent.rightChild == node:
                node.parent.middleChild = node.leftChild
                node.parent.middleChild.parent = node.parent
                node.parent.rightChild = node.rightChild
                node.parent.rightChild.parent = node.parent
                node.parent.entries.append(node.entries[0])
            else:
                node.parent.middleChild = node.rightChild
                node.parent.middleChild.parent = node.parent
                node.parent.leftChild = node.leftChild
                node.parent.leftChild.parent = node.parent
                node.parent.entries.append(node.entries[0])
        return(node)
        
    def find(self, key, node, output):
        if node == None:
            return(output)
        if key < node.entries[0].key:
            return(self.find(key, node.leftChild, output))
        if key == node.entries[0].key:
            output.append(node.entries[0].RID)
        if len(node.entries) == 1 or key > node.entries[1].key:
            return(self.find(key, node.rightChild, output))
        if key == node.entries[1].key:
            output.append(node.entries[1].RID)
        else:
            return(self.find(key, node.middleChild, output))
            
    def findRange(self, low, high, node, output): # need to test more
        if node == None:
            return(output)
        if low <= node.entries[0].key:
            output = (self.findRange(low, high, node.leftChild, output))
        if low <= node.entries[0].key and high >= node.entries[0].key:
            output.append(node.entries[0].RID)
        if len(node.entries) == 1 or high >= node.entries[1].key:
            output = (self.findRange(low, high, node.rightChild, output))
        if len(node.entries) == 2 and low <= node.entries[1].key and high >= node.entries[1].key:
            output.append(node.entries[1].RID)
        if len(node.entries) == 2 and (low <= node.entries[0].key or high >= node.entries[1].key):
            output = (self.findRange(low, high, node.middleChild, output))
        return(output)

tree = BTree() 
# tree.insert(1, 1, tree.root)
# tree.insert(10, 2, tree.root)
# tree.insert(7, 3, tree.root)
# tree.insert(4, 4, tree.root)
# tree.insert(100, 5, tree.root)
# tree.insert(5, 6, tree.root)
# tree.insert(8, 7, tree.root)
# tree.insert(9, 8, tree.root)
# output = []
# print(tree.find(7, tree.root, output))
# output = []
# print(tree.findRange(1, 10, tree.root, output))
# insert_time_0 = process_time()
# for i in range (0, 100000):
    # num = randrange(0, 100000)
    # tree.insert(num, num, tree.root)
    # output = []
    # tree.find(num, tree.root, output)
    # if output[0] != num:
        # print("something went wrong")
#insert_time_1 = process_time()

#print("Inserting 10k records took:  \t\t\t", insert_time_1 - insert_time_0)
# list = []
# for i in range (1, 10000):
    # tree.insert(i+906659671, i, tree.root)
    # list.append(i+906659671)
# for i in range (1, 10000):
    # output = []
    # #num = choice(list)
    # num = i+906659671
    # tree.find(num, tree.root, output)
    # if output[0] != i:
        # print("something went wrong")
        # x = 0

