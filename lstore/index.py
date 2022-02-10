"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""
# may need to change to allow insertions of equal keys. seems like it kinda works?
from random import choice, randrange, randint, seed
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
                    done = 0

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
                temp2 = self.insert(key, RID, node.leftChild)
                if node.leftChild == None:
                    node.leftChild = temp2 
                    node.leftChild.parent = node
            elif len(node.entries) == 1 or key > node.entries[1].key:
                temp2 = self.insert(key, RID, node.rightChild)
                if node.rightChild == None:
                    node.rightChild = temp2
                    node.rightChild.parent = node
            else:
                temp2 = self.insert(key, RID, node.middleChild)
                if node.middleChild == None:
                    node.middleChild = temp2
                    node.middleChild.parent = node
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
                right = BTreeNode(node.parent.entries[1], False)
                middle = BTreeNode(node.parent.entries[0], False)
                middle.parent = node.parent.parent
                if node.parent.parent != None:
                    if node.parent == node.parent.parent.leftChild:
                        node.parent.parent.leftChild = middle
                    elif node.parent == node.parent.parent.rightChild:
                        node.parent.parent.rightChild = middle
                    else:
                        node.parent.parent.middleChild = middle
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
                    if node.parent == node.parent.parent.leftChild:
                        node.parent.parent.leftChild = middle
                    elif node.parent == node.parent.parent.rightChild:
                        node.parent.parent.rightChild = middle
                    else:
                        node.parent.parent.middleChild = middle
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
                # middle.leftChild = node.leftChild
                # middle.leftChild.parent = middle
                # middle.rightChild = node.rightChild
                # middle.rightChild.parent = middle
                # right.leftChild = node.leftChild
                # right.leftChild.parent = right
                # right.rightChild = node.parent.rightChild
                # right.rightChild.parent = right
                # left.leftChild = node.parent.leftChild
                # left.leftChild.parent = left
                # left.rightChild = node.leftChild
                # left.rightChild.parent = left
                left.leftChild = node.parent.leftChild
                node.parent.leftChild.parent = left
                left.rightChild = node.leftChild
                node.leftChild.parent = left
                right.leftChild = node.rightChild
                node.rightChild.parent = right
                right.rightChild = node.parent.rightChild
                node.parent.rightChild.parent = right
                middle.leftChild = left
                left.parent = middle
                middle.rightChild = right
                right.parent = middle
                if node.parent.parent != None:
                    if node.parent == node.parent.parent.leftChild:
                        node.parent.parent.leftChild = middle
                    elif node.parent == node.parent.parent.rightChild:
                        node.parent.parent.rightChild = middle
                    else:
                        node.parent.parent.middleChild = middle
                middle.parent = node.parent.parent
                self.push(middle)
        else:
            if node.parent.rightChild != node and node.parent.leftChild != node:
                print("an error occured")
                print(node.entries[0].key-92106429)
                self.treePrint(node.parent)
                print("end of error log")
            if node.parent.rightChild == node:
                node.parent.middleChild = node.leftChild
                node.parent.middleChild.parent = node.parent
                node.parent.rightChild = node.rightChild
                node.parent.rightChild.parent = node.parent
                node.parent.entries.append(node.entries[0])
                node.parent.sort()
            else:
                node.parent.middleChild = node.rightChild
                node.parent.middleChild.parent = node.parent
                node.parent.leftChild = node.leftChild
                node.parent.leftChild.parent = node.parent
                node.parent.entries.append(node.entries[0])
                node.parent.sort()
        return(True)
        
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

    def treePrint(self, node):
        if len(node.entries) == 1:
            print(node.entries[0].key-92106429)
        else:
            print(node.entries[0].key-92106429,  '   ' , node.entries[1].key-92106429)
        if node.leftChild != None:
            self.treePrint(node.leftChild)
        if node.middleChild != None:
            self.treePrint(node.middleChild)
        if node.rightChild != None:
            self.treePrint(node.rightChild)

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
# for i in range (1, 10):
    # tree.insert(i+906659671, i, tree.root)
    # output = []
    # tree.find(i+906659671, tree.root, output)
    # print(i)
    # list.append(i+906659671)
# print(tree.root.entries[0].RID)
# print(tree.root.leftChild.entries[0].RID)
# print(tree.root.rightChild.entries[0].RID)
# for i in range (1, 10):
    # print(i)
    # output = []
    # #num = choice(list)
    # num = i+906659671
    # tree.find(num, tree.root, output)
    # if output[0] != i:
        # print("something went wrong")
        # x = 0

# records = {}

# number_of_records = 1000
# number_of_aggregates = 100
# seed(3562901)

# for i in range(0, number_of_records):
    # key =  92106429 + randint(0, number_of_records)

    # #skip duplicate keys
    # while key in records:
        # key =  92106429 + randint(0, number_of_records)

    # records[key] = [key, randint(0, 20), randint(0, 20), randint(0, 20), randint(0, 20)]
    # print(key-92106429)
    # tree.insert(key, 0, tree.root)
    # # print("start of tree print")
    # # tree.treePrint(tree.root)
    # # print("end of tree print")
    # # print(tree.root.entries[0].key-92106429)
    
    # # print('inserted', records[key])
# print("Insert finished")


# tree.insert(4, 0, tree.root)
# tree.insert(3, 0, tree.root)
# tree.insert(1, 0, tree.root)
# tree.insert(0, 0, tree.root)
# tree.insert(10, 0, tree.root)
# tree.insert(7, 0, tree.root)
# tree.insert(9, 0, tree.root)
# tree.insert(8, 0, tree.root)
# tree.insert(2, 0, tree.root)
# tree.insert(6, 0, tree.root)
# print(tree.root.entries[0].key-92106429)
# print(tree.root.leftChild.entries[0].key-92106429)
# print(tree.root.rightChild.entries[0].key-92106429)
#print(tree.root.middleChild.entries[0].key-92106429)
# print("start of tree print")
# tree.treePrint(tree.root)
# print("end of tree print")

# for key in records:
    # print( "key = ", key-92106429)
    # #select function will return array of records 
    # #here we are sure that there is only one record in t hat array
    # output = []
    # record = tree.find(key, tree.root, output)
    # print(output[0])
    # error = False
    # for i, column in enumerate(record.columns):
        # if column != records[key][i]:
            # error = True
    # if error:
        # print('select error on', key, ':', record, ', correct:', records[key])
    # else:
        # pass
        # print('select on', key, ':', record)
            
4
3
1
0
10
7
9
8
2
6