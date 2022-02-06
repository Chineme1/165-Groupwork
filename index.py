"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""
# need to change to allow insertions of equal keys

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
        self.leaf = False      
        self.leftChild = None
        self.middleChild = None
        self.rightChild = None
        self.parent = None
        self.entries = [entry]
        self.numEntries = 1

class BTree:
    def __init__(self):
        self.root = None
    
    def sort(self):
        for i in range(self.numEntries):
            for j in range(i+1, self.numEntries):
                if entries[i].key > entries[j].key:
                    temp = entries[i]
                    entries[i] = entries[j]
                    entries[j] = temp

    def insert(self, key, RID, node):
        temp = BTreeEntry(key, RID)
        if self.root == None:
            self.root = BTreeNode(temp, True)
            return(self.root)
        if node == None:
            return(BTreeNode(temp, True))
        if node.leaf == False:
            if key < node.entries[0].key:
                node.leftChild = self.insert(key, RID, node.leftChild)
                node.leftChild.parent = node
            elif node.numEntries == 1 or key > node.entries[1].key:
                node.rightChild = self.insert(key, RID, node.rightChild)
                node.rightChild.parent = node
            else:
                node.middleChild = self.insert(key, RID, node.middleChild)
                node.middleChild.parent = node
            return(node)
        if node.numEntries < 2:
            node.entries.append(temp)
            node.numEntries += 1
            node.sort()
            return(node)
        if node.numEntries >= 2:
            node.entries.append(temp)
            node.sort()
            left = BTreeNode(node.entries[0], True)
            right = BTreeNode(node.entries[2], True)
            entry = [node.entries[1]]
            node.entries = [entry]
            node.numEntries = 1
            node.leftChild = left
            node.leftChild.parent = node
            node.rightChild = right
            node.rightChild.parent = node
            push(entry, node)
            return(node)

    def push(entry, node):
        if node.parent.numEntries == 2:
            if node.parent.leftChild == node:
                left = BTreeNode(entry, False)
                right = BTreeNode(node.parent.entries[1], False)
                middle = BTreeNode(node.parent.entries[0], False)
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
                push(middle, node.parent)
            elif node.parent.rightChild == node:
                right = BTreeNode(entry, False)
                left = BTreeNode(node.parent.entries[0], False)
                middle = BTreeNode(node.parent.entries[1], False)
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
                push(middle, node.parent)
            else:
                left = BTreeNode(node.parent.entries[0], False)
                right = BTreeNode(node.parent.entries[1], False)
                middle = BTreeNode(entry, False)
                middle.leftChild = node.leftChild
                middle.leftChild.parent = middle
                middle.rightChild = node.rightChild
                middle.rightChild.parent = middle
                right.leftChild = node.leftChild
                right.leftChild.parent = right
                right.rightChild.parent = right
                right.rightChild = node.parent.rightChild
                left.leftChild = node.parent.leftChild
                left.leftChild.parent = left
                left.rightChild = node.leftChild
                left.rightChild.parent = left
                push(middle, node.parent)
        else:
            if node.parent.rightChild == node:
                node.parent.middleChild = node.leftChild
                node.parent.middleChild.parent = node.parent
                node.parent.rightChild = node.rightChild
                node.parent.rightChild.parent = node.parent
                node.parent.entries[1] = entry
                node.parent.numEntries = 2
            else:
                node.parent.middleChild = node.rightChild
                node.parent.middleChild.parent = node.parent
                node.parent.leftChild = node.leftChild
                node.parent.leftChild.parent = node.parent
                node.parent.entries[1] = entry
                node.parent.numEntries = 2
        return(node)
        
    def find(self, key, node, output):
        if node == None:
            return(output)
        if key < node.entries[0].key:
            return(self.find(key, node.leftChild, output))
        if key == node.entries[0].key:
            output.append(node.entries[0].RID)
        if node.numEntries == 1 or key > node.entries[1].key:
            return(self.find(key, node.rightChild, output))
        if key == node.entries[1].key:
            output.append(node.entries[1].RID)
        else:
            return(self.find(key, node.middleChild))
            
    def findRange(self, low, high, node, output):
        if node == None:
            return(output)
        if low >= node.entries[0].key:
            output = (self.findRange(low, high, node.leftChild, output))
        if low <= node.entries[0].key and high >= node.entries[0].key:
            output.append(node.entries[0].RID)
        if node.numEntries == 1 or high <= node.entries[1].key:
            output = (self.findRange(low, high, node.rightChild, output))
        if node.numEntries == 2 and low <= node.entries[1].key and high >= node.entries[1].key:
            output.append(node.entries[1].RID)
        if node.numEntries == 2 and (low <= node.entries[0].key or high >= node.entries[1].key):
            output = (self.findRange(low, high, node.middleChild, output))
        return(output)

tree = BTree() 
tree.insert(1, 1, tree.root)
tree.insert(10, 2, tree.root)
tree.insert(7, 3, tree.root)
tree.insert(4, 4, tree.root)
tree.insert(100, 5, tree.root)
tree.insert(5, 6, tree.root)
tree.insert(8, 7, tree.root)
tree.insert(9, 8, tree.root)
output = []
print(tree.find(7, tree.root, output))
output = []
print(tree.findRange(1, 10, tree.root, output))
