from .page import Page

class BasePage:
    def __init__(self,columns):
        self.columns = columns
        self.page = [] #Physiical holder
        self.counter = 0#total number of pages
        self.updates = 0
        for i in range(0, self.columns):
            x0 = Page()
            self.page.append(x0)

    def write(self, columns, position):
        if position == None:    
            i=0                            #write to the end
            for i in range(self.columns):
                if columns[i] != None :
                    self.page[i].write(columns[i],None)
            self.counter += 1
            return(self.counter)
        else:         
            i = 0                                    #write to the position
            for i in range(self.columns):
                if columns[i] != None :
                    self.page[i].write(columns[i],position)
            return (position)



    def read(self, position, column):
        page = column
        schemaPage = 3
        indirectionPage = 0
        bit = self.page[schemaPage].read(position)
        bit = bit%pow(2, self.columns-column)
        bit = bit//pow(2, self.columns-1-column) 
        if bit == 1: 
            indirection = self.page[indirectionPage].read(position)
            return(False, indirection)
        return(True, self.page[page].read(position))#returns: value at the position

    def has_capacity(self):
        if((self.counter == 15 )and(self.page[15].has_capacity == False)): # to check whether there is 16 basepage and all of them are full
            return False
        else:
            return True


# x0 = BasePage(5)
# for i in range(1, 10):
    # arr = [i, i, i, i, i]
    # print(x0.write(arr, None))
# x0.write([0, 0, 0, 0, 0], 5)
# for i in range(1, 10):
    # print(x0.read(i, 0))