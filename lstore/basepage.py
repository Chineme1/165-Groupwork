from lstore.page import Page

class BasePage:
    def __init__(self,columns):
        self.columns = columns
        self.page = []#Physiical holder
        self.counter = 0#total number of pages
        self.updates = 0
        for i in range(0, self.columns+4):
            x0 = Page()
            self.page.append(x0)


    def write(self, columns, position):
        if position == None:                                #write to the end 
            ret = self.counter
            for i in range(self.columns):
                self.page[i].write(columns[i],None)
            self.counter += 1
            return(ret)
        else:
            position = self.counter%512                     #write to the position
            for i in range(self.columns):
                self.page[i].write(columns[i],position)
            self.counter += 1
            return (True)


    def read(self, position, column):
        page = column
        schemaPage = 3
        indirectionPage = 0
        bit = self.page[schemaPage].read(position)
        bit = bit%pow(2, 9-column)
        bit = bit//pow(2, 8-column) 
        if bit == 1: 
            indirection = self.page[indirectionPage].read(position)
            return(False, indirection)
        return(True, self.page[page].read(position))#returns: value at the position

    def has_capacity(self):
        if((self.counter == 15 )and(self.page[15].has_capacity == False)): # to check whether there is 16 basepage and all of them are full
            return False
        else:
            return True
