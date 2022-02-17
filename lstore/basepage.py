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


    def write(self,value, column, position):
        page = column
        if position == None:                                #write to the end 
            ret = self.counter
            if self.updates%(self.columns+4) == self.columns+3:
                self.counter += 1
                self.updates == 0
            self.updates +=1
            self.page[page].write(value,None)
            return(ret)
        else:
            position = self.counter%512                     #write to the position
            self.page[page].write(value, position)
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
