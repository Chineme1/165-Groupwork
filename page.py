
class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(4096)

    def has_capacity(self):
        if self.num_records == 512:
            return False
        else:
            return True

    def write(self, value):
        if self.has_capacity:
            self.num_records[self.num_records*8 : self.num_records*8+7] = value
            self.num_records += 1
        else:
            return False

class PageRange:
    def __init__(self):
        self.TP =1
        self.BP = 0
        self.hold = [None*14]
    
    def I_TP(self):
        x1 = Page()
        self.hold.append(x1)
        self.TP +=1

    def I_BP(self):
        if self.has_capacity:
            np= Page()
            self.hold[self.BP]=np
            self.BP +=1
        else:
            return False

    def has_capacity(self):
        if self.BP == 13:
            return False
        else:
            return True



