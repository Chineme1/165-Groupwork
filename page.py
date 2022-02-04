import config
import sys

#assuming that every column is either a string or int of some predetermined max size
#blank space is left at the start for ints and a the end for strings
#fails if value is neither an int nor string
class Page:

    def __init__(self, size):
        self.num_records = 0
        self.data_size = size
        self.data = bytearray(config.page_size) #constant page size, can change to number of bytes for easier debug

    def has_capacity(self): #haven't fully tested
        if (config.page_size/self.data_size >= self.num_records):
            return(True)
        return(False)

    def write(self, value):
        type_ = type(value);
        if (type_ == int):
            try:
                temp = value.to_bytes(self.data_size, 'big')
            except OverflowError:
                print("Value too Large")
                return(False)
            count = self.data_size-1
            while(count >= 0):
                self.data[self.num_records*self.data_size + count] = temp[count]
                count -= 1
            self.num_records += 1
            return(True)
        elif (type_ == str):
            tempArr = bytearray(value, 'utf-8')
            size = sys.getsizeof(tempArr)
            print(size)
            if (size > self.data_size+57): #no idea why but seems to return size of 57 + num characters
                print("Value too Large")
                return(False)
            else:
                count = len(tempArr)-1
                while(count >= 0):
                    self.data[self.num_records*self.data_size + count] = tempArr[count]
                    count -= 1
                self.num_records += 1
                return(True)
        else:
            print("Invalid Value Type")
            return(False)

test = Page(int(4)) # can change size of object here
#print(test.data)
i = 0
while(i < 1024):
    print(test.write(4294967296)) #should work for any int or string within the size specified on line 40
    i += 1
print(test.data)


