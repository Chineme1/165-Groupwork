import config
import sys

#assuming that every column is either a string or int of some predetermined max size
class Page:

    def __init__(self, size):
        self.num_records = 0
        self.data_size = size
        self.data = bytearray(config.page_size)

    def has_capacity(self): #haven't fully tested
        if (config.page_size/self.data_size >= self.num_records):
            return(True)
        return(False)

    def write(self, value):
        type_ = type(value);
        if (type_ == int):
            temp = str(value)
            tempArr = bytearray(temp)
        elif (type_ == str):
            tempArr = bytearray(value)
        else:
            print("Invalid Value Type")
            return(False)
        size = sys.getsizeof(tempArr)
        print(size)
        if (size > self.data_size+49): #no idea why but seems to return size of 49 + num characters
            print("Value too Large")
            return(False)
        else:
            count = len(tempArr)-1
            while(count >= 0):
                self.data[self.num_records*self.data_size + count] = tempArr[count]
                count -= 1
            self.num_records += 1
            return(True)

test = Page(int(5)) # can change size here
print(test.data)
i = 0
while(i < 10):
    print(test.write(1611)) #should work for any int or string within the size specified on line 40
    i += 1
print(test.data)


