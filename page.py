


class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(4096)

    def has_capacity(self):
        if self.num_records == 512 : #4096 bytes divided by the 8 bytes == 512
            return False
        else:
            return True

    def read(self, position):
        position = position-1 #as RID 0 is reserved but we want to use the 0th position of the array
        arr =  self.data[position*8 : position*8 + 8]
        num = 0
        num = int.from_bytes(arr, 'big')
        return(num)

    def write(self, value, position):
        if self.has_capacity():
            if position == None: #if doesnt give position, add to end
               position = self.num_records 
               self.num_records += 1  
            else:
                position = position - 1
            arr = value.to_bytes(8, 'big')
            self.data[position*8 : position*8+8] = arr 
            return(position+1) #as RID = position+1
        else:
            print("page is full")
            return False

        
    def pagePrint(self):
        print(self.data)

    


# x0 = Page()
# for i in range(1, 10):
    # print(x0.write(i, None))
# x0.write(5, 1)
# for i in range(1, 10):
    # print(x0.read(i))
