import os
from .PageRange import PageRange
import time
class TxT:
    def __init__(self,num_columns, path):
        self.num_columns = num_columns
        self.path = path


    def readPageRange(self, num_columns,index,key):
        output = PageRange(num_columns, index, key)
        path = self.path + '/%s.txt'%index
        with open(path, "rb") as f:
            while(1):
                columns = []
                for i in range(0, self.num_columns+4):

                    line = f.read(8)
                    if not line:
                        return(output)
                    if i == 0 or i == 3:
                        line = '0'
                        columns.append(int(line))
                    else:
                        columns.append(int.from_bytes(line, 'big'))
                output.BaseWrite(columns, None)
        return(output)

    def writePageRange(self, pageRange):
        index = pageRange.pr_key 
        path = self.path + "/%s.txt"%index
        if os.path.exists(path): 
            t = time.time()
            os.rename(path, self.path + '/%s'%index + 'Time%s.txt'%t)
        with open(path, 'wb') as txt_file:
            string_array = ""
            for a in range(1,pageRange.num_base_record+1):

                for i in range(0,pageRange.num_columns):

                    intvalue = pageRange.BaseRead(a,i)
                    txt_file.write(intvalue.to_bytes(8, 'big'))
        return False


# pageRange = PageRange(5,0,1)
# for i in range(1, 10):
    # arr = [0, i, 0, 0, i]
    # pageRange.BaseWrite(arr, None)
# Csv = Csv(5, 0, 1)
# Csv.writePageRange(pageRange)
# print("inserted")
# print(Csv.readPageRange(5,0,1).BaseRead(1, 1))