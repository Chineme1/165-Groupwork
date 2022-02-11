from lstore.db import Database
from lstore.query import Query

from random import choice, randint, sample, seed

db = Database()
# Create a table  with 5 columns
#   Student Id and 4 grades
#   The first argument is name of the table
#   The second argument is the number of columns
#   The third argument is determining the which columns will be primay key
#       Here the first column would be student id and primary key
grades_table = db.create_table('Grades', 5, 0)

# create a query class for the grades table
query = Query(grades_table)

# dictionary for records to test the database: test directory
records = {}

number_of_records = 1000
number_of_aggregates = 100

for i in range(0, number_of_records):
    key = i

    records[key] = [key, 1, 2, 3, 4]
    query.insert(*records[key])
    # print('inserted', records[key])
print("Insert finished")

keys = sorted(list(records.keys()))
# aggregate on every column 
for c in range(0, 1):
    for i in range(0, number_of_aggregates):
        r = sorted(sample(range(0, len(keys)), 2))
        # calculate the sum form test directory
        column_sum = sum(map(lambda key: records[key][c], keys[r[0]: r[1] + 1]))
        result = query.sum(keys[r[0]], keys[r[1]], c)
        if column_sum != result:
            print('sum error on [', keys[r[0]], ',', keys[r[1]], ']: ', result, ', correct: ', column_sum)
        else:
            pass
            # print('sum on [', keys[r[0]], ',', keys[r[1]], ']: ', column_sum)
