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

for i in range(0, number_of_records):
    key = i

    records[key] = [key, 1, 2, 3, 4]
    query.insert(*records[key])
    # print('inserted', records[key])
print("Insert finished")

for i in range(0, number_of_records):
    query.delete(i)
    # print('deleted', i)
print("Delete finished")
