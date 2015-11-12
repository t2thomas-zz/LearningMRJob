# This is the library that will do the map and reduce
from mrjob.job import MRJob

#The class that does the task
class NumFriendsByAge(MRJob):
    
    def mapper(self, _, line):
        (userID, name, age, numFriends) = line.split(',')
        yield age, float(numFriends)
        
    def reducer(self, age, numFriends):
        total = 0
        numElements = 0
        for x in numFriends:
            total += x
            numElements += 1
            
        yield age, total/numElements
        
if __name__ == '__main__':
    NumFriendsByAge.run()
  
# run using: !python NumFriendsByAge.py fakefriends.csv > friendsbyage.txt
