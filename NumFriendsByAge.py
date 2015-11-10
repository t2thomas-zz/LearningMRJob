from mrjob.job import MRJob

class NumFriendsByAge(MRJob):
    def mapper(self, key, line):
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