from mrjob.job import MRJob 

class NumMoviesUsersWatch(MRJob):
    def mapper(self, key, line):
        (userID, movieID, rating, timestamp) = line.split('\t')
        yield userID, movieID
        
    def reducer(self,userID, movieID):
        count = 0
        for x in movieID:
            count += 1
        yield userID, count
        
if __name__ == '__main__':
    NumMoviesUsersWatch.run()