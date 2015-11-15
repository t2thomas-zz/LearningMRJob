from mrjob.job import MRJob
from mrjob.step import MRStep

class MostRatedMovie(MRJob):
    
    def steps(self):
        return [
            MRStep(mapper = self.mapper_getcount, reducer = self.reducer_group),
            MRStep( reducer = self.reducer_findmax)
            ]
            
    def mapper_getcount(self, _, line):
        (_, movieid, _ ,_) = line.split('\t')
        yield movieid,1
        
    def reducer_group(self, movieid, occurences):
        yield None, (sum(occurences), movieid)
        
    def reducer_findmax(self, key, value):
        #It is good to know that python sorts by first element of
        #a tuple, hence we get what we want here
        yield max(value) 
        
if __name__ == '__main__' :
    MostRatedMovie.run()
          
        