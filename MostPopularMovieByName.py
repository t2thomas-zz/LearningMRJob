from mrjob.job import MRJob
from mrjob.step import MRStep

class MostPopularMovieByName(MRJob):
    def configure_options(self):
        super(MostPopularMovieByName, self).configure_options()
        self.add_file_option('--items', help = 'Path to u.item')
        
    def steps(self):
        return [MRStep(mapper = self.mapper_extract,
                       reducer_init = self.reducer_init,
                       reducer = self.reducer_combine),
                MRStep(reducer = self.reducer_find_max)
                ]
        
    def mapper_extract(self,_,line):
        _, movie_id, _, _ = line.split('\t')
        yield movie_id, 1
        
    def reducer_init(self):
        self.movie_names = {}
        with open("u.item") as f:
            for line in f:
                fields = line.split('|')
                self.movie_names[fields[0]] = fields[1]
                
    def reducer_combine(self, movie_id, occurences):
        yield None,(sum(occurences), self.movie_names[movie_id])
        
    def reducer_find_max(self,key,values):
        yield max(values)
        
        
if __name__ == "__main__" :
    MostPopularMovieByName.run()
    
#run using configure_options(MostPopularMovieByName, self)