from mrjob.job import MRJob
from mrjob.step import MRStep
import re

#take only characters, no punctuation, 
#and split it up
WORD_REGEXP = re.compile(r"[\w']+")

class WordCounter(MRJob):
    def steps(self):
        return [
            MRStep(mapper = self.mapper_getwordoccurences, reducer = self.reducer_sum),
            MRStep(mapper = self.mapper_flip, reducer = self.reducer_final)
            ]
    
    def mapper_getwordoccurences(self, _ , line):
        #words = line.split();
        words = WORD_REGEXP.findall(line)
        for word in words:
            yield word.lower(), 1
            
    def reducer_sum(self, word, occurences):
        yield word, sum(occurences)
        
    def mapper_flip(self, word, frequency):
        yield '{0:05d}'.format(frequency), word
        
    def reducer_final(self, frequency_string, words):
        for word in words :
            yield word, int(frequency_string)
        
if __name__ == '__main__':
    WordCounter.run()