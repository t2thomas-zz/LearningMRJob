from mrjob.job import MRJob
from mrjob.step import MRStep
import re

#take only characters, no punctuation, 
#and split it up
WORD_REGEXP = re.compile(r"[\w']+")

class WordCounterCombiner(MRJob):
    
    def mapper(self, _ , line):
        #words = line.split();
        words = WORD_REGEXP.findall(line)
        for word in words:
            yield word.lower(), 1
            
    def combiner(self, word, occurences):
        yield word, sum(occurences)
        
    def reducer(self, word, occurences):
        yield word, sum(occurences)
        
if __name__ == '__main__':
    WordCounterCombiner.run()