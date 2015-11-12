from mrjob.job import MRJob
import re

#take only characters, no punctuation, 
#and split it up
WORD_REGEXP = re.compile(r"[\w']+")

class WordCounter(MRJob):
    def mapper(self, _ , line):
        #words = line.split();
        words = WORD_REGEXP.findall(line)
        for word in words:
            yield word.lower(), 1
            
    def reducer(self, word, occurences):
        yield word, sum(occurences)
        
if __name__ == '__main__':
    WordCounter.run()