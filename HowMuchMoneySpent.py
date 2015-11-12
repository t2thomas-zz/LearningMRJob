from mrjob.job import MRJob
from mrjob.step import MRStep

class HowMuchMoneySpent(MRJob):
    def steps(self):
        return [
        MRStep(mapper = self.mapper_getamtspent, reducer = self.reducer_gettotal),
        MRStep(mapper = self.mapper_flipforsort, reducer = self.reducer_final)
        ]
    
    def mapper_getamtspent(self, _, line):
        (userid, _ , amt_spent) = line.split(',')
        yield userid, float(amt_spent)
        
    def reducer_gettotal(self, userid, amt_spent):
        yield userid, sum(amt_spent)
        
    def mapper_flipforsort(self, userid, totalamtspent):
        yield '{0:08.2f}'.format(totalamtspent), userid
        
    def reducer_final(self, total, users):
        for userid in users :
            yield userid, float(total)
        
        
if __name__ == '__main__' :
    HowMuchMoneySpent.run()
    