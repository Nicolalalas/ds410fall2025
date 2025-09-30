from mrjob.job import MRJob

class Q1CaseInsensitiveWordCountWithCombiner(MRJob):

    def mapper(self, _, line):
        for word in line.split():
            yield word.lower(), 1

    def combiner(self, word, counts):
        yield word, sum(counts)

    def reducer(self, word, counts):
        yield word, sum(counts)

if __name__ == '__main__':
    Q1CaseInsensitiveWordCountWithCombiner.run()

