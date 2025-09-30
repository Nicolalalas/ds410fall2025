from mrjob.job import MRJob

class MRVowelWordsWithCombiner(MRJob):
    def mapper(self, _, line):
        for word in line.split():
            word = word.lower()
            vowels = sum(1 for c in word if c in "aeiou")
            if vowels >= 2:
                yield word, 1

    def combiner(self, word, counts):
        yield word, sum(counts)

    def reducer(self, word, counts):
        yield word, sum(counts)

if __name__ == "__main__":
    MRVowelWordsWithCombiner.run()

