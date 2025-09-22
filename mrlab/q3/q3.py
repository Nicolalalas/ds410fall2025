from mrjob.job import MRJob

class MRVowelWordsFilter(MRJob):
    def mapper(self, _, line):
        for word in line.split():
            w = word.lower()
            vowels = sum(1 for c in w if c in "aeiou")
            if vowels >= 2:
                yield w, 1

    def reducer(self, word, counts):
        total = sum(counts)
        vowels = sum(1 for c in word if c in "aeiou")
        if total >= vowels:
            yield word, total

if __name__ == "__main__":
    MRVowelWordsFilter.run()



