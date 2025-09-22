from mrjob.job import MRJob

class Q3WordCountFilter(MRJob):
    def mapper(self, _, line):
        for word in line.split():
            yield word.lower(), 1

    def reducer(self, key, values):
        total = sum(values)
        vowels = sum(c in "aeiou" for c in key)
        if total >= vowels:
            yield key, total

if __name__ == '__main__':
    Q3WordCountFilter.run()


