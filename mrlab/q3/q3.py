from mrjob.job import MRJob
from mrjob.step import MRStep
import re

WORD_RE = re.compile(r"[A-Za-z]+")
VOWEL_RE = re.compile(r"[aeiou]", re.IGNORECASE)

def vowel_count(word):
    return len(VOWEL_RE.findall(word))

class Q3WordVowelFilteredCount(MRJob):
    def steps(self):
        return [MRStep(mapper=self.mapper_get_words,
                       combiner=self.combiner_count_words,
                       reducer=self.reducer_filter_sum)]

    def mapper_get_words(self, _, line):
        for word in WORD_RE.findall(line):
            if vowel_count(word) >= 2:
                yield word.lower(), 1

    def combiner_count_words(self, word, counts):
        yield word, sum(counts)

    def reducer_filter_sum(self, word, counts):
        total = sum(counts)
        if total >= vowel_count(word):
            yield word, total

if __name__ == "__main__":
    Q3WordVowelFilteredCount.run()

