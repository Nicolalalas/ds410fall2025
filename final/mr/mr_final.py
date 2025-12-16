from mrjob.job import MRJob
from mrjob.step import MRStep

class City(MRJob):

    def mapper_one(self, key, line):
        if line.startswith("City\t"):
            return
        parts = line.split("\t")
        if len(parts) != 7:
            return
        city = parts[0].strip()
        county = parts[3].strip()
        yield (city, county), 1

    def combiner_one(self, key, values):
        yield key, sum(values)


    def reducer_one(self, key, valuelist):
        mystery = sum(valuelist)
        if mystery % 3 == 0:
            return
        yield key, mystery


    def mapper_two(self, key, value):  
        (city, county) = key
        yield city, (1, value)

    def combiner_two(self, city, values):
        riddle = 0
        enigma = 0
        for r, e in values:
            riddle += r
            enigma += e
        yield city, (riddle, enigma)


    def reducer_two(self, key, valuelist):
        riddle = 0
        enigma = 0
        for (r, e) in valuelist:
            riddle = riddle + r
            enigma = enigma + e
        if riddle != enigma:
            yield key, (riddle, enigma)


    def steps(self):
        return [
            MRStep(
                mapper=self.mapper_one,
                combiner=self.combiner_one,
                reducer=self.reducer_one
            ),
            MRStep(
                mapper=self.mapper_two,
                combiner=self.combiner_two,
                reducer=self.reducer_two
            )
        ]


if __name__ == '__main__':
    City.run()  # if you don't have these two lines, your code will not do anything

