from mrjob.job import MRJob

class Q4(MRJob):
    def mapper(self, _, line):
        fields = line.strip().split("\t")
        if fields[0] == "InvoiceNo" or len(fields) < 8:
            return
        country = fields[7]
        description = fields[2]
        quantity = int(fields[3])
        for w in set(description.split()):
            yield (country, w), quantity

    def combiner(self, key, values):
        yield key, sum(values)

    def reducer(self, key, values):
        yield key, sum(values)

if __name__ == "__main__":
    Q4.run()

