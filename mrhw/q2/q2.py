from mrjob.job import MRJob

class Q2(MRJob):
    def mapper(self, _, line):
        fields = line.strip().split("\t")
        if fields[0] == "InvoiceNo" or len(fields) < 8:
            return
        country = fields[7]
        quantity = int(fields[3])
        description = fields[2]
        for w in set(description.split()):
            yield (country, w), quantity

    def reducer(self, key, values):
        yield key, sum(values)

if __name__ == "__main__":
    Q2.run()




