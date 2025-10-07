from mrjob.job import MRJob

class Q3(MRJob):
    def mapper(self, _, line):
        fields = line.strip().split("\t")
        if fields[0] == "InvoiceNo" or len(fields) < 8:
            return
        stock = fields[1]
        price = float(fields[5])
        yield stock, price

    def combiner(self, stock, prices):
        total = 0
        count = 0
        for p in prices:
            total += p
            count += 1
        yield stock, total / count

    def reducer(self, stock, prices):
        total = 0
        count = 0
        for p in prices:
            total += p
            count += 1
        yield stock, round(total / count, 2)


if __name__ == "__main__":
    Q3.run()




