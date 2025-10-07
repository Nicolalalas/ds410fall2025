from mrjob.job import MRJob
from fractions import Fraction

class Q3(MRJob):
    def mapper(self, _, line):
        if line.startswith("InvoiceNo"):
            return
        f = line.strip().split("\t")
        if len(f) < 8:
            return
        stock = f[1]
        price_pennies = int(Fraction(f[5]) * 100)
        yield stock, (price_pennies, 1)

    def combiner(self, stock, values):
        total = 0
        count = 0
        for p, c in values:
            total += p
            count += c
        yield stock, (total, count)

    def reducer(self, stock, values):
        total = 0
        count = 0
        for p, c in values:
            total += p
            count += c
        avg_dollars = round(total / count / 100.0, 2)
        yield stock, avg_dollars


if __name__ == "__main__":
    Q3.run()





