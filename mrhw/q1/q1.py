from mrjob.job import MRJob
from fractions import Fraction

class Q1(MRJob):
    def mapper(self, _, line):
        f = line.rstrip("\n").split("\t")
        if not f or f[0].strip().lower() == "invoiceno":
            return
        if len(f) < 8:
            return
        try:
            stock = f[1]
            price_cent = int(Fraction(f[5]) * 100)
        except Exception:
            return
        yield stock, price_cent

    def reducer(self, stock, cents_iter):
        cents = list(cents_iter)
        avg = sum(cents) / len(cents)
        yield stock, round(avg / 100, 2)

if __name__ == "__main__":
    Q1.run()



