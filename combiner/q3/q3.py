from mrjob.job import MRJob

class Q3(MRJob):
    def mapper(self, _, line):
        f = line.rstrip("\n").split("\t")
        if not f or f[0].strip().lower() == "invoiceno":
            return
        if len(f) < 8:
            return
        if f[5].strip() == "":
            return
        stock = f[1]
        price = float(f[5])
        yield stock, price

    def combiner(self, stock, values):
        count = 0
        total = 0
        for v in values:
            total += v
            count += 1
        avg = total / count
        yield stock, avg

    def reducer(self, stock, values):
        count = 0
        total = 0
        for v in values:
            total += v
            count += 1
        avg = total / count
        yield stock, round(avg, 2)

if __name__ == "__main__":
    Q3.run()



