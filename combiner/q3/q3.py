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
        yield stock, (price, 1)

    def combiner(self, stock, values):
        total_sum = 0
        total_count = 0
        for s, c in values:
            total_sum += s
            total_count += c
        yield stock, (total_sum, total_count)

    def reducer(self, stock, values):
        total_sum = 0
        total_count = 0
        for v in values:
            total_sum += v[0]
            total_count += v[1]
        avg = total_sum / total_count
        yield stock, round(avg, 2)

if __name__ == "__main__":
    Q3.run()


