from mrjob.job import MRJob

class Q2(MRJob):

    def mapper(self, _, line):
        if line.startswith("InvoiceNo"):
            return
        parts = line.strip().split("\t")
        if len(parts) < 8:
            return
        country, desc, qty = parts[7], parts[2].lower(), parts[3]
        try:
            qty = int(qty)
        except:
            return
        for w in set(desc.split()):
            yield (country, w), qty

    def reducer(self, key, vals):
        yield key, sum(vals)

if __name__ == "__main__":
    Q2.run()

