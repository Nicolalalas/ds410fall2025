from mrjob.job import MRJob

class Q2(MRJob):
    def mapper(self, _, line):
        parts = line.strip().split("\t")
        if not parts or parts[0].lower() == "invoiceno":
            return
        if len(parts) < 8:
            return

        country = parts[7].strip()
        desc = parts[2].lower()
        qty_str = parts[3].strip()
        if not qty_str.lstrip("-").isdigit():
            return
        qty = int(qty_str)
        if qty <= 0:
            return

        for w in set(desc.split()):
            yield (country, w), qty

    def reducer(self, key, vals):
        yield key, sum(vals)

if __name__ == "__main__":
    Q2.run()



