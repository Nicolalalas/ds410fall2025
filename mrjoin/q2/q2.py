from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import RawProtocol

def _num_ok(x):
    x = x.strip().replace(",", "")
    if x == "":
        return False
    y = x.replace(".", "", 1)
    return y.isdigit() or (y.startswith("-") and y[1:].replace(".", "", 1).isdigit())

class MRTotalItemsPerCountry(MRJob):
    OUTPUT_PROTOCOL = RawProtocol

    def mapper_step1(self, _, line):
        fields = (line.rstrip("\n").split("\t")
                  if "\t" in line else line.rstrip("\n").split(","))
        if len(fields) < 8 or fields[0] == "InvoiceNo":
            return
        invoice_no = fields[0].strip()
        qty_s = fields[3].strip()
        country = fields[7].strip()
        if invoice_no == "" or country == "" or not _num_ok(qty_s):
            return
        quantity = int(float(qty_s))
        yield invoice_no, (country, quantity)

    def reducer_step1(self, invoice_no, country_qty_pairs):
        total = 0
        country = ""
        for c, q in country_qty_pairs:
            country = c
            total += q
        yield country, total

    def mapper_step2(self, country, invoice_total):
        yield country, invoice_total

    def reducer_step2(self, country, totals):
        yield str(country), str(sum(totals))

    def steps(self):
        return [
            MRStep(mapper=self.mapper_step1, reducer=self.reducer_step1),
            MRStep(mapper=self.mapper_step2, reducer=self.reducer_step2),
        ]

if __name__ == "__main__":
    MRTotalItemsPerCountry.run()

