from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import RawProtocol

def _num_ok(x):
    x = x.strip().replace(",", "")
    if x == "":
        return False
    y = x.replace(".", "", 1)
    return y.isdigit() or (y.startswith("-") and y[1:].replace(".", "", 1).isdigit())

class MRItemsAndCustomersPerCountry(MRJob):
    OUTPUT_PROTOCOL = RawProtocol

    def mapper_step1(self, _, line):
        fields = (line.rstrip("\n").split("\t")
                  if "\t" in line else line.rstrip("\n").split(","))
        if len(fields) < 8 or fields[0] == "InvoiceNo":
            return
        invoice_no = fields[0].strip()
        qty_s = fields[3].strip()
        customer_id = fields[6].strip()
        country = fields[7].strip()
        if invoice_no == "" or country == "" or not _num_ok(qty_s):
            return
        quantity = int(float(qty_s))
        yield invoice_no, (country, customer_id, quantity)

    def reducer_step1(self, invoice_no, rows):
        total = 0
        country = ""
        customer_id = ""
        for cty, cid, q in rows:
            country = cty
            customer_id = cid
            total += q
        yield country, (customer_id, total)

    def mapper_step2(self, country, cid_total):
        cid, total = cid_total
        yield country, ("Q", total)
        yield country, ("C", cid)

    def reducer_step2(self, country, tagged_vals):
        s = 0
        seen = set()
        for tag, v in tagged_vals:
            if tag == "Q":
                s += v
            else:
                seen.add(v)
        out = f"[{s},  {len(seen)}]"
        yield str(country), out

    def steps(self):
        return [
            MRStep(mapper=self.mapper_step1, reducer=self.reducer_step1),
            MRStep(mapper=self.mapper_step2, reducer=self.reducer_step2),
        ]

if __name__ == "__main__":
    MRItemsAndCustomersPerCountry.run()

