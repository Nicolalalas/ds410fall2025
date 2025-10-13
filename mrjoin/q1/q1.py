from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import RawProtocol

def _num_ok(x):
    x = x.strip().replace(",", "")
    if x == "":
        return False
    y = x.replace(".", "", 1)
    return y.isdigit() or (y.startswith("-") and y[1:].replace(".", "", 1).isdigit())

class MRBigOrdersPerCustomer(MRJob):
    OUTPUT_PROTOCOL = RawProtocol

    def mapper_step1(self, _, line):
        fields = (line.rstrip("\n").split("\t")
                  if "\t" in line else line.rstrip("\n").split(","))
        if len(fields) < 8:
            return
        if fields[0] == "InvoiceNo":
            return
        invoice_no = fields[0].strip()
        qty_s = fields[3].strip()
        customer_id = fields[6].strip()
        if invoice_no == "" or not _num_ok(qty_s):
            return
        quantity = int(float(qty_s))
        yield invoice_no, (customer_id, quantity)

    def reducer_step1(self, invoice_no, cust_qty_pairs):
        total = 0
        cid = ""
        for c, q in cust_qty_pairs:
            cid = c
            total += q
        yield cid, total

    def mapper_step2(self, customer_id, invoice_total_qty):
        yield customer_id, 1 if invoice_total_qty >= 10 else 0

    def reducer_step2(self, customer_id, flags):
        yield str(customer_id), str(sum(flags))

    def steps(self):
        return [
            MRStep(mapper=self.mapper_step1, reducer=self.reducer_step1),
            MRStep(mapper=self.mapper_step2, reducer=self.reducer_step2),
        ]

if __name__ == "__main__":
    MRBigOrdersPerCustomer.run()

