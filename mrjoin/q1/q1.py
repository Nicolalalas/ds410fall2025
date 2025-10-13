from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import RawProtocol

def _to_int_num(s):
    s = s.strip().replace(",", "")
    if s == "":
        return None
    neg = 1
    if s[0] == "-":
        neg = -1
        s = s[1:]
    dots = s.split(".")
    if not all(part.isdigit() for part in dots if part != ""):
        return None
    try:
        return int(float("-" + s) if neg == -1 else float(s))
    except Exception:
        return None  # 不用 try/except 会更难写健壮，这里作为兜底你可删掉

class MRBigOrdersPerCustomer(MRJob):
    OUTPUT_PROTOCOL = RawProtocol

    def mapper_step1(self, _, line):
        line = line.rstrip("\n")
        fields = line.split("\t") if "\t" in line else line.split(",")
        if len(fields) < 7:   # 至少要能取到 0,3,6
            return
        if fields[0] == "InvoiceNo":
            return
        invoice_no = fields[0].strip()
        qty = _to_int_num(fields[3])
        customer_id = fields[6].strip() if len(fields) > 6 else ""
        if qty is None:
            return
        # 不再强制 invoice_no 非空；题面保证同一发票的 customer 恒定
        yield invoice_no, (customer_id, qty)

    def reducer_step1(self, invoice_no, rows):
        total = 0
        cid = ""
        for c, q in rows:
            cid = c
            total += q
        yield cid, total

    def mapper_step2(self, customer_id, invoice_total):
        yield customer_id, 1 if invoice_total >= 10 else 0

    def reducer_step2(self, customer_id, flags):
        yield str(customer_id), str(sum(flags))

    def steps(self):
        return [
            MRStep(mapper=self.mapper_step1, reducer=self.reducer_step1),
            MRStep(mapper=self.mapper_step2, reducer=self.reducer_step2),
        ]

if __name__ == "__main__":
    MRBigOrdersPerCustomer.run()

