from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import RawProtocol

# 只接受整数(可带负号)；不做 float/逗号等花活，避免误判
def _parse_int(s):
    s = s.strip()
    if s.startswith("-"):
        t = s[1:]
        if t.isdigit():
            return -int(t)
        return None
    return int(s) if s.isdigit() else None

class MRBigOrdersPerCustomer(MRJob):
    OUTPUT_PROTOCOL = RawProtocol

    # Step 1: 按发票号聚合数量
    def mapper_step1(self, _, line):
        # 强制按 TSV 解析（课程 orders 数据是制表符分隔）
        fields = line.rstrip("\n").split("\t")
        if len(fields) < 7:
            return
        # 更稳的表头判断
        if fields[0].strip().lower().startswith("invoiceno"):
            return

        invoice_no = fields[0].strip()
        qty = _parse_int(fields[3])
        customer_id = fields[6].strip()

        if qty is None:
            return  # 只有数量完全非整数才丢弃
        # 不强制 invoice_no/customer_id 非空；题面保证同一发票 customer 不变
        yield invoice_no, (customer_id, qty)

    def reducer_step1(self, invoice_no, rows):
        total = 0
        cid = ""
        for c, q in rows:
            cid = c
            total += q
        yield cid, total

    # Step 2: 把每个发票总量转换成 0/1，再对客户求和
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

