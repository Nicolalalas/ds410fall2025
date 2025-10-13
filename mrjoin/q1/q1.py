from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import RawProtocol

class MRBigOrdersPerCustomer(MRJob):
    OUTPUT_PROTOCOL = RawProtocol  # 输出: key\tvalue

    def steps(self):
        return [
            MRStep(mapper=self.m1, reducer=self.r1),
            MRStep(reducer=self.r2)  # 第二步只用一个 reducer 就够
        ]

    # Step1: 以发票号聚合数量，并保留该发票对应的 CustomerID
    def m1(self, _, line):
        f = line.rstrip('\n').split('\t')
        if len(f) < 7 or f[0] == 'InvoiceNo':
            return
        # f[0]=InvoiceNo, f[3]=Quantity, f[6]=CustomerID
        yield f[0], (f[6], int(float(f[3])))

    def r1(self, inv, vals):
        cid, s = '', 0
        for c, q in vals:
            cid, s = c, s + q        # 同一发票的 CustomerID 一致
        yield cid, s                 # 发票总量 -> (CustomerID, total)

    # Step2: 统计该客户有多少张“≥10 件”的大订单
    def r2(self, cid, totals):
        cnt = 0
        for t in totals:
            if t >= 10:
                cnt += 1
        yield str(cid), str(cnt)

if __name__ == '__main__':
    MRBigOrdersPerCustomer.run()

