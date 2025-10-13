from mrjob.job import MRJob
from mrjob.step import MRStep

class BigOrderCount(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper, reducer=self.reducer_invoice_total),
            MRStep(reducer=self.reducer_count_big)
        ]

    def mapper(self, _, line):
        f = line.rstrip('\n').split('\t')
        if len(f) < 7 or f[0] == 'InvoiceNo':
            return
        yield f[0], (f[6], int(float(f[3])))

    def reducer_invoice_total(self, invoice_no, values):
        cid, s = '', 0
        for c, q in values:
            cid, s = c, s + q
        yield cid, s

    def reducer_count_big(self, cid, totals):
        c = 0
        for t in totals:
            if t >= 10:
                c += 1
        yield str(cid), str(c)

if __name__ == '__main__':
    BigOrderCount.run()

