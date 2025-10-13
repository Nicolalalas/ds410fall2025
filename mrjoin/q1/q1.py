from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import RawProtocol

class Job(MRJob):
    OUTPUT_PROTOCOL = RawProtocol

    def m1(self, _, line):
        f = line.rstrip('\n').split('\t')
        if len(f) < 7 or f[0] == 'InvoiceNo': return
        yield f[0], (f[6], int(float(f[3])))

    def r1(self, inv, vals):
        c, s = '', 0
        for cid, q in vals:
            c, s = cid, s + q
        yield c, s

    def m2(self, cid, total):
        yield cid, 1 if total >= 10 else 0

    def r2(self, cid, flags):
        yield str(cid), str(sum(flags))

    def steps(self):
        return [MRStep(mapper=self.m1, reducer=self.r1),
                MRStep(mapper=self.m2, reducer=self.r2)]

if __name__ == '__main__':
    Job.run()

