from mrjob.job import MRJob
from mrjob.step import MRStep

class MRNoITECustomers(MRJob):

    def steps(self):
        return [
            MRStep(
                mapper=self.mapper_get_customer,
                combiner=self.combiner_sum_counts,
                reducer=self.reducer_filter_customers
            )
        ]

    def mapper_get_customer(self, _, line):
        fields = line.strip().split('\t')
        if fields[0] == "InvoiceNo" or len(fields) < 8:
            return
        description = fields[2].upper().strip()
        customer_id = fields[6].strip()

        if customer_id:
            has_ite = "ITE" in description
            yield customer_id, (1, has_ite)

    def combiner_sum_counts(self, customer_id, values):
        total_lines = 0
        found_ite = False

        for cnt, has_ite in values:
            total_lines += cnt
            if has_ite:
                found_ite = True

        yield customer_id, (total_lines, found_ite)

    def reducer_filter_customers(self, customer_id, values):
        total_lines = 0
        found_ite = False

        for cnt, has_ite in values:
            total_lines += cnt
            if has_ite:
                found_ite = True

        if not found_ite:
            yield customer_id, total_lines


if __name__ == '__main__':
    MRNoITECustomers.run()

