from mrjob.job import MRJob
from mrjob.step import MRStep

class CountryNumber(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper, reducer=self.reducer_country_orders),
            MRStep(reducer=self.reducer_sum_quantities),
        ]

    def mapper(self, _, line):
        fields = line.rstrip("\n").split("\t")
        if not fields:
            return

        if len(fields) == 2 and fields[0] != "CustomerID":
            yield fields[0].strip(), ("country", fields[1].strip())
            return

        if len(fields) == 5 and fields[0] != "InvoiceNo":
            invoice_no = fields[0].strip()
            qty_s = fields[2].strip()
            customer_id = fields[4].strip()
            if qty_s != "" and qty_s.strip("-").replace(".", "", 1).isdigit():
                quantity = int(float(qty_s))
                yield customer_id, ("order", (invoice_no, quantity))

    def reducer_country_orders(self, customer_id, values):
        country = None
        invoice_sums = {}
        seen_order = False

        for data_type, value in values:
            if data_type == "country":
                country = value
            else:
                seen_order = True
                inv, qty = value
                invoice_sums[inv] = invoice_sums.get(inv, 0) + qty

        if seen_order:
            big_cnt = 0
            for total in invoice_sums.values():
                if total >= 10:
                    big_cnt += 1
            yield customer_id, big_cnt

    def reducer_sum_quantities(self, key, quantities):
        yield str(key), sum(quantities)

if __name__ == "__main__":
    CountryNumber.run()

