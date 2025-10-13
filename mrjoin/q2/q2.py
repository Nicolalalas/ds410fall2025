from mrjob.job import MRJob
from mrjob.step import MRStep

class CountryTotalItems(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper, reducer=self.reducer_join),
            MRStep(reducer=self.reducer_sum)
        ]

    def mapper(self, _, line):
        fields = line.rstrip("\n").split("\t")
        if not fields:
            return

        # customers.txt -> CustomerID\tCountry
        if len(fields) == 2 and fields[0] != "CustomerID":
            yield fields[0].strip(), ("country", fields[1].strip())

        # orders.txt -> InvoiceNo\tStockCode\tQuantity\tInvoiceDate\tCustomerID
        elif len(fields) == 5 and fields[0] != "InvoiceNo":
            customer_id = fields[4].strip()
            qty_str = fields[2].strip()
            if qty_str != "" and qty_str.strip("-").replace(".", "", 1).isdigit():
                quantity = int(float(qty_str))
                yield customer_id, ("order", quantity)

    def reducer_join(self, customer_id, values):
        country = None
        total_qty = 0
        for data_type, value in values:
            if data_type == "country":
                country = value
            else:
                total_qty += value
        if country:
            yield country, total_qty

    def reducer_sum(self, country, quantities):
        yield str(country), sum(quantities)

if __name__ == "__main__":
    CountryTotalItems.run()

