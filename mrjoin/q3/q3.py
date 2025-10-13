from mrjob.job import MRJob
from mrjob.step import MRStep

class CountryItemsAndCustomers(MRJob):
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
        total_items = 0
        has_order = False

        for data_type, value in values:
            if data_type == "country":
                country = value
            else:
                has_order = True
                total_items += value

        if country and has_order:
            yield country, (total_items, 1)  # (items, one customer)

    def reducer_sum(self, country, pairs):
        total_items = 0
        total_customers = 0
        for items, cust_count in pairs:
            total_items += items
            total_customers += cust_count
        yield str(country), [total_items, total_customers]

if __name__ == "__main__":
    CountryItemsAndCustomers.run()

