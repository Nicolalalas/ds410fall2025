from mrjob.job import MRJob

class UninformativeClassName(MRJob):

    def mapper(self, _, line):
        fields = line.strip().split()
        if fields[0] == "name":
            return
        city = fields[1]
        thingy = fields[2]
        population = float(fields[3])
        zipcodes = fields[4].split(",")
        yield thingy, (population, len(zipcodes))

    def reducer(self, state, values):
        total_pop = 0
        total_city = 0
        max_zip = 0
        for pop, z in values:
            total_pop += pop
            total_city += 1
            if z > max_zip:
                max_zip = z
        avg_pop = total_pop / total_city
        yield state, (avg_pop, float(max_zip))

if __name__ == '__main__':
    UninformativeClassName.run()



