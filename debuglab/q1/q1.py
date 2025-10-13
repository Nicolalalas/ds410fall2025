from mrjob.job import MRJob

class CityStats(MRJob):
    def mapper(self, _, line):
        fields = line.strip().split("\t")
        if len(fields) < 6 or fields[0] == "name":
            return
        state = fields[1]
        population = int(fields[3])
        zipcodes = fields[4].split(",")
        zip_count = len([z for z in zipcodes if z.strip() != ""])
        yield state, (population, zip_count)

    def reducer(self, state, values):
        total_pop = 0
        total_zip = 0
        count = 0
        for p, z in values:
            total_pop += p
            total_zip += z
            count += 1
        avg_pop = total_pop / count
        avg_zip = total_zip / count
        yield state, [round(avg_pop, 2), round(avg_zip, 2)]

if __name__ == "__main__":
    CityStats.run()



