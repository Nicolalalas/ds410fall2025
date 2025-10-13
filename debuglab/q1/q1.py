from mrjob.job import MRJob

class CityStats(MRJob):
    def mapper(self, _, line):
        fields = line.strip().split("\t")
        if len(fields) < 6 or fields[0] == "name":
            return
        state = fields[1].strip()
        population = int(fields[3])
        zipcodes = fields[4].split(",")
        zip_count = len([z for z in zipcodes if z.strip() != ""])
        if population >= 100 and zip_count >= 2:
            yield state, (population, zip_count)

    def reducer(self, state, values):
        total_pop = 0
        total_zips = 0
        count = 0
        for p, z in values:
            total_pop += p
            total_zips += z
            count += 1
        avg_pop = total_pop / count
        avg_zip = total_zips / count
        yield state, [round(avg_pop, 2), round(avg_zip, 2)]

if __name__ == "__main__":
    CityStats.run()


