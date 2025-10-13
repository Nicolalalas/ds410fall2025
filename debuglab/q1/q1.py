from mrjob.job import MRJob

class CityStats(MRJob):
    def mapper(self, _, line):
        fields = line.strip().split("\t")
        if len(fields) < 6 or fields[0].lower() == "name":
            return
        state = fields[1].strip()
        if not fields[3].isdigit():
            return
        population = int(fields[3])
        zipcodes = [z.strip() for z in fields[4].split(",") if z.strip()]
        zip_count = len(zipcodes)
        if population >= 100 and zip_count >= 2:
            yield state, (population, zip_count)

    def reducer(self, state, values):
        total_pop = 0
        total_zip = 0
        count = 0
        for p, z in values:
            total_pop += p
            total_zip += z
            count += 1
        if count > 0:
            avg_pop = total_pop / count
            avg_zip = total_zip / count
            yield state, (round(avg_pop, 2), round(avg_zip, 2))

if __name__ == "__main__":
    CityStats.run()





