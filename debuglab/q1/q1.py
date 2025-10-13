from mrjob.job import MRJob

class CityStats(MRJob):
    def mapper(self, _, line):
        fields = line.strip().split()
        if fields[0] == "name":
            return
        state = fields[1]
        population = float(fields[3])
        zipcodes = fields[4].split(",")
        yield state, (population, len(zipcodes))

    def reducer(self, state, values):
        total_pop = 0
        total_cities = 0
        zip_counts = []
        for pop, zcount in values:
            total_pop += pop
            total_cities += 1
            zip_counts.append(zcount)
        avg_pop = total_pop / total_cities
        max_zip = max(zip_counts)
        yield state, (avg_pop, float(max_zip))

if __name__ == '__main__':
    CityStats.run()


