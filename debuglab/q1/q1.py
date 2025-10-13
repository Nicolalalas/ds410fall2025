from mrjob.job import MRJob

class CityStats(MRJob):
    def mapper(self, _, line):
        # 允许空行，允许 header
        if not line:
            return
        # 以 TAB 为主（/datasets/cities 是 TSV）
        fields = line.strip().split("\t")
        if len(fields) < 6:
            return
        if fields[0].lower() == "name":   # 跳过表头
            return

        state = fields[1].strip()

        # 人口字段（第4列）应为整数
        pop_field = fields[3].strip().replace(",", "")
        if not pop_field or not pop_field.isdigit():
            return
        population = int(pop_field)

        # zipcode 列（第5列），按逗号切
        zips_raw = fields[4]
        zips = [z.strip() for z in zips_raw.split(",") if z.strip()]
        zip_count = len(zips)

        # DebugLab 常规：不过度过滤，保留正常记录
        if population >= 0 and zip_count >= 1:
            # 交给 reducer 统计平均人口与平均 zip 数
            yield state, (population, zip_count)

    def reducer(self, state, values):
        total_pop = 0.0
        total_zip = 0.0
        n = 0
        for p, zc in values:
            total_pop += p
            total_zip += zc
            n += 1
        if n > 0:
            avg_pop = round(total_pop / n, 2)
            avg_zip = round(total_zip / n, 2)
            # 用 list 与 expected_output.txt 的方括号风格一致
            yield state, [avg_pop, avg_zip]

if __name__ == "__main__":
    CityStats.run()




