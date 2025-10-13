from mrjob.job import MRJob
import re


def _is_state_abbrev(token: str) -> bool:
    s = token.strip()
    return len(s) == 2 and s.isalpha() and s.isupper()


def _is_number_token(token: str) -> bool:
    t = token.strip().replace(",", "")
    if t == "":
        return False
    return t.replace(".", "", 1).isdigit()


def _to_int_number(token: str) -> int:
    t = token.strip().replace(",", "")
    return int(float(t))


_zip_plus4 = re.compile(r"\b\d{3,5}-\d{3,4}\b")
_digits = re.compile(r"\d+")


def _count_zip_tokens_in_zip_columns(text: str) -> int:
    if not text:
        return 0
    cnt = 0

    def _mark(m):
        nonlocal cnt
        cnt += 1
        return "Z"

    text = _zip_plus4.sub(_mark, text)
    for run in _digits.findall(text):
        L = len(run)
        if L in (3, 4, 5, 7, 8, 9):
            cnt += 1
    return cnt


class StateZipStats(MRJob):
    def mapper(self, _, line):
        fields = line.rstrip("\n").split("\t")
        if len(fields) < 3:
            return
        state = None
        if len(fields) >= 4:
            cand = fields[3].strip()
            if _is_state_abbrev(cand):
                state = cand
        if state is None:
            for tok in fields:
                if _is_state_abbrev(tok):
                    state = tok.strip()
                    break
        if not state or state.lower() == "state":
            return
        population = None
        if len(fields) >= 5:
            pop_s = fields[4].strip().replace(",", "")
            if pop_s and pop_s.replace(".", "", 1).isdigit():
                population = _to_int_number(pop_s)
        if population is None:
            for tok in fields:
                if _is_number_token(tok):
                    raw = tok.strip().replace(",", "")
                    if raw.isdigit() and (len(raw) == 5 or len(raw) == 9):
                        continue
                    population = _to_int_number(raw)
                    break
        if population is None:
            return
        zip_count = 0
        if len(fields) >= 6:
            for tok in fields[5:]:
                zip_count += _count_zip_tokens_in_zip_columns(tok)
        else:
            best_multi = 0
            for tok in fields:
                c = _count_zip_tokens_in_zip_columns(tok)
                if c >= 2 and c > best_multi:
                    best_multi = c
            if best_multi >= 2:
                zip_count = best_multi
            else:
                for tok in fields:
                    if _count_zip_tokens_in_zip_columns(tok) == 1:
                        zip_count = 1
                        break
        yield state, (population, zip_count, 1)

    def reducer(self, state, triples):
        total_pop = 0
        total_zip = 0
        n = 0
        for pop, zc, one in triples:
            total_pop += pop
            total_zip += zc
            n += one
        if n > 0:
            yield state, [total_pop / n, total_zip / n]


if __name__ == "__main__":
    StateZipStats.run()





