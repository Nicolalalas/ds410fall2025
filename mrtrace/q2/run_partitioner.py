from partition import what_would_partitioner_do

emailprefix = "Nicolalalas"
numreducers = 2

keys = [
    ["a","b"],["c","a"],["c","b"],["d","d"],["e","d"],
    ["k","blank"],["c","j"]
]

for k in keys:
    r = what_would_partitioner_do(emailprefix, str(k), numreducers)
    print(k, "-> reducer", r)

