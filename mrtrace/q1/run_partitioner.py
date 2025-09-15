from partition import what_would_partitioner_do

emailprefix = "Nicolalalas"
numreducers = 3

keys = [
    "with","great","data","come","responsibilities","a","wall","spring",
    "Humpty","Dumpty","sat","on","had","fall","semester","and"
]

for k in keys:
    r = what_would_partitioner_do(emailprefix, k, numreducers)
    print(k, "-> reducer", r)

