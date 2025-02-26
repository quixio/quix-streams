import timeit

setup = """
suffixes = ["aaa"]
suffixes = set(suffixes)
"""

endswith = """
for _ in range(1000):
    if "viurnuivbriubiurt"[:3] in suffixes:
        continue
"""

print(timeit.timeit(endswith, setup=setup))
