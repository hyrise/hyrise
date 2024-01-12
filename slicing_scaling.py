import re
from collections import defaultdict

def print_results(item, sl, sc, est, tot, cache, store, nodes, opscan, iterations):
	if item is None:
		return
	print()
	print(item)
	print(f"\tslice: {round(sl / iterations, 2)} ms")
	print(f"\tscale: {round(sc / iterations, 2)} ms")
	print(f"\testim: {round(est / iterations, 2)} ms")
	print(f"\ttotal: {round(tot / iterations, 2)} ms")
	print(f"\tcache: {round(cache / iterations, 2)} ms")
	print(f"\tstore: {round(store / iterations, 2)} ms")
	print("\tnodes:", end="")
	node_types = list(nodes.keys())
	for n in sorted(node_types, key=lambda n: nodes[n], reverse=True):
		print(f" {n} {round(nodes[n] / iterations, 2)} ms,", end="")
	print()
	print(f"\tscanp: {round(opscan / iterations, 2)} ms")

def main():
	current_item = None
	current_slice = 0
	current_scale = 0
	total = 0
	estimator = 0
	cache = 0
	store = 0
	op_scan = 0
	nodes = defaultdict(int)

	iteration_re = re.compile(r"(?<=Executed )\d+(?= times)")

	with open ("slice_scale_new_8.txt") as f:
		for line in f:
			# print(line, end="")
			if "Benchmarking" in line:
				current_item = line.split(" ")[2].strip()
				continue
			if line.startswith("scale"):
				current_scale += float(line.split(" ")[1])
				continue
			if line.startswith("slice"):
				current_slice += float(line.split(" ")[1])
				continue
			if line.startswith("estimate"):
				estimator += float(line.split(" ")[1])
				continue
			if line.startswith("total"):
				total += float(line.split(" ")[1])
				continue
			if line.startswith("cache"):
				#print(line, float(line.split(" ")[1][:-3]))
				cache += float(line.split(" ")[1][:-3]) / 10**6
				continue
			if line.startswith("store"):
				store += float(line.split(" ")[1][:-3]) / 10**6
				continue
			if line.startswith("node"):
				nodes[line.split(" ")[1]] += float(line.split(" ")[2][:-3]) / 10**6
				continue
			if line.startswith("scanp"):
				op_scan += float(line.split(" ")[1][:-3]) / 10**6
				continue


			match = iteration_re.search(line)

			if match is not None:
				iterations = int(match.group())
				print_results(current_item, current_slice, current_scale, estimator, total, cache, store, nodes, op_scan, iterations)
				current_slice = 0
				current_scale = 0
				estimator = 0
				total = 0
				cache = 0
				store = 0
				op_scan = 0
				nodes = defaultdict(int)


if __name__ == '__main__':
	main()
