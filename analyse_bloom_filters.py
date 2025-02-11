#!/usr/bin/env python3

import math
import os

from xml.etree import ElementTree as ET





def get_join_bloom_filtering_information(query_name, dag_svg):
    # Register the SVG namespace
    ET.register_namespace("", "http://www.w3.org/2000/svg")
    ET.register_namespace("xlink", "http://www.w3.org/1999/xlink")

    # Parse the SVG content
    root = ET.fromstring(dag_svg)

    materialized_tuples = 0
    join_input_tuples = 0

    output = ""

    # Find all g nodes
    for g_node in root.findall(".//{http://www.w3.org/2000/svg}g"):
        if g_node.get("class") == "node":
            # Get all text elements within this node
            text_elements = g_node.findall(".//{http://www.w3.org/2000/svg}text")
            if text_elements[0] is None or text_elements[0].text is None or "JoinHash" not in text_elements[0].text:
                continue

            node_texts = [text.text for text in text_elements if text is not None and text.text is not None]

            # Find the 'a' node and get its title
            a_node = g_node.find(".//{http://www.w3.org/2000/svg}a")
            if a_node is not None:
                output += "\t"
                for t in node_texts:
                  output += t
                output += "\n"
                a_title = a_node.get('{http://www.w3.org/1999/xlink}title')
                filter_rates = []
                for line in a_title.split("\n"):
                    if "Bloom filtering:" in line:
                        output += "\t\t" + line
                        build_filter_rate = float(line[line.find("build ") + 6:line.find(" %")])
                        probe_filter_rate = float(line[line.find("probe ") + 6:line.rfind(" %.")])
                        filter_rates.append(build_filter_rate / 100.0)
                        filter_rates.append(probe_filter_rate / 100.0)
                        output += str(filter_rates) + "\n"
                    elif "Materialized tuples" in line:
                        assert len(filter_rates) == 2
                        output += "\t\t" + line + "\n"
                        materialized_build = int(line[line.find("build ") + 6:line.find(", probe")])
                        materialized_probe = int(line[line.find("probe ") + 6:line.rfind(".")])
                        materialized_tuples += materialized_build + materialized_probe
                        if filter_rates[0] < 0.999999:
                            join_input_tuples += materialized_build / (1.0 - filter_rates[0])
                        else:
                            print(f"Warning: Build filter rate of {filter_rates[0]:.2%}")

                        if filter_rates[1] < 0.999999:
                            join_input_tuples += materialized_probe / (1.0 - filter_rates[1])
                        else:
                            print(f"Warning: Probe filter rate of {filter_rates[1]:.2%}")
    print(query_name.replace("_", " "))
    print(output)
    print(f"Query summary: {int(math.ceil(materialized_tuples)):,} materialized and {int(math.ceil(join_input_tuples - materialized_tuples)):,} filtered.")
    perc = ((join_input_tuples - materialized_tuples) / join_input_tuples) if join_input_tuples > 0 else 0.0
    print(f"               Filtered {perc:.2%} of {int(math.ceil(join_input_tuples)):,} tuples.\n\n")


# Example usage
if __name__ == "__main__":
    tpch_files = []
    for filename in os.listdir("."):
        if not filename.startswith("TPC-H_") and filename.endswith("-PQP.svg"):
            tpch_files.append(filename)

    tpch_files = sorted(tpch_files)

    for filename in tpch_files:
        # Read the SVG file
        with open(filename, "r", encoding="utf-8") as f:
            dag_svg = f.read()

        get_join_bloom_filtering_information(filename.replace("-PQP.svg", ""), dag_svg)


# 100 tupel
# 75% filtered > 25 tupel
# 75% / 100 > 0.75
# 25 / 1 - 0.75 > 100

# 100 tupel
# 90 % > 10 tupel
# 0.9
# 10 / 1 - 0.9

# 100 tupel
# 10 % > 90 tupel
# 0.1
# 90 / (1 - 0.1) > 