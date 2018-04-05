import re
import os

class_names = set()

def process(file_path):
    print("Processing {}".format(file_path))

    out_lines = []

    regex_repl_pairs = [
        (re.compile("std::shared_ptr<(opossum::)?([a-zA-Z]*)(<([:_a-zA-Z0-9]*)>)?>"), r"\1\2SPtr\3"),
        (re.compile("std::shared_ptr<const (opossum::)?([:a-zA-Z]*)(<([:_a-zA-Z0-9]*)>)?>"), r"\1\2CSPtr\3"),
        (re.compile("std::weak_ptr<(opossum::)?([:a-zA-Z]*)(<([:_a-zA-Z0-9]*)>)?>"), r"\1\2WPtr\2\3"),
        (re.compile("std::weak_ptr<const (opossum::)?([:a-zA-Z]*)(<([:_a-zA-Z0-9]*)>)?>"), r"\1\2CWPtr\3"),
    ]

    with open(file_path) as file:
        for in_line in file.readlines():
            out_line = in_line

            for regex, repl in regex_repl_pairs:
                m = regex.match(in_line)
                while m is not None:
                    class_names.add(m.group(2))
                    m = regex.match(in_line, pos=m.pos + 1)

                out_line, _ = regex.subn(repl, out_line)

            #if out_line != in_line:
            #    print("{} -> {}".format(in_line, out_line))

            out_lines.append(out_line)

    with open(file_path, "w") as file:
        file.writelines(out_lines)



if __name__ == "__main__":
    directories = ["src"]

    for root_directory in directories:
        for root, _, file_names in os.walk(root_directory):
            for file_name in file_names:
                if file_name == "create_ptr_aliases.hpp" or file_name == "resolve_type.hpp":
                    continue

                if file_name.endswith(("cpp", "hpp")):
                    process(os.path.join(root, file_name))

    for class_name in class_names:
        print("Class {}".format(class_name))