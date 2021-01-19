#!/usr/bin/env python3

import json
import subprocess

# This helper test class takes two JSON result files, runs the compare_benchmarks.py script, and checks that the output
# is as expected. It is called from benchmark tests such as hyriseBenchmarkTPCC_test.py.


# The next two functions check for string matches instead of using math.isclose() since extracting values from
# ANSI-colored text turned out to be too cumbersome.
def assert_latency_equals(item_count, runtimes, latency_string):
    if item_count == 0:
        assert "nan" in latency_string
        return
    avg_latency = sum(runtimes) / item_count / 1_000_000
    assert str(round(avg_latency, 1)) in latency_string


def assert_throughput_equals(item_count, duration, throughput_string):
    throughput = item_count / duration * 1_000_000_000
    assert str(round(throughput, 2)) in throughput_string


class CompareBenchmarkScriptTest:
    results_1 = {}
    results_2 = {}
    script_output = ""
    script_output_lines = ""

    def __init__(self, script_path, result_filepath_1, result_filepath_2):
        with open(result_filepath_1) as json_file:
            self.results_1 = json.load(json_file)
        with open(result_filepath_2) as json_file:
            self.results_2 = json.load(json_file)

        with subprocess.Popen([script_path, result_filepath_1, result_filepath_2], stdout=subprocess.PIPE) as proc:
            self.script_output = proc.stdout.read().decode("utf-8")

        self.script_output_lines = self.script_output.splitlines()

    def run(self):
        self.check_context_table()
        self.check_result_table()

    # Checks the contents of context table which is printed above the results.
    def check_context_table(self):
        assert self.script_output.startswith("+Configuration Overview----")

        horizontal_table_separators = [
            i for i, line in enumerate(self.script_output_lines) if line.startswith("+-------")
        ]
        end_of_context_table = horizontal_table_separators[1]  # first is end of context table header

        context_1 = self.results_1["context"]
        context_2 = self.results_2["context"]

        common_keys = set(context_1.keys()) & set(context_2.keys())
        expected_row_count = (
            len(common_keys) + (len(context_1) - len(common_keys)) + (len(context_2) - len(common_keys))
        )
        assert (end_of_context_table - 3) == expected_row_count  # s -3 to substact header

        # Check for diverging contexts
        if len(context_1) != len(context_2):
            # when an item exists only in one context, it is still printed and the other side is shown an "undefined"
            lines_with_undefined = [
                line for line in self.script_output_lines[3:end_of_context_table] if "undefined" in line
            ]
            # If the context differ, there needs to be at least one undefined
            assert (expected_row_count - len(common_keys)) == len(lines_with_undefined)

    # Checks the contents of result table.
    def check_result_table(self):
        horizontal_table_separators = [
            i for i, line in enumerate(self.script_output_lines) if line.startswith("+-------")
        ]
        begin_of_result_table = horizontal_table_separators[3]  # first three are for context table
        # Note: depending on the benchmarks notes (e.g., limited runs note) there can be 6 or 7 horizontal tables lines

        # The compare script somewhat handles benchmark files with diverging benchmark items, but this automated test
        # expects valid benchmark files.
        assert len(self.results_1["benchmarks"]) == len(self.results_2["benchmarks"])

        current_position = begin_of_result_table + 1  # first result line
        for old_benchmark, new_benchmark in zip(self.results_1["benchmarks"], self.results_2["benchmarks"]):
            current_line = self.script_output_lines[current_position]
            fields = current_line.split("|")  # obtain cells of table
            assert len(fields) == 12

            # Get actual values of result JSON files
            old_successful_runs = old_benchmark["successful_runs"]
            new_successful_runs = new_benchmark["successful_runs"]
            old_unsuccessful_runs = old_benchmark["unsuccessful_runs"]
            new_unsuccessful_runs = new_benchmark["unsuccessful_runs"]

            # Expect name in first column
            assert fields[1].strip() == old_benchmark["name"]

            # Check the latency values for both JSON files
            assert_latency_equals(
                len(old_successful_runs), [run["duration"] for run in old_successful_runs], fields[3],
            )
            assert_latency_equals(
                len(new_successful_runs), [run["duration"] for run in new_successful_runs], fields[4],
            )

            # Get the divisors for both benchmark files. They differ for Ordered and Shuffled mode.
            divisors = (
                0.0,
                0.0,
            )  # 0.0 ensures failure when new benchmark mode is added through div by 0.0
            if self.results_1["context"]["benchmark_mode"] == "Ordered":
                divisors = (old_benchmark["duration"], new_benchmark["duration"])
            elif self.results_1["context"]["benchmark_mode"] == "Shuffled":
                divisors = (
                    self.results_1["summary"]["total_duration"],
                    self.results_2["summary"]["total_duration"],
                )

            # Check the throughput values for both JSON files
            assert_throughput_equals(len(old_successful_runs), divisors[0], fields[7])
            assert_throughput_equals(len(new_successful_runs), divisors[1], fields[8])

            current_position += 1

            # Check if there are unsucessful runs. In this case, a second line for the current benchmark item is shown.
            if len(old_unsuccessful_runs) > 0 or len(new_unsuccessful_runs) > 0:
                current_line = self.script_output_lines[current_position]
                fields = current_line.split("|")

                assert "unsucc.:" in fields[1]

                if len(old_unsuccessful_runs) > 0:
                    assert_latency_equals(
                        len(old_unsuccessful_runs), [run["duration"] for run in old_unsuccessful_runs], fields[3],
                    )
                    assert_throughput_equals(len(old_unsuccessful_runs), divisors[0], fields[7])
                else:
                    assert "nan" in fields[3]
                    assert "0.00" in fields[7]

                if len(new_unsuccessful_runs) > 0:
                    assert_latency_equals(
                        len(new_unsuccessful_runs), [run["duration"] for run in new_unsuccessful_runs], fields[4],
                    )
                    assert_throughput_equals(len(new_unsuccessful_runs), divisors[1], fields[8])
                else:
                    assert "nan" in fields[4]
                    assert "0.00" in fields[8]

                current_position += 1
