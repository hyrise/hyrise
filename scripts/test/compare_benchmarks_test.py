#!/usr/bin/env python3

import os
import unittest
import subprocess

SCRIPT_PATH = 'scripts/compare_benchmarks.py'

class CompareBenchmarksTest(unittest.TestCase):
  def do_strings_exist_in_order(self, search_strings, given_string):
    string_found_position = 0
    for search_string in search_strings:
      string_found_position = given_string[string_found_position:].find(search_string)
      self.assertTrue(string_found_position >= 0)


  def test_tpcc(self):
    with subprocess.Popen([SCRIPT_PATH, "resources/test_data/result_json/tpcc_1.json",
                           "resources/test_data/result_json/tpcc_2.json"], stdout=subprocess.PIPE) as proc:
      output = proc.stdout.read().decode("utf-8")

      output_lines = [l for l in output.splitlines()]
      for output_line in output_lines[:25]:
        self.assertFalse('≠' in output_line)

      # context table is shown
      self.do_strings_exist_in_order(['encoding', "{'default': {'encoding': 'Dictionary'}}", "{'default': {'encoding': 'Dictionary'}}"], output_lines[11])

      self.do_strings_exist_in_order(['Delivery', '2.0', '1.0', '-50%', '1000.00', '1100.00', '+10%'], output_lines[27])
      self.do_strings_exist_in_order(['unsucc.:', '3.0', '2.0', '-33%', '1000.00', '600.00', '-40%'], output_lines[28])
      self.do_strings_exist_in_order(['New-Order', '2.0', 'nan', '1000.00', '600.00', '-40%'], output_lines[29])
      self.do_strings_exist_in_order(['unsucc.:', '3.0', '2.0', '-33%', '1000.00', '600.00', '-40%'], output_lines[30])
      self.do_strings_exist_in_order(['Order-Status', '2.0', '1.0', '-50%', '1000.00', '1100.00', '+10%'], output_lines[31])
      self.do_strings_exist_in_order(['unsucc.:', 'nan', '2.0', '0.00', '600.00', '', ''], output_lines[32])
      self.do_strings_exist_in_order(['Payment', '2.0', '1.0', '-50%', '1000.00', '1100.00', '+10%'], output_lines[33])
      self.do_strings_exist_in_order(['unsucc.:', '3.0', 'nan', '1000.00', '0.00', '', ''], output_lines[34])
      self.do_strings_exist_in_order(['Stock-Level', '2.0', '2.0', '+0%', '1000.00', '1000.00', '+0%'], output_lines[35])


  def test_tpch(self):
    with subprocess.Popen([SCRIPT_PATH, "resources/test_data/result_json/tpch_1.json",
                           "resources/test_data/result_json/tpch_2.json"], stdout=subprocess.PIPE) as proc:
      output = proc.stdout.read().decode("utf-8")

      output_lines = [l for l in output.splitlines()]
      for output_line in output_lines[:10]:
        self.assertFalse('≠' in output_line)
      self.assertTrue('≠' in output_lines[11])
      self.do_strings_exist_in_order(['encoding', "{'default': {'encoding': 'Dictionary'}}", "{'default': {'encoding': 'LZ4'}}"], output_lines[11])
      for output_line in output_lines[12:25]:
        self.assertFalse('≠' in output_line)

      self.do_strings_exist_in_order(['TPC-H 06', '4000.0', '3500.0', '-12%', '171.34', '85.64', '-50%', '0.0000'], output_lines[27])
      self.do_strings_exist_in_order(['TPC-H 07', '5000.0', '5000.0', '+0%', '0.25', '0.25', '+0%', '(run time too short)'], output_lines[28])
      self.do_strings_exist_in_order(['Sum', '9000.0', '8500.0', '-6%'], output_lines[30])
      self.do_strings_exist_in_order(['Geomean',  '-29%'], output_lines[31])


  def test_ordered_vs_shuffled(self):
    with subprocess.Popen([SCRIPT_PATH, "resources/test_data/result_json/tpch_1.json",
                           "resources/test_data/result_json/tpch_2_shuffled.json"], stderr=subprocess.PIPE) as proc:
      output = proc.stderr.read().decode("utf-8")
      expected = 'Benchmark runs with different modes (ordered/shuffled) are not comparable'
      self.assertEqual(output.strip(), expected)


  def test_github_formatting(self):
    with subprocess.Popen([SCRIPT_PATH, "resources/test_data/result_json/tpcc_1.json",
                           "resources/test_data/result_json/tpcc_2.json", "--github"], stdout=subprocess.PIPE) as proc:
      output = proc.stdout.read().decode("utf-8")

      output_lines = [l for l in output.splitlines()]

      self.assertTrue(output_lines[23].startswith('```diff'))
      self.assertTrue(output_lines[28].startswith('+'))
      self.assertTrue(output_lines[29].startswith(' '))
      self.assertTrue(output_lines[30].startswith('-'))
      self.assertEqual(output_lines[-2], '```')


if __name__ == '__main__':
  unittest.main()
