from abstract_benchmark import AbstractBenchmark

class CalibrationBenchmark(AbstractBenchmark):
  def name(self):
  	return "calibration"

  def visualization_pattern(self):
    raise NotImplementedError()

  def exec_path(self):
    return "build-release/hyriseCalibration"

  #def result_path(self):
  #  return "/home/Alexander.Loeser/hyrise/benchmark_results/final/tpch/sf10-3d-corrected"

  def max_runs(self):
    return 100

  def time(self):
    return 5000

  def scale(self):
    return 1

  def chunk_sizes(self):
    return [65535]

  def sort_orders(self):
    return {
      'nosort': {}
    }
