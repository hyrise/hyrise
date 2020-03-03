
class AbstractBenchmark:
 
  def name(self):
    raise NotImplementedError()

  def exec_path(self):
    raise NotImplementedError()

  def result_path(self):
    raise NotImplementedError()

  def time(self):
    raise NotImplementedError()

  def scale(self):
    raise NotImplementedError()

  def chunk_sizes(self):
    raise NotImplementedError()

  def sort_orders(self):
    raise NotImplementedError()