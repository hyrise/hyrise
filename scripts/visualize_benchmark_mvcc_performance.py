import pandas
import re
import sys

'''
    Input: GoogleTest output saved as CSV.
      name,iterations,real_time,cpu_time,time_unit,bytes_per_second,items_per_second,label,error_occurred,error_message
      "MVCC_Benchmark_Fixture/BM_MVCC_VALIDATE/1",100,13476.1,13470,ns,,,,,
    Output: Graph of time in nanosecdonds per number of iterations for validate and update operations.
'''

def main():
  points = []
  with open(sys.argv[1]) as f:
    for line in f:
      if line.startswith('"'):        
        line = line.split(',')
        iteration = re.search(r'[0-9]+', line[0]).group(0)
        time = line[2]
        points.append((iteration, time))

  dfs = [points[:len(points)//2], points[len(points)//2:]]

  df1 = pandas.DataFrame(dfs[0], columns=['Iterations', 'Validate']).apply(pandas.to_numeric)
  df2 = pandas.DataFrame(dfs[1], columns=['Iterations', 'Update']).apply(pandas.to_numeric)

  graph1 = df1.plot(x=0, y=1)
  graph2 = df2.plot(x=0, y=1, ax=graph1)
  graph2.set_ylabel("Nanoseconds")
  graph2.get_figure().savefig('mvcc_benchmark', bbox_inches='tight')

if __name__ == '__main__':
  main()