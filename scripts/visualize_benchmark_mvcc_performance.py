import pandas
import re
import sys

def main():
    points = []
    with open(sys.argv[1]) as f:
        for line in f:
            if line.startswith('"'):        
                line = line.split(',')
                time = re.search(r'[0-9]+', line[0]).group(0)
                value = line[2]
                points.append((time, value))

    dfs = [points[:len(points)//2], points[len(points)//2:]]
    print(dfs)
    df1 = pandas.DataFrame(dfs[0], columns=['Iterations', 'Validate'])
    df2 = pandas.DataFrame(dfs[1], columns=['Iterations', 'Update'])
    df1 = df1.apply(pandas.to_numeric)
    df2 = df2.apply(pandas.to_numeric)
    #import pdb;pdb.set_trace()
    ax = df1.plot(x=0, y=1)
    plot = df2.plot(x=0, y=1, ax=ax)
    plot.set_ylabel("Nanoseconds")
    plot.get_figure().savefig('mvcc_benchmark', bbox_inches='tight')

if __name__ == '__main__':
    main()