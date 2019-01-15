
# Shorthand to execute MVCC benchmark only
`cmake-build-debug/hyriseMicroBenchmarks --benchmark_filter=BM_MVCC_UPDATE | tee mvcc-benchmark-results.txt`


# Modify benchmark output
1. Filter console output using `grep BM_MVCC_UPDATE`
2. Regex with capture groups to extract values
`^MVCC_Benchmark_Fixture/BM_MVCC_UPDATE/([0-9]+)\s*([0-9]*)\sns\s*([0-9]+)\sns\s*[0-9]*\s*$`

3. Use sed and the regex to create a csv-output:

cat mvcc-benchmark-results.txt | \
 grep MVCC_Benchmark_Fixture/BM_MVCC_UPDATE | \
  sed -E 's=MVCC_Benchmark_Fixture/BM_MVCC_UPDATE/([0-9]+)\s*([0-9]*)\sns\s*([0-9]+)\sns\s*[0-9]*\s*$=\1,\2,\3=gm'

(not yet working)

