estimate_statistics() {
[0] [Predicate] l_shipdate BETWEEN '1995-01-01' AND '1996-12-31'
 \_[1] [StoredTable] Name: 'lineitem'

Found no statistics for 1000000100000 in cache
estimate_statistics() {
[0] [StoredTable] Name: 'lineitem'

Found no statistics for 0000000100000 in cache
}
}
estimate_statistics() {
[0] [StoredTable] Name: 'lineitem'

Found in Cache! 
 }
estimate_statistics() {
[0] [StoredTable] Name: 'lineitem'

Found in Cache! 
 }
estimate_statistics() {
[0] [Join] Mode: Inner (n_name = 'FRANCE' AND n_name = 'GERMANY') OR (n_name = 'GERMANY' AND n_name = 'FRANCE')
 \_[1] [StoredTable] Name: 'nation'
 \_[2] [StoredTable] Name: 'nation'

Found no statistics for 0100000000011 in cache
estimate_statistics() {
[0] [StoredTable] Name: 'nation'

Found no statistics for 0000000000010 in cache
}
estimate_statistics() {
[0] [StoredTable] Name: 'nation'

Found no statistics for 0000000000001 in cache
}
}
estimate_statistics() {
[0] [StoredTable] Name: 'nation'

Found in Cache! 
 }
estimate_statistics() {
[0] [StoredTable] Name: 'nation'

Found in Cache! 
 }
estimate_statistics() {
[0] [StoredTable] Name: 'nation'

Found in Cache! 
 }
estimate_statistics() {
[0] [StoredTable] Name: 'nation'

Found in Cache! 
 }
estimate_statistics() {
[0] [Join] Mode: Inner c_nationkey = n_nationkey
 \_[1] [StoredTable] Name: 'customer'
 \_[2] [StoredTable] Name: 'nation'

Found no statistics for 0010000000101 in cache
estimate_statistics() {
[0] [StoredTable] Name: 'customer'

Found no statistics for 0000000000100 in cache
}
estimate_statistics() {
[0] [StoredTable] Name: 'nation'

Found in Cache! 
 }
generate_table_statistics2():    Compacting...
generate_table_statistics2():     Column 0 compacted to 2 bins
generate_table_statistics2():     Skipping string column 1
generate_table_statistics2():     Column 2 compacted to 2 bins
generate_table_statistics2():     Skipping string column 3
cardinality_estimation_inner_equi_join(): 1x1
  3 + 3
}
estimate_statistics() {
[0] [StoredTable] Name: 'customer'

Found in Cache! 
 }
estimate_statistics() {
[0] [StoredTable] Name: 'nation'

Found in Cache! 
 }
estimate_statistics() {
[0] [StoredTable] Name: 'customer'

Found in Cache! 
 }
estimate_statistics() {
[0] [StoredTable] Name: 'nation'

Found in Cache! 
 }
estimate_statistics() {
[0] [Join] Mode: Inner c_nationkey = n_nationkey
 \_[1] [StoredTable] Name: 'customer'
 \_[2] [Predicate] (n_name = 'FRANCE' AND n_name = 'GERMANY') OR (n_name = 'GERMANY' AND n_name = 'FRANCE')
    \_[3] [Join] Mode: Cross
       \_[4] [StoredTable] Name: 'nation'
       \_[5] [StoredTable] Name: 'nation'

No bitmask
estimate_statistics() {# Generated with the dbgen utility invoking `dbgen -s 0.001`
