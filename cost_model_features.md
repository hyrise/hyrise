| Category | Feature | Type | TableScan | JoinHash  | AggregateHash |
|----------|---------|------|:---------:|:---------:|:-------------:|
|Input Table Statistics*| # rows | C | x | x | x |
| | # columns | C | x | x | x |
| | # chunks | C | x | x | x |
| | data type | D | x |  |  |
| | sortedness | D | x | x | x |
| | table type | D | x | x | x |
| | ratio of pruned chunks | C |  | x |  |
| Output table statistics | # rows | C | x | x | x |
| Operator-specific | selectivity | C | x |  |  |
| | join mode | D |  | x |  |
| | is `COUNT(*)` | B |  |  | x |
| | # group by columns | C |  |  | x |
| | # aggregate columns | C |  |  | x |

Features of the different cost models per Hyrise's operator. B denotes a
boolean, C a continuous, D a discrete variable.

*for build and probe side of JoinHash
