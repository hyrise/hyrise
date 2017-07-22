Column Types: Value, Dictionary, Reference

Column Value Types: `int`, `double`, `long`, `float`, `std::string`



Table Scan Types: constant value, 2-columns, like



1. Resolve left side: column
   1. retrieve value type
2. Resolve right side: constant value, column
   1. If constant value
      1. convert to const iterable (convert type ?)
   2. else (column)
      1. retrieve value type
      2. continue with 3.
3. either both columns are reference columns or data columns
   1. data columns
   2. reference columns