# Parts of Class Names

| Name      | Meaning                                                                                                                                                                                                                                                |
| --------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Abstract* | An incomplete class used for *dynamic* polymorphism (i.e., OOP with run-time resolved virtual methods). It has pure virtual methods that need to be implemented by subclasses.                                                                         |
| Base*     | An incomplete class used for *static* polymorphism (i.e., compile-time resolved templates). It implements methods shared by all implementations. Mostly, it is used so that we have a non-templated superclass, which can be stored in e.g., a vector. |
| *Node     | An entry in a Logical Query Plan, for example a Predicate or a Join.                                                                                                                                                                                   |
| *Operator | An entry in a Physical Query Plan, for example a Table Scan or a Sort-Merge Join. Currently, each operator is the physical representation of exactly one node. This is not a hard limitation.                                                          |

# Approved Abbreviations

We limit the use of abbreviations to very few cases. Unless an abbreviation is listed here or is something that we can expect every Computer Science Graduate to know (e.g., CSV, NUMA), do not use it in interfaces, code, or documentation.

| Name        | Short for                                                                |
| ----------- | ------------------------------------------------------------------------ |
| ART         | Adaptive Radix Tree (only for internal use of that data structure)       |
| cid         | Commit-ID (used for MVCC)                                                |
| dict_column | Dictionary Column                                                        |
| expr        | *Expression*, but only ok if used for an *expression* as described below |
| impl        | Implementation, mainly used for the [Pimpl] Pattern                      |
| *_it        | Iterator (in variable names)                                             |
| LQP         | *Logical Query Plan*                                                     |
| MVCC        | Multi-Version Concurrency Control                                        |
| PMR         | Polymorphic Memory Resource (see types.hpp)                              |
| PQP         | *Physical Query Plan*                                                    |
| PosList     | Position List (as used in ReferenceColumns)                              |
| ref_column  | Reference Column                                                         |
| tid         | Transaction ID (used for MVCC)                                           |
| val_column  | Value Column                                                             |

[Pimpl]: http://en.cppreference.com/w/cpp/language/pimpl

# Glossary

This explains high-level terms that have a specific meaning within Hyrise and that are used all over the code base. We do not want to document every class name here.

| Name                       | Description                                                                                                                                                                |
| -------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Abstract Syntax Tree (AST) | The old name of the *Logical Query Plan*. It was renamed when the LQP stopped being a tree and became a DAG. Shoot this old term on sight.                                 |
| Chunk                      | Each table in Hyrise is horizontally partitioned into one or more [chunks](https://github.com/hyrise/hyrise/wiki/chunk-concept).                                              |
| Column Type                | Type of a class inheriting from BaseColumn (mostly used as template parameter name)                                                                                        |
| Data Type                  | One of the currently five supported column data types (int, long, float, double, std::string)                                                                              |
| Expression                 | Any type of SQL expression that is either logical or creates a new column (e.g., `col_a = 3`, `col_a + 4`).                                                                |
| Iterable                   | A good way to iterate over different types of columns. See `column_iterables.hpp` to start.                                                                                  |
| Logical Query Plan         | The logical representation of a query plan. It includes nodes like "Predicate" (a filter) or "Join" but does not describe what implementation is used for executing these. |
| Node (LQP)                 | A node in a logical query plan.                                                                                                                                            |
| Node (NUMA)                | A NUMA memory node (usually a CPU socket).                                                                                                                                 |
| Operator                   | A class that usually takes 0-2 input tables and creates one output table. Example: TableScan, Join, Insert.                                                                |
| Physical Query Plan        | The physical representation of a query plan. It holds the actual *operators* used to execute the query. Usually created by a translator from the LQP.                      |
| Rule                       | A rule in our optimizer. For instance, pushing all predicates as far down as possible is a rule.                                                                           |
| Task                       | Everything that can be scheduled.                                                                                                                                          |
