# Coding Style
Avoid exception handling. Because Opossum is not a product, we do not have to recover from errors. Instead, fail loud (i.e., terminate the program) so that developers immediately notice that something is wrong and can fix the problem at its root.

## Formatting
- 2 spaces for indentation
- 120 columns
- comments above code
- clang_format (executed with each make) enforces these rules automatically

## C++ guidelines
- do not use `new` or `malloc`
- do not nest namespaces
- do not import namespaces (`std::`)

- when overriding a `virtual` method, avoid repeating `virtual` and always use `override` or `final`
- use const (including cbegin() and cend()) whenever possible
- use [u]int(8|16|32|64)_t instead of `int`, long, uint` etc.
- include in this order: header for implementation file, c system, c++ system, other
- use smart pointers over c-style pointers
- use `IS_DEBUG` macro for non-essential checks
- be specific: `double a = 3.0;` but `float a = 3.0f;`

## Naming

- Files: lowercase separated by underscores, e.g., abstract_operator.cpp
- Types (classes, structs, enums, typedefs, using): CamelCase starting with uppercase letter, e.g., `BaseColumn`
- Variables: lowercase separated by underscores, e.g., `chunk_size`
- Functions: lowercase separated by underscores, e.g., `get_num_in_tables()`
- Private / proctected members / methods: like variables / functions with leading underscore, e.g., `_get_chunks()`
