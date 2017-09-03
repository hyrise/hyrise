# Contribution Guidelines
Do not commit/push directly to the master or develop branch. Instead, create a feature branch/fork and file a merge request.

# Coding Style
Avoid exception handling. Because Hyrise is not a product, we do not have to recover from errors. Instead, fail loud (i.e., terminate the program) so that developers immediately notice that something is wrong and can fix the problem at its root.

## Formatting
- 2 spaces for indentation
- 120 columns
- Comments above code
- clang_format enforces these rules automatically

## C++ guidelines
- Do not use `new` or `malloc`
- Do not nest namespaces
- Do not import namespaces (`std::`)

- When overriding a `virtual` method, avoid repeating `virtual` and always use `override` or `final`
- Use const (including cbegin() and cend()) whenever possible
- Use [u]int(8|16|32|64)_t instead of `int`, long, uint` etc.
- Include in this order: header for implementation file, c system, c++ system, other
- Use smart pointers over c-style pointers
- Use `IS_DEBUG` macro for non-essential checks
- Be specific: `double a = 3.0;` but `float a = 3.0f;`
- Use forward declarations whenever possible to reduce compile time
- We mostly use structs for PODS (plain old data structures). If it has methods, chances are that it is a class.

## Naming Conventions

- Files: lowercase separated by underscores, e.g., abstract_operator.cpp
- Types (classes, structs, enums, typedefs, using): CamelCase starting with uppercase letter, e.g., `BaseColumn`
- Variables: lowercase separated by underscores, e.g., `chunk_size`
- Functions: lowercase separated by underscores, e.g., `get_num_in_tables()`
- Private / proctected members / methods: like variables / functions with leading underscore, e.g., `_get_chunks()`
- Classes that are used only to have a non-templated base class are named `BaseXY` (e.g., BaseColumn), while classes that have multiple differing implementations are named `AbstractXY` (e.g., AbstractOperator)

- Choose descriptive names. Avoid `i`, `j`, etc. in loops.

## Review

- Things to look for:
	- Guidelines (see above)
	- Is the copy constructor deleted where it makes sense?
	- Is the destructor virtual for base classes?
	- Are unnecessary copies of heavy elements made? (prefer vector& over vector, but not int& over int)
