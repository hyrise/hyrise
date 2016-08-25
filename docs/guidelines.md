# General Guidelines
- Fail early, fail loud

# Coding Style

## Formatting
- 4 spaces for indentation
- 120 columns
- comments above code
- clang_format (executed with each make) should do most of the job for you

## C++ guidelines
- avoid new / have clear ownership
	- problem: what about vector.push_back(new xy)
    - das geht doch irgendwie mit emplace oder so?
- use const whenever possible
- no nested namespaces
- no namespace imports (std::)
- use uint(32...)_t instead of int, long, etc.
- include stdlib before 3rdparty before own
- use shared_ptr
- use "if(debug)" for non-essential checks
- double a = 3.0; but float a = 3.0f;

## Naming

- Files: lowercase separated by underscores, e.g., abstract_operator.cpp
- Types (classes, structs, enums, typedefs, using): CamelCase starting with uppercase letter, e.g., BaseColumn
- Variables: lowercase separated by underscores, e.g., chunk_size
- Functions: lowercase separated by underscores, e.g., get_num_in_tables()
- Private / proctected members / methods: like variables / functions with leading underscore, e.g., _get_chunks()

# Todo
- simple performance tool to profile the impact of changes
- get_col_count() vs col_count()
