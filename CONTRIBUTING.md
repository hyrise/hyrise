# Contribution Guidelines
Do not commit/push directly to the master branch. Instead, create a feature branch/fork and file a merge request.

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
- Use [u]int(8|16|32|64)_t instead of `int, long, uint` etc.
- Include in this order: header for implementation file, c system, c++ system, other
- If your templated methods/classes are used outside of your file, they have to be in the header. But if you only use them internally, you should place them in the cpp file.
- Try to keep the size of your templated methods as small as possible (Thin Template idiom). If only a small part of your method depends on the template parameter, consider moving the rest into a non-templated method. This reduces compile times.
- Use smart pointers over c-style pointers
- Use `IS_DEBUG` macro for non-essential checks
- Be specific: `double a = 3.0;` but `float a = 3.0f;`
- Use forward declarations whenever possible to reduce compile time
- We mostly use structs for PODS (plain old data structures). If it has methods, chances are that it is a class.
- Don't write `this->` if you don't have to
- Use C++11 for loops when possible: `for (const auto& item : items) {...}`
- When creating a vector where you know the size beforehand, use `reserve` to avoid unnecessary resizes and allocations
- Don’t evaluate end() every time through a loop: http://llvm.org/docs/CodingStandards.html#don-t-evaluate-end-every-time-through-a-loop


## Naming Conventions
- Files: lowercase separated by underscores, e.g., abstract_operator.cpp
- Types (classes, structs, enums, typedefs, using): CamelCase starting with uppercase letter, e.g., `BaseColumn`
- Variables: lowercase separated by underscores, e.g., `chunk_size`
- Functions: lowercase separated by underscores, e.g., `get_num_in_tables()`
- Private / protected members / methods: like variables / functions with leading underscore, e.g., `_get_chunks()`
- Classes that are used only to have a non-templated base class are named `BaseXY` (e.g., BaseColumn), while classes that have multiple differing implementations are named `AbstractXY` (e.g., AbstractOperator)
- Choose descriptive names. Avoid `i`, `j`, etc. in loops.

### Naming convention for gtest macros:

TEST(ModuleNameClassNameTest, TestName), e.g., TEST(OperatorsGetTableTest, RowCount)
same for fixtures Test_F()

If you want to test a single module, class or test you have to execute the test binary and use the `gtest_filter` option:

- Testing the storage module: `./build/hyriseTest --gtest_filter="Storage*"`
- Testing the table class: `./build/hyriseTest --gtest_filter="StorageTableTest*"`
- Testing the RowCount test: `./build/hyriseTest --gtest_filter="StorageTableTest.RowCount"`

## Performance Warnings
- Sometimes, we have convenience functions, such as BaseColumn::operator[], or workarounds, such as performing multiple stable sorts instead of a single one. Because these might negatively affect the performance, the user should be warned if a query causes one of these slow paths to be chosen. For this, we have the PerformanceWarning() macro defined in assert.hpp.

## Documentation
- Most documentation should happen in the code or in the beginning of the header file
- More complex documentation, such as an explanation of an algorithm that profits from images, can be put in the Wiki. Please make sure to link the Wiki page in the code - otherwise, no one will find it.

# Review

**Things to look for:**

	- Guidelines (see above)
	- Is the copy constructor deleted where it makes sense?
	- Is the destructor virtual for base classes?
	- Are unnecessary copies of heavy elements made? (prefer vector& over vector, but not int& over int)
	- Did the author update documentation and dependencies (Wiki, README.md, DEPENDENCIES.md, Dockerfile, install.sh, Jenkinsfile)
