# The Four Commandments
1. Thy code shalt be the primary method of documentation. Part of this is to choose concise but descriptive names.
   Comments should be used to explain the concept and usage of classes (in the hpp file) and the structure of the
   algorithms (in the implementation).
2. Thou shalt program defensively. Use `Assert` wherever it makes sense, use `DebugAssert` in performance-critical parts
   (e.g., within hot loops). Also, we do not handle exceptions in Hyrise. If an invalid state is reached, we crash
   immediately. This makes debugging easier. An exception to this is user-facing code where we handle, e.g., typos in
   SQL queries.
3. Thou shalt reflect on existing code. Just as your code is not perfect, neither is the code that people wrote before
   you. Try to improve it as part of your PRs and do not hesitate to ask if anything is unclear. Chances are that it can
   be improved.
4. Thou shalt properly test thy code. This includes unit *and* integration tests. Try to isolate parts that can be
   independently tested.

# C++
* Use automatic memory management (RAII, smart pointers). `new` and `malloc` are evil words.
* Be mindful of ownership. Not everything needs to be a smart pointer. Consider passing around references to the object
  or references to a shared_ptr instead of copying the shared_ptr. Remember that this might not be safe when passing
  shared_ptrs into, e.g., JobTasks.
* Use `const` whenever possible. Consider variables, methods, pointers, and their pointees.

* Header files
  * Reduce the size of your hpp files, both in terms of the number of lines and the code complexity. This keeps the
    compilation duration low.
  * Code in hpp files is compiled for every cpp file that includes the header. As such, move code to cpp files where
    possible. This often includes templated classes, where it is sometimes possible to implement their code in cpp
    files.
  * Anonymous namespaces are a good way to define local helper methods.
  * Use forward declarations instead of full header includes wherever possible.

* Loops
  * Use range-based `for` loops when possible: `for (const auto& item : items) {...}`.
  * If you have to use old-style loops, keep in mind that the loop condition is evaluated every time: Instead of
    `for (auto offset = size_t{0}; offset < something.size(); ++offset)`, the size should be retrieved just once. See
    also [this document](http://llvm.org/docs/CodingStandards.html#don-t-evaluate-end-every-time-through-a-loop).

* Data structures
  * When creating a vector where you know the size beforehand, use `reserve` to avoid unnecessary resizes and
    allocations.
  * Hash-based data structures are usually faster than tree-based data structures. Unless you have a reason to use the
    latter, prefer `unordered_(map|set)` over `map` and `set`.

* Copies
    * Avoid unnecessary copies, C++ makes it too easy to inadvertently copy objects.
    * For larger elements (e.g., vectors), pass a (`const`) reference instead.
    * If your implemented class does not need to be copied (e.g., a `Table` should never exist more than once), inherit
      from `Noncopyable` to avoid these potentially expensive copies.

* Miscellaneous
  * Prefer `if (object)` over `if (object != nullptr)` or `if (object.has_value())`.
  * Don't write `this->` if you don't have to.
  * Be explicit with types: Use [u]int(8|16|32|64)_t instead of `int, long, uint` etc.
  * Use [auto-to-stick](https://www.fluentcpp.com/2018/09/28/auto-stick-changing-style/): `auto x = 17;` or
    `auto y = std::vector<size_t>{};`.
  * Namespaces: Do not create nested namespaces, do not import namespaces.
  * Prefer pre-increment over post-increment. See the [LLVM Coding Standards](https://llvm.org/docs/CodingStandards.html#prefer-preincrement)
  * Consider structured bindings: `const auto& [iterator, added] = unordered_map.emplace(...);`
  * Use braced control statements, even for single-line blocks. Moreover, unless the block
    is empty (e.g., `while (!ready) {}`), add line breaks. Instead of `if (...) x();` (or `if (...) { x(); }`), write:
    
    ```
       if (...) {
         x();
       }
    ```


# Formatting and Naming
* Much of this is enforced by clang-tidy. However, clang-tidy does not yet cover hpp files (see #1901). Also, while
  clang-tidy is a great help, do not rely on it.
* Call ./scripts/format.sh before committing your code.
* Choose clear and concise names, and avoid, e.g., `i`, `j`, `ch_ptr`.
* Formatting details: 2 spaces for indentation, 120 columns, comments above code.
* Use empty lines to structure your code.
* Naming conventions:
    * Files: lowercase separated by underscores, e.g., abstract_operator.cpp, usually corresponding to a class, e.g.,
      AbstractOperator.
    * Types (classes, structs, enums, typedefs, using): PascalCase starting with uppercase letter, e.g., `TableScan`.
    * Variables: lowercase separated by underscores, e.g., `chunk_size`.
    * Functions: lowercase separated by underscores, e.g., `append_mutable_chunk()`.
    * Private / protected members / methods: like variables / functions with leading underscore, e.g., `_on_execute()`.
    * Classes that are used only to have a non-templated base class are named `BaseXY` (e.g., `BaseValueSegment`, while
      classes that have multiple differing implementations are named `AbstractXY` (e.g., `AbstractOperator`).
    * In cases where a constructor parameter would have the same name as the member it initializes, prefix it with
      `init`: `C(int init_foo) : foo(init_foo) {}`.
    * If an identifier contains a verb or an adjective in addition to a noun, the schema [verb|adjective]\[noun] is
      preferred, e.g., use `left_input` rather than ~~`input_left`~~ and `set_left_input()` rather than
      ~~`set_input_left()`~~.
    * Unused variables: instead of leaving out the parameter name, comment the parameter name out (e.g.,
      `void function(const size_t /* count */) { ... }`). If the context does not provide a good name (such as
      `/* count */`), use `/* unused */`.

* Maintain correct orthography and grammar. Comments should start with a capital letter, sentences should be finished
  with a full stop.
  * Class names within comments are written in PascalCase - e.g., "As the input may be a ReferenceSegment, a valid RowID
    may point to a row that is NULL."

# Pull Requests
## Opening PRs
* When you submit a non-trivial PR, include the results of benchmark_all.sh.
  * These results help in understanding potential performance changes as well as document potential changes to the compilation
    costs.
  * We do not do this automatically as the CI server is not sufficiently isolated and the performance results would
    vary. Similarly, your personal laptop is likely to produce unreliable results.
* If your PR is related to an existing issue, reference it in the PR's description (e.g., `fixes #123` or `refs #123`).
* If you are not a member of the Hyrise organization, your PR will not be built by our CI server. Contact a maintainer
  for this. They can add you to the organization or manually trigger builds from within Jenkins.
* For your PR to be merged, it must pass a FullCI run. Set the FullCI tag in GitHub before committing to trigger the
  complete (but more expensive!) run.
* When merging your PR, copy your PR description (excluding the benchmark results) into the commit message. The commit
  message of the squash merge should NOT contain the individual commit messages from your branch.

## Reviewing PRs
* Keep the whole picture in mind. Often, it makes sense to make two passes: One for the code style and line-level
  modifications; one for checking how it fits into the overall picture.
* Check if the PR sufficiently adds tests both for happy and unhappy cases.
* Do not shy away from requesting changes on surrounding code that was not modified in the PR. Remember that after a PR,
  the code base should be better than before.
* Verify the CI results, including how the reported coverage changed, and check if the compile time or query performance
  have been negatively affected. For this, the author should have posted the results of benchmark_all.sh into the PR
  description.
