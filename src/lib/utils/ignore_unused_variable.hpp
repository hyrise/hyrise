#pragma once

namespace opossum {

/**
 * Sometimes the compiler may complain about unused variables.
 * Call this function with those variables to silence the compiler.
 *
 * USAGE EXAMPLE (not a good example of an unavoidable unused variable ;) )
 *
 * template<typename T> void foo(const T& value) {
 *      if constexpr (std::is_same_v<T, pmr_string) {
 *          ignore_unused_variable(value);
 *          Fail("String not supported");
 *      } else {
 *          // Do something with value
 *      }
 * }
 */
template <typename T>
void ignore_unused_variable(const T& value) {
  do {
    (void)(value);
  } while (false);
}

}  // namespace opossum
