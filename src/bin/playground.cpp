#include <iostream>
#include <memory>
#include <string>

template <typename Lambda>
inline void Assert(bool expr, Lambda msg) {
  if (expr) {
    return;
  }
  throw std::logic_error(msg());
}
template <typename Lambda>
inline void Fail(Lambda msg) {
  throw std::logic_error(msg());
}

int test_function() {
  std::cout << "Message evaluated\n";
  return 42;
}

template <typename Lambda>
inline void DebugAssert(bool expr, Lambda msg) {
  if (!IS_DEBUG)
    ;
  else
    Assert(expr, msg);
}

template <typename Lambda>
inline void DebugFail(Lambda msg) {
  if (!IS_DEBUG)
    ;
  else
    Fail(msg);
}
int main() {
  DebugAssert(false, []() { return std::string("Failed with code ") + std::to_string(test_function()); });
}
