#define UNW_LOCAL_ONLY
#include <cxxabi.h>
#include <gtest/gtest.h>
#include <libunwind.h>

#include "utils/assert.hpp"

namespace opossum {

[[noreturn]] void skip_test() {
  // The idea is to unwind the stack, search for the googletest method that called the test and reset the instruction
  // pointer to that stack frame.
  // cf. https://stackoverflow.com/questions/53765909/googletest-how-to-skip-test-from-an-inner-method/53777804#53777804

  unw_cursor_t cursor;
  unw_context_t context;

  unw_getcontext(&context);
  unw_init_local(&cursor, &context);

  while (unw_step(&cursor)) {
    unw_word_t off;

    char symbol[256] = {"<unknown>"};
    char* name = symbol;

    if (!unw_get_proc_name(&cursor, symbol, sizeof(symbol), &off)) {
      int status;
      if ((name = abi::__cxa_demangle(symbol, nullptr, nullptr, &status)) == nullptr) name = symbol;
    }

    if (std::string(name) == "testing::Test::Run()") {
      ::testing::internal::AssertHelper(::testing::TestPartResult::kSkip, __FILE__, __LINE__, "skipped") =
          ::testing::Message();
      unw_resume(&cursor);
    }

    if (name != symbol) free(name);
  }

  Fail("Did not find test method on the stack, could not skip test");
}

}  // namespace opossum