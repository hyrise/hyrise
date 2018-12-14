#pragma once

namespace opossum {
// Skips the current (google)test. Use it *really, really, really* carefully. If you have to ask, you shouldn't use this.
[[noreturn]] void skip_test();
}  // namespace opossum