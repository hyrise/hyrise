#pragma once

#include <chrono>
#include <string>

namespace opossum {

/**
 * Starts a std::chrono::high_resolution_clock base timer on construction and returns and resets measurement when
 * lap() is called.
 */
class Timer final {
 public:
  Timer();

  /**
   * @return Time elapsed since construction or the last call to lap(), whichever was later
   */
  std::chrono::nanoseconds lap();

  /**
   * Calls lap() and formats the result into a human-readable form
   */
  std::string lap_formatted();

 private:
  std::chrono::high_resolution_clock::time_point _begin;
};

}  // namespace opossum
