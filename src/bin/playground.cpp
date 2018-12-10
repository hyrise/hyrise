#include <iostream>

#include "types.hpp"
#include "import_export/csv_parser.hpp"
#include "utils/format_duration.hpp"
#include "utils/timer.hpp"

using namespace opossum;  // NOLINT

int main() {
  Timer timer;
  CsvParser{}.parse("imdb/name.csv");

  std::cout << "Duration: " << format_duration(std::chrono::duration_cast<std::chrono::nanoseconds>(timer.lap())) << std::endl;

  return 0;
}
