#pragma once

#include <tbb/tbb.h>

#include <iostream>
#include <memory>
#include <utility>
#include <vector>
#include <ctime>
#include <iomanip>

namespace opossum {

template <typename>
class ValueColumn;

template <typename T>
std::shared_ptr<ValueColumn<T>> create_single_value_column(T value) {
  tbb::concurrent_vector<T> column;
  column.push_back(value);

  return std::make_shared<ValueColumn<T>>(std::move(column));
}

std::string time_stamp() {
  auto t = std::time(nullptr);
  auto tm = *std::localtime(&t);

  std::ostringstream oss;
  oss << std::put_time(&tm, "%Y-%m-%d %H:%M:%S");
  return oss.str();
}

std::string trim(const std::string & str) {
  size_t first = str.find_first_not_of(' ');
  if (std::string::npos == first)
  {
    return "";
  }
  size_t last = str.find_last_not_of(' ');
  return str.substr(first, (last - first + 1));
}

}  // namespace opossum
