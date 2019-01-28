#include "null_value.hpp"

#include <iostream>

namespace std {
std::ostream& operator<<(std::ostream& out, const opossum::NullValue&) {
  out << "NULL";
  return out;
}

}  // namespace std
