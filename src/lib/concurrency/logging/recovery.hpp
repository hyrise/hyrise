#pragma once

#include "types.hpp"

namespace opossum {

class Recovery {
 public:   
  Recovery(const Recovery&) = delete;
  Recovery& operator=(const Recovery&) = delete;

  static Recovery& getInstance();

  void recover();

 private:
  Recovery(){};
};

}  // namespace opossum