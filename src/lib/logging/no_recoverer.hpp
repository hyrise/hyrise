#pragma once

#include "abstract_recoverer.hpp"
#include "types.hpp"

namespace opossum {

class NoRecoverer : public AbstractRecoverer {
 public:
  NoRecoverer(const NoRecoverer&) = delete;
  NoRecoverer& operator=(const NoRecoverer&) = delete;

  static NoRecoverer& get() {
    static NoRecoverer instance;
    return instance; 
  } ;

  // Recovers db from logfiles and returns the number of loaded tables
  uint32_t recover() final { return 0u; };

 private:
  NoRecoverer() {}
};

}  // namespace opossum
