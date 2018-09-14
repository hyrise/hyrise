#pragma once

#include "abstract_recoverer.hpp"
#include "types.hpp"

namespace opossum {

/*
 *  Used when logging is turned off, so there is nothing to recover.
 */

class NoRecoverer : public AbstractRecoverer {
 public:
  NoRecoverer(const NoRecoverer&) = delete;
  NoRecoverer& operator=(const NoRecoverer&) = delete;

  static NoRecoverer& get() {
    static NoRecoverer instance;
    return instance;
  };

  uint32_t recover() final { return 0u; };

 private:
  NoRecoverer() {}
};

}  // namespace opossum
