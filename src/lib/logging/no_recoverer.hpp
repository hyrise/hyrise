#pragma once

#include "abstract_recoverer.hpp"
#include "types.hpp"
#include "utils/singleton.hpp"

namespace opossum {

/*
 *  Used when logging is turned off, so there is nothing to recover.
 */

class NoRecoverer : public Singleton<NoRecoverer>, public AbstractRecoverer {
  friend class Singleton;
 public:
  NoRecoverer(const NoRecoverer&) = delete;
  NoRecoverer& operator=(const NoRecoverer&) = delete;

  uint32_t recover() final { return 0u; };

 private:
  NoRecoverer() {}
};

}  // namespace opossum
