#pragma once

#include "abstract_jittable.hpp"
#include "types.hpp"

namespace opossum {

/* The JitLimit operator limits the input to n rows.
 */
class JitLimit : public AbstractJittable {
 public:
  std::string description() const final;

 protected:
  void _consume(JitRuntimeContext& context) const final;
};

}  // namespace opossum
