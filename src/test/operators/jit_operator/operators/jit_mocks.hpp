#pragma once

#include "operators/jit_operator/operators/abstract_jittable.hpp"

namespace opossum {
// Mock JitOperator that records whether tuples are passed to it
class MockSink : public AbstractJittable {
 public:
  std::string description() const final;

  void reset() const;

  bool consume_was_called() const;

 private:
  void _consume(JitRuntimeContext& context) const final;

  // Must be static, since _consume is const
  static bool _consume_was_called;
};

// Mock JitOperator that passes on individual tuples
class MockSource : public AbstractJittable {
 public:
  std::string description() const final;

  void emit(JitRuntimeContext& context);

 private:
  void _consume(JitRuntimeContext& context);
};
}  // namespace opossum