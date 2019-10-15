#pragma once

#include "operators/jit_operator/operators/abstract_jittable.hpp"

namespace opossum {
  // Mock JitOperator that records whether tuples are passed to it
  class MockSink : public AbstractJittable {
   public:
    std::string description() const final { return "MockSink"; }

    void reset() const { _consume_was_called = false; }

    bool consume_was_called() const { return _consume_was_called; }

   private:
    void _consume(JitRuntimeContext& context) const final { _consume_was_called = true; }

    // Must be static, since _consume is const
    static bool _consume_was_called;
  };

  bool MockSink::_consume_was_called = false;

  // Mock JitOperator that passes on individual tuples
  class MockSource : public AbstractJittable {
   public:
    std::string description() const final { return "MockSource"; }

    void emit(JitRuntimeContext& context) { _emit(context); }

   private:
    void _consume(JitRuntimeContext& context) const final {}
  };
}