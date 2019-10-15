#include "jit_mocks.hpp"

#include "operators/jit_operator/operators/abstract_jittable.hpp"

namespace opossum {

std::string MockSink::description() const { return "MockSink"; }

void MockSink::reset() const { _consume_was_called = false; }

bool MockSink::consume_was_called() const { return _consume_was_called; }

void MockSink::_consume(JitRuntimeContext& context) const { _consume_was_called = true; }

bool MockSink::_consume_was_called = false;

std::string MockSource::description() const { return "MockSource"; }

void MockSource::emit(JitRuntimeContext& context) { _emit(context); }

void MockSource::_consume(JitRuntimeContext& context) const {}

}  // namespace opossum
