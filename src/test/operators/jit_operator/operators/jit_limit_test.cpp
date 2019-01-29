#include "../../../base_test.hpp"
#include "operators/jit_operator/operators/jit_limit.hpp"

namespace opossum {

namespace {

// Mock JitOperator that records whether tuples are passed to it
class MockSink : public AbstractJittable {
 public:
  MockSink() : AbstractJittable(JitOperatorType::Write) {}

  std::string description() const final { return "MockSink"; }

 private:
  void _consume(JitRuntimeContext& context) const final {}
};

// Mock JitOperator that passes on individual tuples
class MockSource : public AbstractJittable {
 public:
  MockSource() : AbstractJittable(JitOperatorType::Read) {}

  std::string description() const final { return "MockSource"; }

  void emit(JitRuntimeContext& context) { _emit(context); }

 private:
  void _consume(JitRuntimeContext& context) const final {}
};

}  // namespace

class JitLimitTest : public BaseTest {};

TEST_F(JitLimitTest, FiltersTuplesAccordingToLimit) {
  const uint32_t chunk_size{123};

  JitRuntimeContext context;
  context.chunk_size = chunk_size;
  context.limit_rows = 2;

  auto source = std::make_shared<MockSource>();
  auto limit = std::make_shared<JitLimit>();
  auto sink = std::make_shared<MockSink>();

  // Link operators to pipeline
  source->set_next_operator(limit);
  limit->set_next_operator(sink);

  // limit not reached
  source->emit(context);
  ASSERT_EQ(context.chunk_size, chunk_size);
  ASSERT_EQ(context.limit_rows, 1);

  // limit reached
  source->emit(context);
  ASSERT_EQ(context.chunk_size, 0);
  ASSERT_EQ(context.limit_rows, 0);
}

}  // namespace opossum
