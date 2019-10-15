#include "../../../base_test.hpp"
#include "jit_mocks.hpp"
#include "operators/jit_operator/operators/jit_limit.hpp"

namespace opossum {

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

  // Limit not reached
  sink->reset();
  source->emit(context);
  ASSERT_EQ(context.chunk_size, chunk_size);
  ASSERT_EQ(context.limit_rows, 1);
  ASSERT_TRUE(sink->consume_was_called());

  // Limit reached with next tuple
  sink->reset();
  source->emit(context);
  // Once the limit is reached, the chunk_size is set to 0.
  ASSERT_EQ(context.chunk_size, 0);
  ASSERT_EQ(context.limit_rows, 0);
  ASSERT_TRUE(sink->consume_was_called());
}

}  // namespace opossum
