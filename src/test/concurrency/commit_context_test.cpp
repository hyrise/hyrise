#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "concurrency/commit_context.hpp"
#include "types.hpp"

namespace opossum {

class CommitContextTest : public BaseTest {
 protected:
  void SetUp() override {}
};

TEST_F(CommitContextTest, HasNextReturnsFalse) {
  auto context = std::make_unique<CommitContext>(0u);

  EXPECT_EQ(context->has_next(), false);
}

TEST_F(CommitContextTest, HasNextReturnsTrueAfterCallingGetOrCreateNext) {
  auto context = std::make_unique<CommitContext>(0u);

  context->get_or_create_next();

  EXPECT_EQ(context->has_next(), true);
}

TEST_F(CommitContextTest, CidOfNextIncrementedByOne) {
  auto context = std::make_unique<CommitContext>(0u);

  auto next = context->get_or_create_next();

  EXPECT_EQ(context->commit_id() + 1u, next->commit_id());
}

}  // namespace opossum
