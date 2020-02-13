#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "base_test.hpp"

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

TEST_F(CommitContextTest, HasNextReturnsTrueAfterNextHasBeenSet) {
  auto context = std::make_unique<CommitContext>(0u);

  auto next_context = std::make_shared<CommitContext>(context->commit_id() + 1u);

  EXPECT_TRUE(context->try_set_next(next_context));

  EXPECT_EQ(context->has_next(), true);
}

TEST_F(CommitContextTest, TrySetNextFailsIfNotNullptr) {
  auto context = std::make_unique<CommitContext>(0u);

  auto next_context = std::make_shared<CommitContext>(context->commit_id() + 1u);

  EXPECT_TRUE(context->try_set_next(next_context));

  next_context = std::make_shared<CommitContext>(context->commit_id() + 1u);

  EXPECT_FALSE(context->try_set_next(next_context));
}

}  // namespace opossum
