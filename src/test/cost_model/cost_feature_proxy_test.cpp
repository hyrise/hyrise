#include <cmath>
#include <string>

#include "gtest/gtest.h"

#include "cost_model/cost_feature_lqp_node_proxy.hpp"
#include "cost_model/cost_feature_operator_proxy.hpp"
#include "expression/expression_functional.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "operators/get_table.hpp"
#include "operators/join_hash.hpp"
#include "operators/table_scan.hpp"
#include "storage/storage_manager.hpp"
#include "utils/load_table.hpp"

using namespace opossum::expression_functional;  // NOLINT
using namespace std::string_literals;            // NOLINT

namespace opossum {

/**
 *
 */
template <typename ProxyType>
class CostFeatureProxyTest : public ::testing::Test {
 public:
  struct Proxies final {
    std::shared_ptr<AbstractCostFeatureProxy> join_proxy;
    std::shared_ptr<AbstractCostFeatureProxy> predicate_proxy;
  };

  Proxies get_proxies() const {
    // clang-format off
    if constexpr (std::is_same_v<ProxyType, CostFeatureLQPNodeProxy>) {
      auto customer = StoredTableNode::make("customer");
      auto nation = StoredTableNode::make("nation");

      auto join = JoinNode::make(JoinMode::Inner,
                                 equals_(customer->get_column("c_nationkey"s),
                                                                         nation->get_column("n_nationkey"s)),
                                 customer,
                                 nation);
      auto predicate = PredicateNode::make(not_equals_(nation->get_column("n_name"s),
                                           "ALGERIA"),
                                           join);

      return Proxies{std::make_shared<CostFeatureLQPNodeProxy>(join),
                     std::make_shared<CostFeatureLQPNodeProxy>(predicate)};
    } else {
      auto customer = std::make_shared<GetTable>("customer");
      customer->execute();

      auto nation = std::make_shared<GetTable>("nation");
      nation->execute();

      auto join = std::make_shared<JoinHash>(customer, nation, JoinMode::Inner,
                                 ColumnIDPair{3, 0},
                                 PredicateCondition::Equals);
      join->execute();

      auto predicate = std::make_shared<TableScan>(join, ColumnID{1},
                                           PredicateCondition::NotEquals,
                                           "ALGERIA");
      predicate->execute();

      return Proxies{std::make_shared<CostFeatureOperatorProxy>(join),
                     std::make_shared<CostFeatureOperatorProxy>(predicate)};
    }
    // clang-format on
  }

  void SetUp() override {
    StorageManager::get().add_table("customer", load_table("src/test/tables/tpch/sf-0.001/customer.tbl"));
    StorageManager::get().add_table("nation", load_table("src/test/tables/tpch/sf-0.001/nation.tbl"));
  }

  void TearDown() override { StorageManager::reset(); }
};

typedef ::testing::Types<CostFeatureLQPNodeProxy, CostFeatureOperatorProxy> ProxyTypes;
TYPED_TEST_CASE(CostFeatureProxyTest, ProxyTypes);

TYPED_TEST(CostFeatureProxyTest, AllFeatures) {
  const auto proxies = CostFeatureProxyTest<TypeParam>::get_proxies();

  EXPECT_EQ(proxies.join_proxy->extract_feature(CostFeature::LeftInputRowCount).scalar(), 150);
  EXPECT_EQ(proxies.join_proxy->extract_feature(CostFeature::RightInputRowCount).scalar(), 25);
  EXPECT_EQ(proxies.join_proxy->extract_feature(CostFeature::InputRowCountProduct).scalar(), 150 * 25);
  EXPECT_EQ(proxies.join_proxy->extract_feature(CostFeature::LeftInputReferenceRowCount).scalar(), 0);
  EXPECT_EQ(proxies.join_proxy->extract_feature(CostFeature::RightInputReferenceRowCount).scalar(), 0);
  EXPECT_GT(proxies.predicate_proxy->extract_feature(CostFeature::LeftInputReferenceRowCount).scalar(), 0);
  EXPECT_FLOAT_EQ(proxies.join_proxy->extract_feature(CostFeature::LeftInputRowCountLogN).scalar(),
                  150 * std::log(150));
  EXPECT_FLOAT_EQ(proxies.join_proxy->extract_feature(CostFeature::RightInputRowCountLogN).scalar(), 25 * std::log(25));
  EXPECT_EQ(proxies.join_proxy->extract_feature(CostFeature::LargerInputRowCount).scalar(), 150);
  EXPECT_EQ(proxies.join_proxy->extract_feature(CostFeature::SmallerInputRowCount).scalar(), 25);
  EXPECT_EQ(proxies.join_proxy->extract_feature(CostFeature::LargerInputReferenceRowCount).scalar(), 0);
  EXPECT_EQ(proxies.join_proxy->extract_feature(CostFeature::SmallerInputReferenceRowCount).scalar(), 0);
  EXPECT_GT(proxies.join_proxy->extract_feature(CostFeature::OutputRowCount).scalar(), 0);
  EXPECT_EQ(proxies.join_proxy->extract_feature(CostFeature::OutputReferenceRowCount).scalar(), 0);
  EXPECT_GT(proxies.predicate_proxy->extract_feature(CostFeature::OutputReferenceRowCount).scalar(), 0);
  EXPECT_EQ(proxies.join_proxy->extract_feature(CostFeature::LeftDataType).data_type(), DataType::Int);
  EXPECT_EQ(proxies.join_proxy->extract_feature(CostFeature::RightDataType).data_type(), DataType::Int);
  EXPECT_EQ(proxies.join_proxy->extract_feature(CostFeature::PredicateCondition).predicate_condition(),
            PredicateCondition::Equals);
  EXPECT_EQ(proxies.predicate_proxy->extract_feature(CostFeature::PredicateCondition).predicate_condition(),
            PredicateCondition::NotEquals);
  EXPECT_EQ(proxies.join_proxy->extract_feature(CostFeature::LeftInputIsReferences).boolean(), false);
  EXPECT_EQ(proxies.predicate_proxy->extract_feature(CostFeature::LeftInputIsReferences).boolean(), true);
  EXPECT_EQ(proxies.join_proxy->extract_feature(CostFeature::RightInputIsReferences).boolean(), false);
  EXPECT_EQ(proxies.predicate_proxy->extract_feature(CostFeature::RightOperandIsColumn).boolean(), false);
  EXPECT_EQ(proxies.join_proxy->extract_feature(CostFeature::LeftInputIsMajor).boolean(), true);
}

}  // namespace opossum
