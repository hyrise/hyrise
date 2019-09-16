#include "base_test.hpp"

#include "cost_estimation/feature/aggregate_features.hpp"
//#include "cost_estimation/feature/calibration_features.hpp"
#include "cost_estimation/feature/constant_hardware_features.hpp"
#include "cost_estimation/feature/join_features.hpp"
#include "cost_estimation/feature/projection_features.hpp"
#include "cost_estimation/feature/runtime_hardware_features.hpp"
#include "cost_estimation/feature/table_scan_features.hpp"

namespace opossum {

//template <typename T>
//class CalibrationFeatureTest : public BaseTest {
// protected:
//  void SetUp() override {}
//};
//
//using FeatureTypes = ::testing::Types<CalibrationAggregateFeatures, CalibrationConstantHardwareFeatures,
//                                      CalibrationFeatures, CalibrationJoinFeatures, CalibrationProjectionFeatures,
//                                      CalibrationRuntimeHardwareFeatures, CalibrationTableScanFeatures>;
//
//TYPED_TEST_SUITE(CalibrationFeatureTest, FeatureTypes, );  // NOLINT(whitespace/parens)
//
//TYPED_TEST(CalibrationFeatureTest, SimpleTest) {
//  const auto features = std::make_shared<TypeParam>();
//
//  const auto num_columns = features->feature_names.size();
//  const auto num_features = features->serialize(*features).size();
//
//  EXPECT_EQ(num_columns, num_features);
//}
//
//TYPED_TEST(CalibrationFeatureTest, SerializeNullopt) {
//  const auto features = std::make_shared<TypeParam>();
//
//  const auto num_columns = features->feature_names.size();
//  const auto num_features = features->serialize({}).size();
//
//  EXPECT_EQ(num_columns, num_features);
//}

}  // namespace opossum
