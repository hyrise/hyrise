#include <memory>
#include <string>
#include <utility>

#include "gtest/gtest.h"

#include "../../lib/storage/base_column.hpp"
#include "../../lib/storage/dictionary_column.hpp"
#include "../../lib/storage/value_column.hpp"

class StorageDictionaryColumnTest : public ::testing::Test {
 protected:
  std::shared_ptr<opossum::ValueColumn<int>> vc_int = std::make_shared<opossum::ValueColumn<int>>();
  std::shared_ptr<opossum::ValueColumn<std::string>> vc_str = std::make_shared<opossum::ValueColumn<std::string>>();
  std::shared_ptr<opossum::ValueColumn<double>> vc_double = std::make_shared<opossum::ValueColumn<double>>();
};

TEST_F(StorageDictionaryColumnTest, CompressColumnInt) {
  vc_int->append(4);
  vc_int->append(4);
  vc_int->append(3);
  vc_int->append(4);
  vc_int->append(5);
  vc_int->append(3);

  auto col = opossum::make_shared_by_column_type<opossum::BaseColumn, opossum::DictionaryColumn>("int", vc_int);
  auto dict_col = std::dynamic_pointer_cast<opossum::DictionaryColumn<int>>(col);

  // Test attribute_vector size
  EXPECT_EQ(dict_col->size(), 6u);

  // Test dictionary size (uniqueness)
  EXPECT_EQ(dict_col->unique_values_count(), 3u);

  // Test sorting
  auto dict = dict_col->dictionary();
  EXPECT_EQ((*dict)[0], 3);
  EXPECT_EQ((*dict)[1], 4);
  EXPECT_EQ((*dict)[2], 5);
}

TEST_F(StorageDictionaryColumnTest, CompressColumnString) {
  vc_str->append("Bill");
  vc_str->append("Steve");
  vc_str->append("Alexander");
  vc_str->append("Steve");
  vc_str->append("Hasso");
  vc_str->append("Bill");

  auto col = opossum::make_shared_by_column_type<opossum::BaseColumn, opossum::DictionaryColumn>("string", vc_str);
  auto dict_col = std::dynamic_pointer_cast<opossum::DictionaryColumn<std::string>>(col);

  // Test attribute_vector size
  EXPECT_EQ(dict_col->size(), 6u);

  // Test dictionary size (uniqueness)
  EXPECT_EQ(dict_col->unique_values_count(), 4u);

  // Test sorting
  auto dict = dict_col->dictionary();
  EXPECT_EQ((*dict)[0], "Alexander");
  EXPECT_EQ((*dict)[1], "Bill");
  EXPECT_EQ((*dict)[2], "Hasso");
  EXPECT_EQ((*dict)[3], "Steve");
}

TEST_F(StorageDictionaryColumnTest, CompressColumnDouble) {
  vc_double->append(0.9);
  vc_double->append(1.0);
  vc_double->append(1.0);
  vc_double->append(1.1);
  vc_double->append(0.9);
  vc_double->append(1.1);

  auto col = opossum::make_shared_by_column_type<opossum::BaseColumn, opossum::DictionaryColumn>("double", vc_double);
  auto dict_col = std::dynamic_pointer_cast<opossum::DictionaryColumn<double>>(col);

  // Test attribute_vector size
  EXPECT_EQ(dict_col->size(), 6u);

  // Test dictionary size (uniqueness)
  EXPECT_EQ(dict_col->unique_values_count(), 3u);

  // Test sorting
  auto dict = dict_col->dictionary();
  EXPECT_EQ((*dict)[0], 0.9);
  EXPECT_EQ((*dict)[1], 1.0);
  EXPECT_EQ((*dict)[2], 1.1);
}

TEST_F(StorageDictionaryColumnTest, GetValueId) {
  vc_int->append(4);
  vc_int->append(4);
  vc_int->append(3);
  vc_int->append(4);
  vc_int->append(5);
  vc_int->append(3);

  auto col = opossum::make_shared_by_column_type<opossum::BaseColumn, opossum::DictionaryColumn>("int", vc_int);
  auto dict_col = std::dynamic_pointer_cast<opossum::DictionaryColumn<int>>(col);

  EXPECT_EQ(dict_col->get_value_id(2), opossum::INVALID_VALUE_ID);
  EXPECT_EQ(dict_col->get_value_id(3), 0u);
  EXPECT_EQ(dict_col->get_value_id(4), 1u);
  EXPECT_EQ(dict_col->get_value_id(5), 2u);
  EXPECT_EQ(dict_col->get_value_id(6), opossum::INVALID_VALUE_ID);
}

TEST_F(StorageDictionaryColumnTest, GetValueIdRange) {
  vc_int->append(2);
  vc_int->append(4);
  vc_int->append(5);
  vc_int->append(6);
  vc_int->append(8);

  auto col = opossum::make_shared_by_column_type<opossum::BaseColumn, opossum::DictionaryColumn>("int", vc_int);
  auto dict_col = std::dynamic_pointer_cast<opossum::DictionaryColumn<int>>(col);

  // Both in dict
  std::pair<opossum::ValueID, opossum::ValueID> expected_result = {0, 4};
  auto result = dict_col->get_value_id_range(2, 8);
  EXPECT_EQ(result, expected_result);

  expected_result = {0, 4};
  result = dict_col->get_value_id_range(8, 2);
  EXPECT_EQ(result, expected_result);

  // Smaller not in dict 1)
  expected_result = {0, 4};
  result = dict_col->get_value_id_range(1, 8);
  EXPECT_EQ(result, expected_result);

  expected_result = {0, 4};
  result = dict_col->get_value_id_range(8, 1);
  EXPECT_EQ(result, expected_result);

  // Smaller not in dict 2)
  expected_result = {1, 4};
  result = dict_col->get_value_id_range(3, 8);
  EXPECT_EQ(result, expected_result);

  // Larger not in dict 1)
  expected_result = {0, 3};
  result = dict_col->get_value_id_range(2, 7);
  EXPECT_EQ(result, expected_result);

  // Larger not in dict 2)
  expected_result = {0, 4};
  result = dict_col->get_value_id_range(2, 9);
  EXPECT_EQ(result, expected_result);

  // Both not in dict
  expected_result = {1, 3};
  result = dict_col->get_value_id_range(3, 7);
  EXPECT_EQ(result, expected_result);

  expected_result = {1, 3};
  result = dict_col->get_value_id_range(7, 3);
  EXPECT_EQ(result, expected_result);

  // Equal, both in dict
  expected_result = {2, 2};
  result = dict_col->get_value_id_range(5, 5);
  EXPECT_EQ(result, expected_result);

  // Equal, both not in dict
  expected_result = {4, 3};
  result = dict_col->get_value_id_range(7, 7);
  EXPECT_EQ(result, expected_result);
}

TEST_F(StorageDictionaryColumnTest, FittedAttributeVectorSize) {
  vc_int->append(0);
  vc_int->append(1);
  vc_int->append(2);

  auto col = opossum::make_shared_by_column_type<opossum::BaseColumn, opossum::DictionaryColumn>("int", vc_int);
  auto dict_col = std::dynamic_pointer_cast<opossum::DictionaryColumn<int>>(col);
  auto attribute_vector_uint8_t =
      std::dynamic_pointer_cast<const opossum::FittedAttributeVector<uint8_t>>(dict_col->attribute_vector());
  auto attribute_vector_uint16_t =
      std::dynamic_pointer_cast<const opossum::FittedAttributeVector<uint16_t>>(dict_col->attribute_vector());

  EXPECT_NE(attribute_vector_uint8_t, nullptr);
  EXPECT_EQ(attribute_vector_uint16_t, nullptr);

  for (int i = 3; i < 257; ++i) {
    vc_int->append(i);
  }

  col = opossum::make_shared_by_column_type<opossum::BaseColumn, opossum::DictionaryColumn>("int", vc_int);
  dict_col = std::dynamic_pointer_cast<opossum::DictionaryColumn<int>>(col);
  attribute_vector_uint8_t =
      std::dynamic_pointer_cast<const opossum::FittedAttributeVector<uint8_t>>(dict_col->attribute_vector());
  attribute_vector_uint16_t =
      std::dynamic_pointer_cast<const opossum::FittedAttributeVector<uint16_t>>(dict_col->attribute_vector());

  EXPECT_EQ(attribute_vector_uint8_t, nullptr);
  EXPECT_NE(attribute_vector_uint16_t, nullptr);
}
