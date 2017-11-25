#include <memory>
#include <string>
#include <utility>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "../lib/storage/base_column.hpp"
#include "../lib/storage/dictionary_column.hpp"
#include "../lib/storage/dictionary_compression.hpp"
#include "../lib/storage/fitted_attribute_vector.hpp"
#include "../lib/storage/value_column.hpp"

namespace opossum {

class StorageDictionaryColumnTest : public BaseTest {
 protected:
  std::shared_ptr<ValueColumn<int>> vc_int = std::make_shared<ValueColumn<int>>();
  std::shared_ptr<ValueColumn<std::string>> vc_str = std::make_shared<ValueColumn<std::string>>();
  std::shared_ptr<ValueColumn<double>> vc_double = std::make_shared<ValueColumn<double>>();
};

TEST_F(StorageDictionaryColumnTest, CompressColumnInt) {
  vc_int->append(4);
  vc_int->append(4);
  vc_int->append(3);
  vc_int->append(4);
  vc_int->append(5);
  vc_int->append(3);

  auto col = DictionaryCompression::compress_column(DataType::Int, vc_int);
  auto dict_col = std::dynamic_pointer_cast<DictionaryColumn<int>>(col);

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

  auto col = DictionaryCompression::compress_column(DataType::String, vc_str);
  auto dict_col = std::dynamic_pointer_cast<DictionaryColumn<std::string>>(col);

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

  auto col = DictionaryCompression::compress_column(DataType::Double, vc_double);
  auto dict_col = std::dynamic_pointer_cast<DictionaryColumn<double>>(col);

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

TEST_F(StorageDictionaryColumnTest, CompressNullableColumnInt) {
  vc_int = std::make_shared<ValueColumn<int>>(true);

  vc_int->append(4);
  vc_int->append(4);
  vc_int->append(3);
  vc_int->append(4);
  vc_int->append(NULL_VALUE);
  vc_int->append(3);

  auto col = DictionaryCompression::compress_column(DataType::Int, vc_int);
  auto dict_col = std::dynamic_pointer_cast<DictionaryColumn<int>>(col);

  // Test attribute_vector size
  EXPECT_EQ(dict_col->size(), 6u);

  // Test dictionary size (uniqueness)
  EXPECT_EQ(dict_col->unique_values_count(), 2u);

  // Test sorting
  auto dict = dict_col->dictionary();
  EXPECT_EQ((*dict)[0], 3);
  EXPECT_EQ((*dict)[1], 4);

  // Test retrieval of null value
  EXPECT_TRUE(variant_is_null((*dict_col)[4]));
}

TEST_F(StorageDictionaryColumnTest, LowerUpperBound) {
  for (int i = 0; i <= 10; i += 2) vc_int->append(i);

  auto col = DictionaryCompression::compress_column(DataType::Int, vc_int);
  auto dict_col = std::dynamic_pointer_cast<DictionaryColumn<int>>(col);

  // Test for template-type as parameter
  EXPECT_EQ(dict_col->lower_bound(4), (ValueID)2);
  EXPECT_EQ(dict_col->upper_bound(4), (ValueID)3);

  EXPECT_EQ(dict_col->lower_bound(5), (ValueID)3);
  EXPECT_EQ(dict_col->upper_bound(5), (ValueID)3);

  EXPECT_EQ(dict_col->lower_bound(15), INVALID_VALUE_ID);
  EXPECT_EQ(dict_col->upper_bound(15), INVALID_VALUE_ID);

  // Test for AllTypeVariant as parameter
  EXPECT_EQ(dict_col->lower_bound(AllTypeVariant(4)), (ValueID)2);
  EXPECT_EQ(dict_col->upper_bound(AllTypeVariant(4)), (ValueID)3);

  EXPECT_EQ(dict_col->lower_bound(AllTypeVariant(5)), (ValueID)3);
  EXPECT_EQ(dict_col->upper_bound(AllTypeVariant(5)), (ValueID)3);

  EXPECT_EQ(dict_col->lower_bound(AllTypeVariant(15)), INVALID_VALUE_ID);
  EXPECT_EQ(dict_col->upper_bound(AllTypeVariant(15)), INVALID_VALUE_ID);
}

TEST_F(StorageDictionaryColumnTest, FittedAttributeVectorSize) {
  vc_int->append(0);
  vc_int->append(1);
  vc_int->append(2);

  auto col = DictionaryCompression::compress_column(DataType::Int, vc_int);
  auto dict_col = std::dynamic_pointer_cast<DictionaryColumn<int>>(col);
  auto attribute_vector_uint8_t =
      std::dynamic_pointer_cast<const FittedAttributeVector<uint8_t>>(dict_col->attribute_vector());
  auto attribute_vector_uint16_t =
      std::dynamic_pointer_cast<const FittedAttributeVector<uint16_t>>(dict_col->attribute_vector());

  EXPECT_NE(attribute_vector_uint8_t, nullptr);
  EXPECT_EQ(attribute_vector_uint16_t, nullptr);

  for (int i = 3; i < 257; ++i) {
    vc_int->append(i);
  }

  col = DictionaryCompression::compress_column(DataType::Int, vc_int);
  dict_col = std::dynamic_pointer_cast<DictionaryColumn<int>>(col);
  attribute_vector_uint8_t =
      std::dynamic_pointer_cast<const FittedAttributeVector<uint8_t>>(dict_col->attribute_vector());
  attribute_vector_uint16_t =
      std::dynamic_pointer_cast<const FittedAttributeVector<uint16_t>>(dict_col->attribute_vector());

  EXPECT_EQ(attribute_vector_uint8_t, nullptr);
  EXPECT_NE(attribute_vector_uint16_t, nullptr);
}

}  // namespace opossum
