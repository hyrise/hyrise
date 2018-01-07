#pragma once

#include <boost/hana/type.hpp>

#include <memory>

#include "storage/base_value_column.hpp"
#include "storage/encoded_columns/column_encoding_type.hpp"

#include "all_type_variant.hpp"
#include "resolve_type.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

namespace hana = boost::hana;

/**
 * @brief Base class of all column encoders
 *
 * Use the column_encoder.template.hpp to add new implementations!
 */
class BaseColumnEncoder {
 public:
  /**
   * @brief Returns true if the encoder supports the given data type.
   */
  virtual bool supports(DataType data_type) const = 0;

  /**
   * @brief Encodes a value column with the given data type.
   *
   * @return encoded column if data type is supported else throws exception
   */
  virtual std::shared_ptr<BaseColumn> encode(DataType data_type, const std::shared_ptr<BaseValueColumn>& column) = 0;
};

template <typename Derived>
class ColumnEncoder : public BaseColumnEncoder {
 public:
  /**
   * @defgroup Virtual interface implementation
   * @{
   */
  bool supports(DataType data_type) const final {
    auto result = bool{};
    resolve_data_type(data_type, [&](auto type_obj) { result = this->supports(type_obj); });
    return result;
  }

  // Resolves the data type and calls the appropriate instantiation of encode().
  std::shared_ptr<BaseColumn> encode(DataType data_type, const std::shared_ptr<BaseValueColumn>& column) final {
    auto encoded_column = std::shared_ptr<BaseColumn>{};
    resolve_data_type(data_type, [&](auto type_obj) {
      const auto data_type_supported = this->supports(type_obj);
      // clang-format off
      if constexpr (decltype(data_type_supported)::value) {
        /**
         * The templated method encode() where the actual encoding happens
         * is only instantiated for data types supported by the encoding type.
         */
        encoded_column = this->encode(type_obj, column);
      } else {
        Fail("Passed data type not supported by encoding.");
      }
      // clang-format on
    });

    return encoded_column;
  }
  /**@}*/

 public:
  /**
   * @defgroup Non-virtual interface
   * @{
   */

  /**
   * @return an integral constant implicitly convertible to bool
   *
   * Since this method is used in compile-time expression,
   * it cannot simply return bool.
   *
   * Hint: Use decltype(result)::value if you want to use the result
   *       in a constant expression such as constexpr-if.
   */
  template <typename ColumnDataType>
  auto supports(hana::basic_type<ColumnDataType> data_type) const {
    return encoding_supports_data_type(Derived::_encoding_type, data_type);
  }

  /**
   * @brief Encodes a value column with the given data type.
   *
   * Compiles only for supported data types.
   */
  template <typename ColumnDataType>
  std::shared_ptr<BaseColumn> encode(hana::basic_type<ColumnDataType> data_type,
                                     const std::shared_ptr<BaseValueColumn>& value_column) {
    static_assert(decltype(supports(data_type))::value);

    return _self()._on_encode(std::static_pointer_cast<ValueColumn<ColumnDataType>>(value_column));
  }
  /**@}*/

 private:
  Derived& _self() { return static_cast<Derived&>(*this); }
  const Derived& _self() const { return static_cast<const Derived&>(*this); }
};

}  // namespace opossum
