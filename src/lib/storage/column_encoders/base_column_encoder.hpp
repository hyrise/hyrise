#pragma once

#include <boost/hana/type.hpp>
#include <boost/hana/contains.hpp>
#include <boost/hana/tuple.hpp>

#include <memory>

#include "storage/base_value_column.hpp"

#include "all_type_variant.hpp"
#include "resolve_type.hpp"
#include "types.hpp"

namespace opossum {

/**
 * @brief Base class of all column encoders
 *
 * Use the column_encoder.template.hpp to add new implementations!
 */
class BaseColumnEncoder {
 public:
  /**
   * @brief Returns if the encoder supports the given data type.
   */
  virtual bool supports(DataType data_type) const = 0;

  /**
   * @brief Encodes a value column with the given data type.
   *
   * @return encoded column if data type is supported else nullptr
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
    resolve_data_type(data_type, [&] (auto type_obj) {
      result = this->supports(type_obj);
    });
    return result;
  }

  std::shared_ptr<BaseColumn> encode(DataType data_type, const std::shared_ptr<BaseValueColumn>& column) final {
    auto encoded_column = std::shared_ptr<BaseColumn>{};
    resolve_data_type(data_type, [&](auto type_obj) {
      const auto data_type_supported = this->supports(type_obj);
      if constexpr (decltype(data_type_supported)::value) {
        encoded_column = this->encode(type_obj, column);
      }
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
   * Hint: Use decltype(result)::value if you want to use the result
   *       in a constant expression such as constexpr-if.
   */
  template <typename ColumnDataType>
  auto supports(hana::basic_type<ColumnDataType> type) const {
    return hana::contains(Derived::_supported_types, type);
  }

  /**
   * @brief Encodes a value column with the given data type.
   *
   * Compiles only for supported data types.
   */
  template <typename ColumnDataType>
  std::shared_ptr<BaseColumn> encode(hana::basic_type<ColumnDataType> type, const std::shared_ptr<BaseValueColumn>& value_column) {
    static_assert(decltype(supports(type))::value);

    return _self()._encode(std::static_pointer_cast<ValueColumn<ColumnDataType>>(value_column));
  }
  /**@}*/

 protected:
  /**
   * You may use this as a value of Derived::_supported_types
   * if the encoder supports all data types.
   */
  static constexpr auto _all_data_types = data_types;

 private:
  Derived& _self() { return static_cast<Derived&>(*this); }
  const Derived& _self() const { return static_cast<const Derived&>(*this); }
};

};
