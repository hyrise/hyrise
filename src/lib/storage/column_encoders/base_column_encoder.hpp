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

class BaseColumnEncoder {
 public:
  virtual bool supports(DataType data_type) const = 0;
  virtual std::shared_ptr<BaseColumn> encode(DataType data_type, const std::shared_ptr<BaseValueColumn>& column) = 0;
};

template <typename Derived>
class ColumnEncoder : public BaseColumnEncoder {
 public:
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
      if constexpr (this->supports(type_obj)) {
        encoded_column = this->encode(type_obj, column);
      }
    });
    return encoded_column;
  }

 public:
  /**
   * You may use this as a value of Derived::supported_types
   * if the encoder supports all data types.
   */
  static constexpr auto all_data_types = data_types;

  template <typename ColumnDataType>
  bool supports(hana::basic_type<ColumnDataType> type) const {
    return hana::contains(Derived::_supported_types, type);
  }

  template <typename ColumnDataType>
  std::shared_ptr<BaseColumn> encode(hana::basic_type<ColumnDataType> type, const std::shared_ptr<BaseValueColumn>& value_column) {
    using ColumnDataType = typename decltype(type)::type;
    return self()._encode(std::static_pointer_cast<ValueColumn<ColumnDataType>>(value_column));
  }

 private:
  Derived& _self() { return static_cast<Derived&>(*this); }
  const Derived& _self() const { return static_cast<const Derived&>(*this); }
};

};
