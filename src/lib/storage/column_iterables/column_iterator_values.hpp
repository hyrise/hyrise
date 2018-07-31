#pragma once

#include <boost/blank.hpp>

#include <cstddef>

#include "types.hpp"

namespace opossum {

/**
 * @brief Return type of column iterators
 *
 * The class documents the interface that can be expected
 * of an object returned by a column iterator.
 * The actual returned method will however be a sub-class
 * in order to avoid expensive virtual method calls.
 * For this reason, all methods in sub-classes should be
 * declared using `final`.
 */
template <typename T>
class AbstractColumnIteratorValue {
 public:
  using Type = T;

 public:
  virtual const T& value() const = 0;
  virtual bool is_null() const = 0;

  /**
   * @brief Returns the chunk offset of the current value.
   *
   * The chunk offset can point either into a reference column,
   * if returned by a point-access iterator, or into an actual data column.
   */
  virtual const ChunkOffset& chunk_offset() const = 0;
};

/**
 * @brief The most generic column iterator value
 *
 * Used in most column iterators.
 */
template <typename T>
class ColumnIteratorValue : public AbstractColumnIteratorValue<T> {
 public:
  static constexpr bool Nullable = true;

  ColumnIteratorValue(const T& value, const bool null_value, const ChunkOffset& chunk_offset)
      : _value{value}, _null_value{null_value}, _chunk_offset{chunk_offset} {}

  const T& value() const final { return _value; }
  bool is_null() const final { return _null_value; }
  const ChunkOffset& chunk_offset() const final { return _chunk_offset; }

 private:
  const T _value;
  const bool _null_value;
  const ChunkOffset _chunk_offset;
};

/**
 * @brief Column iterator value which is never null.
 *
 * Used when an underlying column (or data structure) cannot be null.
 */
template <typename T>
class NonNullColumnIteratorValue : public AbstractColumnIteratorValue<T> {
 public:
  static constexpr bool Nullable = false;

  NonNullColumnIteratorValue(const T& value, const ChunkOffset& chunk_offset)
      : _value{value}, _chunk_offset{chunk_offset} {}

  const T& value() const final { return _value; }
  bool is_null() const final { return false; }
  const ChunkOffset& chunk_offset() const final { return _chunk_offset; }

 private:
  const T _value;
  const ChunkOffset _chunk_offset;
};

/**
 * @brief Column iterator value without value information
 *
 * Used for data structures that only store if the entry is null or not.
 *
 * @see NullValueVectorIterable
 */
class ColumnIteratorNullValue : public AbstractColumnIteratorValue<boost::blank> {
 public:
  static constexpr bool Nullable = true;

  ColumnIteratorNullValue(const bool null_value, const ChunkOffset& chunk_offset)
      : _null_value{null_value}, _chunk_offset{chunk_offset} {}

  const boost::blank& value() const final { return _blank; }
  bool is_null() const final { return _null_value; }
  const ChunkOffset& chunk_offset() const final { return _chunk_offset; }

 private:
  static constexpr auto _blank = boost::blank{};
  const bool _null_value;
  const ChunkOffset _chunk_offset;
};

}  // namespace opossum
