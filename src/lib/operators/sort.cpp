#include "sort.hpp"

#include <algorithm>
#include <atomic>
#include <bit>
#include <chrono>
#include <compare>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <format>
#include <functional>
#include <iomanip>
#include <ios>
#include <limits>
#include <memory>
#include <numeric>
#include <optional>
#include <queue>
#include <ranges>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <valarray>
#include <vector>

#include <boost/accumulators/statistics_fwd.hpp>
#include <boost/algorithm/cxx11/iota.hpp>
#include <boost/range/numeric.hpp>
#include <boost/sort/pdqsort/pdqsort.hpp>

#include "all_type_variant.hpp"
#include "hyrise.hpp"
#include "operators/abstract_operator.hpp"
#include "operators/abstract_read_only_operator.hpp"
#include "operators/operator_performance_data.hpp"
#include "operators/print.hpp"
#include "resolve_type.hpp"
#include "scheduler/abstract_task.hpp"
#include "scheduler/job_task.hpp"
#include "scheduler/operator_task.hpp"
#include "storage/abstract_segment.hpp"
#include "storage/base_segment_accessor.hpp"
#include "storage/chunk.hpp"
#include "storage/pos_lists/row_id_pos_list.hpp"
#include "storage/reference_segment.hpp"
#include "storage/segment_accessor.hpp"
#include "storage/segment_iterate.hpp"
#include "storage/table.hpp"
#include "storage/value_segment.hpp"
#include "types.hpp"
#include "utils/assert.hpp"
#include "utils/pdqsort.h"
#include "utils/timer.hpp"

namespace {

using namespace hyrise;  // NOLINT

// Ceiling of integer division
size_t div_ceil(const size_t lhs, const ChunkOffset rhs) {
  DebugAssert(rhs > 0, "Divisor must be larger than 0.");
  return (lhs + rhs - 1u) / rhs;
}

// Given an unsorted_table and a pos_list that defines the output order, this materializes all columns in the table,
// creating chunks of output_chunk_size rows at maximum.
std::shared_ptr<Table> write_materialized_output_table(const std::shared_ptr<const Table>& unsorted_table,
                                                       RowIDPosList pos_list, const ChunkOffset output_chunk_size) {
  // First, we create a new table as the output
  // We have decided against duplicating MVCC data in https://github.com/hyrise/hyrise/issues/408
  auto output = std::make_shared<Table>(unsorted_table->column_definitions(), TableType::Data, output_chunk_size);

  // After we created the output table and initialized the column structure, we can start adding values. Because the
  // values are not sorted by input chunks anymore, we can't process them chunk by chunk. Instead the values are copied
  // column by column for each output row.

  const auto output_chunk_count = div_ceil(pos_list.size(), output_chunk_size);
  Assert(pos_list.size() == unsorted_table->row_count(), "Mismatching size of input table and PosList");

  // Vector of segments for each chunk
  auto output_segments_by_chunk = std::vector<Segments>{output_chunk_count};

  // Materialize column by column, starting a new ValueSegment whenever output_chunk_size is reached
  const auto input_chunk_count = unsorted_table->chunk_count();
  const auto output_column_count = unsorted_table->column_count();
  const auto row_count = unsorted_table->row_count();
  for (auto column_id = ColumnID{0}; column_id < output_column_count; ++column_id) {
    const auto column_data_type = output->column_data_type(column_id);
    const auto column_is_nullable = unsorted_table->column_is_nullable(column_id);

    resolve_data_type(column_data_type, [&](auto type) {
      using ColumnDataType = typename decltype(type)::type;

      auto chunk_it = output_segments_by_chunk.begin();
      auto current_segment_size = size_t{0};

      auto value_segment_value_vector = pmr_vector<ColumnDataType>{};
      auto value_segment_null_vector = pmr_vector<bool>{};

      {
        const auto next_chunk_size = std::min(static_cast<size_t>(output_chunk_size), static_cast<size_t>(row_count));
        value_segment_value_vector.reserve(next_chunk_size);
        if (column_is_nullable) {
          value_segment_null_vector.reserve(next_chunk_size);
        }
      }

      auto accessor_by_chunk_id =
          std::vector<std::unique_ptr<AbstractSegmentAccessor<ColumnDataType>>>(unsorted_table->chunk_count());
      for (auto input_chunk_id = ChunkID{0}; input_chunk_id < input_chunk_count; ++input_chunk_id) {
        const auto& abstract_segment = unsorted_table->get_chunk(input_chunk_id)->get_segment(column_id);
        accessor_by_chunk_id[input_chunk_id] = create_segment_accessor<ColumnDataType>(abstract_segment);
      }

      for (auto row_index = size_t{0}; row_index < row_count; ++row_index) {
        const auto [chunk_id, chunk_offset] = pos_list[row_index];

        auto& accessor = accessor_by_chunk_id[chunk_id];
        const auto typed_value = accessor->access(chunk_offset);
        const auto is_null = !typed_value;
        value_segment_value_vector.push_back(is_null ? ColumnDataType{} : typed_value.value());
        if (column_is_nullable) {
          value_segment_null_vector.push_back(is_null);
        }

        ++current_segment_size;

        // Check if value segment is full
        if (current_segment_size >= output_chunk_size) {
          current_segment_size = 0;

          std::shared_ptr<ValueSegment<ColumnDataType>> value_segment;
          if (column_is_nullable) {
            value_segment = std::make_shared<ValueSegment<ColumnDataType>>(std::move(value_segment_value_vector),
                                                                           std::move(value_segment_null_vector));
          } else {
            value_segment = std::make_shared<ValueSegment<ColumnDataType>>(std::move(value_segment_value_vector));
          }

          chunk_it->push_back(value_segment);
          value_segment_value_vector = pmr_vector<ColumnDataType>{};
          value_segment_null_vector = pmr_vector<bool>{};

          const auto next_chunk_size =
              std::min(static_cast<size_t>(output_chunk_size), static_cast<size_t>(row_count - row_index));
          value_segment_value_vector.reserve(next_chunk_size);
          if (column_is_nullable) {
            value_segment_null_vector.reserve(next_chunk_size);
          }

          ++chunk_it;
        }
      }

      // Last segment has not been added
      if (current_segment_size > 0) {
        std::shared_ptr<ValueSegment<ColumnDataType>> value_segment;
        if (column_is_nullable) {
          value_segment = std::make_shared<ValueSegment<ColumnDataType>>(std::move(value_segment_value_vector),
                                                                         std::move(value_segment_null_vector));
        } else {
          value_segment = std::make_shared<ValueSegment<ColumnDataType>>(std::move(value_segment_value_vector));
        }
        chunk_it->push_back(value_segment);
      }
    });
  }

  for (auto& segments : output_segments_by_chunk) {
    output->append_chunk(segments);
  }

  return output;
}

// Given an unsorted_table and an input_pos_list that defines the output order, this writes the output table as a
// reference table. This is usually faster, but can only be done if a single column in the input table does not
// reference multiple tables. An example where this restriction applies is the sorted result of a union between two
// tables. The restriction is needed because a ReferenceSegment can only reference a single table. It does, however,
// not necessarily apply to joined tables, so two tables referenced in different columns is fine.
//
// If unsorted_table is of TableType::Data, this is trivial and the input_pos_list is used to create the output
// reference table. If the input is already a reference table, the double indirection needs to be resolved.
std::shared_ptr<Table> write_reference_output_table(const std::shared_ptr<const Table>& unsorted_table,
                                                    RowIDPosList input_pos_list, const ChunkOffset output_chunk_size) {
  // First we create a new table as the output
  // We have decided against duplicating MVCC data in https://github.com/hyrise/hyrise/issues/408
  auto output_table = std::make_shared<Table>(unsorted_table->column_definitions(), TableType::References);

  const auto resolve_indirection = unsorted_table->type() == TableType::References;
  const auto column_count = output_table->column_count();

  const auto output_chunk_count = div_ceil(input_pos_list.size(), output_chunk_size);
  Assert(input_pos_list.size() == unsorted_table->row_count(), "Mismatching size of input table and PosList");

  // Vector of segments for each chunk
  auto output_segments_by_chunk = std::vector<Segments>(output_chunk_count, Segments(column_count));

  if (!resolve_indirection && input_pos_list.size() <= output_chunk_size) {
    // Shortcut: No need to copy RowIDs if input_pos_list is small enough and we do not need to resolve the indirection.
    const auto output_pos_list = std::make_shared<RowIDPosList>(std::move(input_pos_list));
    auto& output_segments = output_segments_by_chunk.at(0);
    for (auto column_id = ColumnID{0}; column_id < column_count; ++column_id) {
      output_segments[column_id] = std::make_shared<ReferenceSegment>(unsorted_table, column_id, output_pos_list);
    }
  } else {
    for (auto column_id = ColumnID{0}; column_id < column_count; ++column_id) {
      // To keep the implementation simple, we write the output ReferenceSegments column by column. This means that even
      // if input ReferenceSegments share a PosList, the output will contain independent PosLists. While this is
      // slightly more expensive to generate and slightly less efficient for following operators, we assume that the
      // lion's share of the work has been done before the Sort operator is executed and that the relative cost of this
      // is acceptable. In the future, this could be improved.
      auto output_pos_list = std::make_shared<RowIDPosList>();
      output_pos_list->reserve(output_chunk_size);

      // Collect all input segments for the current column
      const auto input_chunk_count = unsorted_table->chunk_count();
      auto input_segments = std::vector<std::shared_ptr<AbstractSegment>>(input_chunk_count);
      for (auto input_chunk_id = ChunkID{0}; input_chunk_id < input_chunk_count; ++input_chunk_id) {
        input_segments[input_chunk_id] = unsorted_table->get_chunk(input_chunk_id)->get_segment(column_id);
      }

      const auto first_reference_segment = std::dynamic_pointer_cast<ReferenceSegment>(input_segments.at(0));
      const auto referenced_table = resolve_indirection ? first_reference_segment->referenced_table() : unsorted_table;
      const auto referenced_column_id =
          resolve_indirection ? first_reference_segment->referenced_column_id() : column_id;

      // write_output_pos_list creates an output reference segment for a given ChunkID, ColumnID and PosList.
      auto output_chunk_id = ChunkID{0};
      const auto write_output_pos_list = [&] {
        DebugAssert(!output_pos_list->empty(), "Asked to write empty output_pos_list");
        output_segments_by_chunk.at(output_chunk_id)[column_id] =
            std::make_shared<ReferenceSegment>(referenced_table, referenced_column_id, output_pos_list);
        ++output_chunk_id;

        output_pos_list = std::make_shared<RowIDPosList>();
        if (output_chunk_id < output_chunk_count) {
          output_pos_list->reserve(output_chunk_size);
        }
      };

      // Iterate over rows in sorted input pos list, dereference them if necessary, and write a chunk every
      // `output_chunk_size` rows.
      const auto input_pos_list_size = input_pos_list.size();
      for (auto input_pos_list_offset = size_t{0}; input_pos_list_offset < input_pos_list_size;
           ++input_pos_list_offset) {
        const auto& row_id = input_pos_list[input_pos_list_offset];
        if (resolve_indirection) {
          const auto& input_reference_segment = static_cast<ReferenceSegment&>(*input_segments[row_id.chunk_id]);
          DebugAssert(input_reference_segment.referenced_table() == referenced_table,
                      "Input column references more than one table");
          DebugAssert(input_reference_segment.referenced_column_id() == referenced_column_id,
                      "Input column references more than one column");
          const auto& input_reference_pos_list = input_reference_segment.pos_list();
          output_pos_list->emplace_back((*input_reference_pos_list)[row_id.chunk_offset]);
        } else {
          output_pos_list->emplace_back(row_id);
        }

        if (output_pos_list->size() == output_chunk_size) {
          write_output_pos_list();
        }
      }
      if (!output_pos_list->empty()) {
        write_output_pos_list();
      }
    }
  }

  for (auto& segments : output_segments_by_chunk) {
    output_table->append_chunk(segments);
  }

  return output_table;
}

using PmrByteIter = std::byte*;

// Encodes the row value of
class ColumnDataEncoder : Noncopyable {
 public:
  explicit ColumnDataEncoder() = default;
  ColumnDataEncoder(const ColumnDataEncoder&) = delete;
  ColumnDataEncoder(ColumnDataEncoder&&) = delete;
  ColumnDataEncoder& operator=(const ColumnDataEncoder&) = delete;
  ColumnDataEncoder& operator=(ColumnDataEncoder&&) = delete;
  virtual ~ColumnDataEncoder() = default;

  // Set the number of bytes a value is padded to. This is required for to ensure all columnar values have the same size.
  virtual void set_width(size_t padding) = 0;

  // Returns the number of bytes required to encode this value.
  virtual size_t required_bytes(RowID row_id) const = 0;

  // Encodes the value of the given row to the byte vector. Returns the iterator to the end of the encoded data.
  virtual PmrByteIter encode(RowID row_id, PmrByteIter start) const = 0;
};

auto to_unsigned_representation(auto signed_value, const uint32_t expected_num_bytes) {
  if constexpr (std::is_same_v<decltype(signed_value), int32_t>) {
    const auto zero_value = (std::numeric_limits<uint32_t>::max() / 2) + 1;
    const auto result = zero_value + std::bit_cast<uint32_t>(signed_value);
    if (expected_num_bytes < 4) {
      const auto mask = uint32_t{1} << (8 * expected_num_bytes - 1);
      return result ^ mask;
    }
    return result;
  } else if constexpr (std::is_same_v<decltype(signed_value), int64_t>) {
    const auto zero_value = (std::numeric_limits<uint64_t>::max() / 2) + 1;
    const auto result = zero_value + std::bit_cast<uint64_t>(signed_value);
    if (expected_num_bytes < 4) {
      const auto mask = uint64_t{1} << (8 * expected_num_bytes - 1);
      return result ^ mask;
    }
    return result;
  } else if constexpr (std::is_same_v<decltype(signed_value), float>) {
    auto unsigned_int = std::bit_cast<uint32_t>(signed_value);
    if (signed_value < 0) {
      unsigned_int ^= std::numeric_limits<uint32_t>::max();
    } else {
      unsigned_int |= std::numeric_limits<uint32_t>::max() / 2 + 1;
    }
    return unsigned_int;
  } else if constexpr (std::is_same_v<decltype(signed_value), double>) {
    auto unsigned_int = std::bit_cast<uint64_t>(signed_value);
    if (signed_value < 0) {
      unsigned_int ^= std::numeric_limits<uint64_t>::max();
    } else {
      unsigned_int |= std::numeric_limits<uint64_t>::max() / 2 + 1;
    }
    return unsigned_int;
  } else {
    Fail(std::format("Not implemented `{}`", typeid(decltype(signed_value)).name()));
    return uint32_t{0};
  }
}

template <typename DataType>
class TypedColumnDataEncoder : public ColumnDataEncoder {
 public:
  TypedColumnDataEncoder(const TypedColumnDataEncoder&) = delete;
  TypedColumnDataEncoder(TypedColumnDataEncoder&&) = delete;
  TypedColumnDataEncoder& operator=(const TypedColumnDataEncoder&) = delete;
  TypedColumnDataEncoder& operator=(TypedColumnDataEncoder&&) = delete;
  ~TypedColumnDataEncoder() override = default;

  TypedColumnDataEncoder(const std::shared_ptr<const Table>& table, ColumnID column_id, SortMode sort_mode)
      : ColumnDataEncoder(),
        _table(table),
        _column_id(column_id),
        _sort_mode(sort_mode),
        _accessor(table->chunk_count()),
        _is_nullable(_table->column_is_nullable(column_id)),
        _width(size_t{0}) {
    for (auto chunk_id = ChunkID{0}; chunk_id < table->chunk_count(); ++chunk_id) {
      const auto segment = _table->get_chunk(chunk_id)->get_segment(_column_id);
      _accessor[chunk_id] = create_segment_accessor<DataType>(segment);
    }
  }

  void set_width(size_t width) override {
    if constexpr (std::is_same_v<DataType, int32_t> || std::is_same_v<DataType, float>) {
      DebugAssert(width == (_is_nullable) ? 5 : 4, std::format("Cannot encode int32_t/float with padding %d", width));
    } else if constexpr (std::is_same_v<DataType, int64_t> || std::is_same_v<DataType, double>) {
      DebugAssert(width == (_is_nullable) ? 9 : 8, std::format("Cannot encode int64_t/double with padding %d", width));
    }
    _width = width;
  }

  size_t required_bytes(RowID row_id) const override {
    auto null_bytes = static_cast<size_t>((_is_nullable) ? 1 : 0);
    if constexpr (std::is_same_v<DataType, float>) {
      return size_t{4} + null_bytes;
    }
    if constexpr (std::is_same_v<DataType, double>) {
      return size_t{8} + null_bytes;
    }
    const auto value = _accessor[row_id.chunk_id]->access(row_id.chunk_offset);
    if constexpr (std::is_same_v<DataType, int32_t>) {
      if (value) {
        auto required_bytes = size_t{0};
        if (*value == 0) {
          required_bytes = 1;
        } else if (*value < 0) {
          const auto unsigned_value = to_unsigned_representation(*value, 4) | 0x80000000;
          required_bytes = 4 - ((std::countl_one(unsigned_value) - 1) / 8);
        } else {
          required_bytes = 4 - ((std::countl_zero(static_cast<uint32_t>(*value)) - 1) / 8);
        }
        return required_bytes + null_bytes;
      }
    }
    if constexpr (std::is_same_v<DataType, int64_t>) {
      if (value) {
        auto required_bytes = size_t{0};
        if (*value == 0) {
          required_bytes = 1;
        } else if (*value < 0) {
          const auto unsigned_value = to_unsigned_representation(*value, 8);
          required_bytes = 8 - ((std::countl_one(unsigned_value) - 1) / 8);
        } else {
          required_bytes = 8 - ((std::countl_zero(static_cast<uint32_t>(*value)) - 1) / 8);
        }
        return required_bytes + null_bytes;
      }
    }
    if constexpr (std::is_same_v<DataType, pmr_string>) {
      if (value) {
        return value->size() + null_bytes;
      }
    }
    return null_bytes;
  }

  PmrByteIter encode(RowID row_id, PmrByteIter start) const override {
    const auto value = _accessor[row_id.chunk_id]->access(row_id.chunk_offset);
    if (value) {
      if (_is_nullable) {
        if (_sort_mode == SortMode::AscendingNullsFirst || _sort_mode == SortMode::DescendingNullsFirst) {
          (*start++) = std::numeric_limits<std::byte>::max();
        } else {
          (*start++) = std::numeric_limits<std::byte>::min();
        }
      }
      return _encode_to_bytes(*value, start);
    }
    return _encode_null(start);
  }

 private:
  PmrByteIter _encode_null(PmrByteIter start) const {
    DebugAssert(_is_nullable, "Cannot encode null value for non-nullable columns");
    if (_sort_mode == SortMode::AscendingNullsFirst || _sort_mode == SortMode::DescendingNullsFirst) {
      (*start++) = std::numeric_limits<std::byte>::min();
    } else {
      (*start++) = std::numeric_limits<std::byte>::max();
    }
    for (auto counter = size_t{1}; counter < _width; counter++) {
      (*start++) = std::byte{0};
    }
    return start;
  }

  PmrByteIter _encode_to_bytes(const pmr_string& value, PmrByteIter start) const {
    const auto byte_count = (_is_nullable) ? _width - 1 : _width;
    if (_sort_mode == SortMode::AscendingNullsFirst || _sort_mode == SortMode::AscendingNullsLast) {
      for (const auto chr : value) {
        (*start++) = std::bit_cast<std::byte>(chr);
      }
    } else {
      for (const auto chr : value) {
        (*start++) = std::bit_cast<std::byte>(chr) ^ std::byte{0xff};
      }
    }
    const auto fill_byte = std::numeric_limits<std::byte>::min();
    for (auto index = static_cast<ChunkOffset>(value.size()); index < byte_count; ++index) {
      (*start++) = fill_byte;
    }
    return start;
  }

  // Encode signed value to a byte array for proper use with memcmp.
  PmrByteIter _encode_to_bytes(auto value, PmrByteIter start) const {
    DebugAssert(_width > 0, "Padding not set");
    auto byte_count = _width;
    if (_is_nullable) {
      byte_count -= 1;
    }

    // Convert to an unsigned representation of the signed value (integer or floating point).
    // For all signed a < b <=> unsgined representation a' < b' holds.
    auto unsigned_value = to_unsigned_representation(value, byte_count);
    using UnsignedInt = decltype(unsigned_value);
    if (_sort_mode == SortMode::DescendingNullsFirst || _sort_mode == SortMode::DescendingNullsLast) {
      // Invert order.
      unsigned_value ^= std::numeric_limits<UnsignedInt>::max();
    }

    for (auto offset = (static_cast<int32_t>(byte_count) - 1) * 8; offset >= 0; offset -= 8) {
      const auto unsigned_offset = static_cast<UnsignedInt>(offset);
      *start++ = static_cast<std::byte>((unsigned_value >> unsigned_offset) & 0xFF);
    }

    return start;
  }

  const std::shared_ptr<const Table>& _table;  // NO LINT
  ColumnID _column_id;
  SortMode _sort_mode;
  pmr_vector<std::unique_ptr<AbstractSegmentAccessor<DataType>>> _accessor;
  bool _is_nullable;
  size_t _width;
};

struct NormalizedKeyStorage {
  NormalizedKeyStorage(const NormalizedKeyStorage&) = delete;
  NormalizedKeyStorage& operator=(const NormalizedKeyStorage&) = delete;
  NormalizedKeyStorage(NormalizedKeyStorage&& other) noexcept = delete;

  NormalizedKeyStorage& operator=(NormalizedKeyStorage&& other) noexcept {
    delete ptr;
    ptr = other.ptr;
    other.ptr = nullptr;
    return *this;
  }

  NormalizedKeyStorage() : ptr(nullptr) {}

  explicit NormalizedKeyStorage(size_t len) : ptr(new std::byte[len]) {
    Assert(ptr, "Failed to initlaize raw array");
  }

  ~NormalizedKeyStorage() {
    delete[] ptr;
  }

  std::byte* ptr;  // NOLINT
};

template <size_t start, size_t end>
int static_memcmp(std::byte* left, std::byte* right, size_t len) {
  if (len == start) {
    return memcmp(left, right, start);
  }
  if constexpr (start < end) {
    return static_memcmp<start + 1, end>(left, right, len);
  } else {
    return memcmp(left, right, len);
  }
}

struct NormalizedKeyRow {
  std::byte* key_head;
  RowID row_id;

  bool less_than(const NormalizedKeyRow& other, size_t expected_size) const {
    if (expected_size == 0) {
      return false;
    }
    return static_memcmp<1, 32>(key_head, other.key_head, expected_size) < 0;
  }
};

template <typename T>
std::vector<std::shared_ptr<AbstractTask>> process_in_parallel(const std::vector<T>& elements, size_t max_parallelism,
                                                               auto handler) {
  auto tasks = std::vector<std::shared_ptr<AbstractTask>>();
  tasks.reserve(max_parallelism);
  const auto task_count = std::min(elements.size(), max_parallelism);
  const auto task_workload = elements.size() / task_count;
  auto from_index = size_t{0};
  for (auto task_index = size_t{1}; task_index <= task_count; ++task_index) {
    const auto to_index = (task_index != task_count) ? task_index * task_workload : elements.size();
    const auto task = std::make_shared<JobTask>([from_index, to_index, handler, &elements] {
      for (auto index = from_index; index < to_index; ++index) {
        handler(elements[index]);
      }
    });
    task->schedule();
    tasks.push_back(task);
    from_index = to_index;
  }
  return tasks;
}

std::vector<std::pair<size_t, ChunkID>> generate_column_chunk_id_pairs(size_t column_count, ChunkID chunk_count) {
  auto result = std::vector<std::pair<size_t, ChunkID>>();
  result.reserve(column_count * chunk_count);
  for (auto column_index = size_t{0}; column_index < column_count; ++column_index) {
    for (auto chunk_id = size_t{0}; chunk_id < chunk_count; ++chunk_id) {
      result.emplace_back(column_index, chunk_id);
    }
  }
  return result;
}

}  // namespace

namespace hyrise {
Sort::Sort(const std::shared_ptr<const AbstractOperator>& input_operator,
           const std::vector<SortColumnDefinition>& sort_definitions, const ChunkOffset output_chunk_size,
           const ForceMaterialization force_materialization)
    : AbstractReadOnlyOperator(OperatorType::Sort, input_operator, nullptr,
                               std::make_unique<OperatorPerformanceData<OperatorSteps>>()),
      _sort_definitions(sort_definitions),
      _output_chunk_size(output_chunk_size),
      _force_materialization(force_materialization) {
  DebugAssert(!_sort_definitions.empty(), "Expected at least one sort criterion");
}

const std::vector<SortColumnDefinition>& Sort::sort_definitions() const {
  return _sort_definitions;
}

const std::string& Sort::name() const {
  static const auto name = std::string{"Sort"};
  return name;
}

std::shared_ptr<AbstractOperator> Sort::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_left_input,
    const std::shared_ptr<AbstractOperator>& /*copied_right_input*/,
    std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& /*copied_ops*/) const {
  return std::make_shared<Sort>(copied_left_input, _sort_definitions, _output_chunk_size, _force_materialization);
}

void Sort::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

std::shared_ptr<const Table> Sort::_on_execute() {
  auto timer = Timer{};
  const auto& input_table = left_input_table();

  for (const auto& column_sort_definition : _sort_definitions) {
    Assert(column_sort_definition.column != INVALID_COLUMN_ID, "Sort: Invalid column in sort definition");
    Assert(column_sort_definition.column < input_table->column_count(),
           "Sort: Column ID is greater than table's column count");
    Assert(column_sort_definition.sort_mode == SortMode::AscendingNullsFirst ||
               column_sort_definition.sort_mode == SortMode::DescendingNullsFirst,
           "Sort does not support NULLS LAST.");
  }

  if (input_table->row_count() == 0) {
    if (_force_materialization == ForceMaterialization::Yes && input_table->type() == TableType::References) {
      return Table::create_dummy_table(input_table->column_definitions());
    }

    return input_table;
  }

  auto sorted_table = std::shared_ptr<Table>{};

  auto column_encoders = pmr_vector<std::shared_ptr<ColumnDataEncoder>>();
  column_encoders.reserve(_sort_definitions.size());
  for (const auto& column_sort_definition : _sort_definitions) {
    const auto column_data_type = input_table->column_data_type(column_sort_definition.column);
    resolve_data_type(column_data_type, [&](auto type) {
      using ColumnDataType = typename decltype(type)::type;
      auto encoder = std::make_shared<TypedColumnDataEncoder<ColumnDataType>>(
          input_table, column_sort_definition.column, column_sort_definition.sort_mode);
      column_encoders.push_back(encoder);
    });
  }
  const auto init_time = timer.lap();
  std::cerr << "sort::init_time " << init_time << "\n";

  // Scan all chunks for the maximum number of bytes necessary to represent all column values. The scanning is
  // done in parallel on multiple threads.

  const auto column_count = column_encoders.size();
  const auto chunk_count = input_table->chunk_count();

  const auto hardware_parallelism = std::thread::hardware_concurrency();
  auto column_chunk_max_bytes = std::vector(column_count, std::vector(chunk_count, size_t{0}));
  const auto column_chunk_pairs = generate_column_chunk_id_pairs(column_count, chunk_count);
  const auto scan_tasks = process_in_parallel(column_chunk_pairs, hardware_parallelism, [&](const auto element) {
    const auto [column_index, chunk_id] = element;
    const auto chunk_size = input_table->get_chunk(chunk_id)->size();
    auto max_bytes = size_t{0};
    for (auto chunk_offset = ChunkOffset{0}; chunk_offset < chunk_size; ++chunk_offset) {
      const auto row_id = RowID{chunk_id, chunk_offset};
      max_bytes = std::max(max_bytes, column_encoders[column_index]->required_bytes(row_id));
    }
    column_chunk_max_bytes[column_index][chunk_id] = max_bytes;
  });
  Hyrise::get().scheduler()->wait_for_tasks(scan_tasks);

  auto row_size = size_t{0};
  for (auto column_index = size_t{0}; column_index < column_encoders.size(); ++column_index) {
    const auto column_max_bytes = std::ranges::max(column_chunk_max_bytes[column_index]);
    row_size += column_max_bytes;
    column_encoders[column_index]->set_width(column_max_bytes);
  }
  const auto padded_row_size = ((row_size + 3) / 4) * 4;
  std::cerr << "row_size " << row_size << "\n";
  std::cerr << "padded_row_size " << padded_row_size << "\n";

  const auto scan_time = timer.lap();
  std::cerr << "sort::scan_time " << scan_time << "\n";

  // Convert the columnar layout into a row layout for better sorting. This is done by encoding all sorted columns
  // into an array of bytes. These rows can be compared using memcmp.

  auto materialized_rows = pmr_vector<NormalizedKeyRow>();
  materialized_rows.resize(input_table->row_count());

  auto total_offset = size_t{0};
  auto chunk_ids = std::vector<std::pair<ChunkID, size_t>>(chunk_count);
  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    const auto chunk_size = input_table->get_chunk(chunk_id)->size();
    chunk_ids[chunk_id] = {chunk_id, total_offset};
    total_offset += chunk_size;
  }

  auto chunk_allocations = std::vector<NormalizedKeyStorage>(chunk_count);

  const auto materialization_tasks = process_in_parallel(chunk_ids, hardware_parallelism, [&](auto element) {
    const auto [chunk_id, offset] = element;
    const auto chunk_size = input_table->get_chunk(chunk_id)->size();

    chunk_allocations[chunk_id] = NormalizedKeyStorage(chunk_size * padded_row_size);

    auto encoded_rows = pmr_vector<NormalizedKeyRow>(chunk_size);
    auto encoding_iter = pmr_vector<PmrByteIter>(chunk_size);
    for (auto chunk_offset = ChunkOffset{0}; chunk_offset < chunk_size; ++chunk_offset) {
      const auto row_id = RowID{chunk_id, chunk_offset};
      encoded_rows[chunk_offset] = NormalizedKeyRow{
          .key_head = chunk_allocations[chunk_id].ptr + (chunk_offset * padded_row_size),
          .row_id = row_id,
      };
      encoding_iter[chunk_offset] = encoded_rows[chunk_offset].key_head;
    }

    for (const auto& encoder : column_encoders) {
      for (auto chunk_offset = ChunkOffset{0}; chunk_offset < chunk_size; ++chunk_offset) {
        const auto row_id = RowID{chunk_id, chunk_offset};
        encoding_iter[chunk_offset] = encoder->encode(row_id, encoding_iter[chunk_offset]);
      }
    }

    for (auto chunk_offset = ChunkOffset{0}; chunk_offset < chunk_size; ++chunk_offset) {
      for (auto encoding_offset = row_size; encoding_offset < padded_row_size; ++encoding_offset) {
        *(encoding_iter[chunk_offset]++) = std::byte{0};
      }
      DebugAssert(encoding_iter[chunk_offset] == encoded_rows[chunk_offset].key_head + padded_row_size,
                  "Raw data not fully initialized");
      materialized_rows[offset + chunk_offset] = std::move(encoded_rows[chunk_offset]);
    }
  });
  Hyrise::get().scheduler()->wait_for_tasks(materialization_tasks);

  const auto materialization_time = timer.lap();
  std::cerr << "sort::materialization_time " << materialization_time << "\n";

  // TODO(student): Use pdqsort
  boost::sort::pdqsort(materialized_rows.begin(), materialized_rows.end(), [&](const auto& lhs, const auto& rhs) {
    return lhs.less_than(rhs, padded_row_size);
  });

  const auto sort_time = timer.lap();
  std::cerr << "sort::sort_time " << sort_time << "\n";

  // Extract the positions from the sorted rows.
  auto position_list = RowIDPosList();
  position_list.reserve(materialized_rows.size());
  for (const auto& row : materialized_rows) {
    position_list.push_back(row.row_id);
  }
  const auto write_back_time = timer.lap();
  std::cerr << "sort::write_back_time " << write_back_time << "\n";

  // TODO(student): Update performance metrics.
  auto& step_performance_data = dynamic_cast<OperatorPerformanceData<OperatorSteps>&>(*performance_data);
  step_performance_data.set_step_runtime(OperatorSteps::MaterializeSortColumns, materialization_time);
  step_performance_data.set_step_runtime(OperatorSteps::TemporaryResultWriting, write_back_time);
  step_performance_data.set_step_runtime(OperatorSteps::Sort, sort_time);

  // We have to materialize the output (i.e., write ValueSegments) if
  //  (a) it is requested by the user,
  //  (b) a column in the table references multiple tables (see write_reference_output_table for details), or
  //  (c) a column in the table references multiple columns in the same table (which is an unlikely edge case).
  // Cases (b) and (c) can only occur if there is more than one ReferenceSegment in an input chunk.
  auto must_materialize = _force_materialization == ForceMaterialization::Yes;
  const auto input_chunk_count = input_table->chunk_count();
  if (!must_materialize && input_table->type() == TableType::References && input_chunk_count > 1) {
    const auto input_column_count = input_table->column_count();

    for (auto input_column_id = ColumnID{0}; input_column_id < input_column_count; ++input_column_id) {
      const auto& first_segment = input_table->get_chunk(ChunkID{0})->get_segment(input_column_id);
      const auto& first_reference_segment = static_cast<ReferenceSegment&>(*first_segment);

      const auto& common_referenced_table = first_reference_segment.referenced_table();
      const auto& common_referenced_column_id = first_reference_segment.referenced_column_id();

      for (auto input_chunk_id = ChunkID{1}; input_chunk_id < input_chunk_count; ++input_chunk_id) {
        const auto& segment = input_table->get_chunk(input_chunk_id)->get_segment(input_column_id);
        const auto& referenced_table = static_cast<ReferenceSegment&>(*segment).referenced_table();
        const auto& referenced_column_id = static_cast<ReferenceSegment&>(*segment).referenced_column_id();

        if (common_referenced_table != referenced_table || common_referenced_column_id != referenced_column_id) {
          must_materialize = true;
          break;
        }
      }
      if (must_materialize) {
        break;
      }
    }
  }

  if (must_materialize) {
    sorted_table = write_materialized_output_table(input_table, std::move(position_list), _output_chunk_size);
  } else {
    sorted_table = write_reference_output_table(input_table, std::move(position_list), _output_chunk_size);
  }

  const auto& final_sort_definition = _sort_definitions[0];
  // Set the sorted_by attribute of the output's chunks according to the most significant sort operation, which is the
  // column the table was sorted by last.
  const auto output_chunk_count = sorted_table->chunk_count();
  for (auto output_chunk_id = ChunkID{0}; output_chunk_id < output_chunk_count; ++output_chunk_id) {
    const auto& output_chunk = sorted_table->get_chunk(output_chunk_id);
    output_chunk->set_immutable();
    output_chunk->set_individually_sorted_by(final_sort_definition);
  }

  step_performance_data.set_step_runtime(OperatorSteps::WriteOutput, timer.lap());
  return sorted_table;
}

}  // namespace hyrise
