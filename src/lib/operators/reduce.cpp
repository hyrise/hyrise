#include "reduce.hpp"

#include <atomic>
#include <memory>
#include <vector>

#include "storage/segment_iterate.hpp"
#include "types.hpp"

namespace {
template <typename DataType>
class MultiplyShiftHasher {
 private:
  uint64_t _multiplier = 0x9E3779B97F4A7C15;  // 64-bit multiplier
  uint32_t _output_size = 22;                 // Output size

 public:
  uint32_t hash(const DataType input) const {
    // if
    // Convert to unsigned 64-bit to handle negative numbers and allow full range
    uint64_t input_unsigned;
    if constexpr (std::is_same_v<DataType, int32_t>) {
      input_unsigned = static_cast<uint64_t>(input);
    } else {
      input_unsigned = 0;
    }

    // Multiply (result is 64-bit)
    const auto product = input_unsigned * _multiplier;
    // For full 64-bit output, no shift needed if m=64
    return product >> (64 - _output_size);
  }
};
}  // namespace

namespace hyrise {

Reduce::Reduce(const std::shared_ptr<const AbstractOperator>& input_relation,
               const std::shared_ptr<const AbstractOperator>& input_filter)
    : AbstractReadOnlyOperator{OperatorType::Reduce, input_relation, input_filter} {}

void Reduce::_create_filter(const ColumnID column_id, const uint32_t filter_size) {
  _filter = std::make_shared<std::vector<std::atomic_uint64_t>>(filter_size);

  const auto input_table = left_input_table();
  const auto chunk_count = input_table->chunk_count();

  Assert(input_table->column_data_type(column_id) == DataType::Int, "Only int supported for now.");

  resolve_data_type(input_table->column_data_type(column_id), [&](const auto column_data_type) {
    using ColumnDataType = typename decltype(column_data_type)::type;
    auto hasher = MultiplyShiftHasher<ColumnDataType>{};
    (void)hasher;

    for (auto chunk_index = ChunkID{0}; chunk_index < chunk_count; ++chunk_index) {
      const auto& segment = input_table->get_chunk(chunk_index)->get_segment(column_id);
      (void)segment;

      segment_iterate<ColumnDataType>(*segment, [&](const auto& position) {
        (void)hasher;
        if (!position.is_null()) {
          hasher.hash(position.value());
        }
      });
    }
  });
}

const std::string& Reduce::name() const {
  static const auto name = std::string{"Reduce"};
  return name;
}

const std::shared_ptr<std::vector<std::atomic_uint64_t>>& Reduce::export_filter() const {
  return _filter;
}

std::shared_ptr<const Table> Reduce::_on_execute() {
  return left_input_table();
}

void Reduce::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

std::shared_ptr<AbstractOperator> Reduce::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_left_input,
    const std::shared_ptr<AbstractOperator>& /*copied_right_input*/,
    std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& copied_ops) const {
  return nullptr;
}

}  // namespace hyrise
