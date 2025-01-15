#include "reduce.hpp"

#include <atomic>
#include <memory>
#include <vector>
#include "types.hpp"

namespace {
class MultiplyShiftHasher {
 private:
  uint64_t _multiplier = 0x9E3779B97F4A7C15;  // 64-bit multiplier
  uint32_t _output_size = 22;       // Output size

 public:
  uint32_t hash(const int32_t input) const {
    // Convert to unsigned 64-bit to handle negative numbers and allow full range
    const auto input_unsigned = static_cast<uint64_t>(static_cast<uint32_t>(input));
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

  auto hasher = MultiplyShiftHasher{};
  hasher.hash(42);
  
  // const auto input_table = left_input_table();
  // const auto chunk_count = input_table->chunk_count();

  // for (auto chunk_index = ChunkID{0}; chunk_index < chunk_count; ++chunk_index) {
  //   const auto segment = input_table->get_chunk(chunk_index)->get_segment(column_id);

  //   segment_with_iterators(segment, [&](auto iter, [[maybe_unused]] const auto end) {
  //     for (; iter != end; ++iter) {
  //     const auto left = *iter;
  //       if ((!CheckForNull || !left.is_null()) && func(left)) {
  //         matches_out.emplace_back(chunk_id, left.chunk_offset());
  //       }
      
  //     }
  //   });
  // }
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
