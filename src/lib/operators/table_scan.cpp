#include "table_scan.hpp"

#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "all_parameter_variant.hpp"
#include "constant_mappings.hpp"
#include "scheduler/abstract_task.hpp"
#include "scheduler/current_scheduler.hpp"
#include "scheduler/job_task.hpp"
#include "storage/base_segment.hpp"
#include "storage/chunk.hpp"
#include "storage/proxy_chunk.hpp"
#include "storage/reference_segment.hpp"
#include "storage/table.hpp"
#include "table_scan/between_table_scan_impl.hpp"
#include "table_scan/column_comparison_table_scan_impl.hpp"
#include "table_scan/is_null_table_scan_impl.hpp"
#include "table_scan/like_table_scan_impl.hpp"
#include "table_scan/single_column_table_scan_impl.hpp"
#include "type_cast.hpp"
#include "utils/assert.hpp"
#include "utils/performance_warning.hpp"

namespace opossum {

TableScan::TableScan(const std::shared_ptr<const AbstractOperator>& in, const OperatorScanPredicate& predicate)
    : AbstractReadOnlyOperator{OperatorType::TableScan, in}, _predicate{predicate} {}

TableScan::~TableScan() = default;

void TableScan::set_excluded_chunk_ids(const std::vector<ChunkID>& chunk_ids) { _excluded_chunk_ids = chunk_ids; }

const std::string TableScan::name() const { return "TableScan"; }

const std::string TableScan::description(DescriptionMode description_mode) const {
  const auto separator = description_mode == DescriptionMode::MultiLine ? "\n" : " ";
  return name() + separator + _predicate.to_string(input_table_left());
}

const OperatorScanPredicate& TableScan::predicate() const { return _predicate; }

void TableScan::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {
  if (is_parameter_id(_predicate.value)) {
    const auto value_iter = parameters.find(boost::get<ParameterID>(_predicate.value));
    if (value_iter != parameters.end()) _predicate.value = value_iter->second;
  }

  if (_predicate.value2 && is_parameter_id(*_predicate.value2)) {
    const auto value_iter = parameters.find(boost::get<ParameterID>(*_predicate.value2));
    if (value_iter != parameters.end()) _predicate.value2 = value_iter->second;
  }
}

std::shared_ptr<AbstractOperator> TableScan::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_input_left,
    const std::shared_ptr<AbstractOperator>& copied_input_right) const {
  return std::make_shared<TableScan>(copied_input_left, _predicate);
}

std::shared_ptr<const Table> TableScan::_on_execute() {
  _in_table = input_table_left();

  _init_scan();

  _output_table = std::make_shared<Table>(_in_table->column_definitions(), TableType::References);

  std::mutex output_mutex;

  const auto excluded_chunk_set = std::unordered_set<ChunkID>{_excluded_chunk_ids.cbegin(), _excluded_chunk_ids.cend()};

  auto jobs = std::vector<std::shared_ptr<AbstractTask>>{};
  jobs.reserve(_in_table->chunk_count() - excluded_chunk_set.size());

  for (ChunkID chunk_id{0u}; chunk_id < _in_table->chunk_count(); ++chunk_id) {
    if (excluded_chunk_set.count(chunk_id)) continue;

    auto job_task = std::make_shared<JobTask>([=, &output_mutex]() {
      const auto chunk_guard = _in_table->get_chunk_with_access_counting(chunk_id);
      // The actual scan happens in the sub classes of BaseTableScanImpl
      const auto matches_out = _impl->scan_chunk(chunk_id);
      if (matches_out->empty()) return;

      // The ChunkAccessCounter is reused to track accesses of the output chunk. Accesses of derived chunks are counted
      // towards the original chunk.
      Segments out_segments;

      /**
       * matches_out contains a list of row IDs into this chunk. If this is not a reference table, we can
       * directly use the matches to construct the reference segments of the output. If it is a reference segment,
       * we need to resolve the row IDs so that they reference the physical data segments (value, dictionary) instead,
       * since we don’t allow multi-level referencing. To save time and space, we want to share position lists
       * between segments as much as possible. Position lists can be shared between two segments iff
       * (a) they point to the same table and
       * (b) the reference segments of the input table point to the same positions in the same order
       *     (i.e. they share their position list).
       */
      if (_in_table->type() == TableType::References) {
        const auto chunk_in = _in_table->get_chunk(chunk_id);

        auto filtered_pos_lists = std::map<std::shared_ptr<const PosList>, std::shared_ptr<PosList>>{};

        for (ColumnID column_id{0u}; column_id < _in_table->column_count(); ++column_id) {
          auto segment_in = chunk_in->get_segment(column_id);

          auto ref_segment_in = std::dynamic_pointer_cast<const ReferenceSegment>(segment_in);
          DebugAssert(ref_segment_in != nullptr, "All segments should be of type ReferenceSegment.");

          const auto pos_list_in = ref_segment_in->pos_list();

          const auto table_out = ref_segment_in->referenced_table();
          const auto column_id_out = ref_segment_in->referenced_column_id();

          auto& filtered_pos_list = filtered_pos_lists[pos_list_in];

          if (!filtered_pos_list) {
            filtered_pos_list = std::make_shared<PosList>();
            filtered_pos_list->reserve(matches_out->size());

            for (const auto& match : *matches_out) {
              const auto row_id = (*pos_list_in)[match.chunk_offset];
              filtered_pos_list->push_back(row_id);
            }
          }

          auto ref_segment_out = std::make_shared<ReferenceSegment>(table_out, column_id_out, filtered_pos_list);
          out_segments.push_back(ref_segment_out);
        }
      } else {
        for (ColumnID column_id{0u}; column_id < _in_table->column_count(); ++column_id) {
          auto ref_segment_out = std::make_shared<ReferenceSegment>(_in_table, column_id, matches_out);
          out_segments.push_back(ref_segment_out);
        }
      }

      std::lock_guard<std::mutex> lock(output_mutex);
      _output_table->append_chunk(out_segments, chunk_guard->get_allocator(), chunk_guard->access_counter());
    });

    jobs.push_back(job_task);
    job_task->schedule();
  }

  CurrentScheduler::wait_for_tasks(jobs);

  return _output_table;
}

void TableScan::_on_cleanup() { _impl.reset(); }

void TableScan::_init_scan() {
  const auto column_id = _predicate.column_id;
  const auto condition = _predicate.predicate_condition;
  const auto parameter = _predicate.value;

  if (condition == PredicateCondition::Like || condition == PredicateCondition::NotLike) {
    const auto left_column_type = _in_table->column_data_type(column_id);
    Assert((left_column_type == DataType::String), "LIKE operator only applicable on string columns.");

    Assert(is_variant(parameter), "Right parameter must be variant.");

    const auto right_value = boost::get<AllTypeVariant>(parameter);

    Assert(!variant_is_null(right_value), "Right value must not be NULL.");

    const auto right_wildcard = type_cast<std::string>(right_value);

    _impl = std::make_unique<LikeTableScanImpl>(_in_table, column_id, condition, right_wildcard);

    return;
  } else if (condition == PredicateCondition::IsNull || condition == PredicateCondition::IsNotNull) {
    _impl = std::make_unique<IsNullTableScanImpl>(_in_table, column_id, condition);
    return;
  } else if (condition == PredicateCondition::Between) {
    const auto left_value = boost::get<AllTypeVariant>(parameter);

    Assert(_predicate.value2, "Expected right value for BETWEEN");
    const auto right_value = boost::get<AllTypeVariant>(*_predicate.value2);

    Assert(left_value.which() == right_value.which(),
           "Expected left and right value to be of the same type (see operator_scan_predicate.cpp)");
    Assert(!variant_is_null(left_value) && !variant_is_null(right_value), "Expected BETWEEN values to be non-null");

    _impl = std::make_unique<BetweenTableScanImpl>(_in_table, column_id, left_value, right_value);
  } else if (is_variant(parameter)) {
    const auto right_value = boost::get<AllTypeVariant>(parameter);

    _impl = std::make_unique<SingleColumnTableScanImpl>(_in_table, column_id, condition, right_value);
  } else if (is_column_id(parameter)) {
    const auto right_column_id = boost::get<ColumnID>(parameter);

    _impl = std::make_unique<ColumnComparisonTableScanImpl>(_in_table, column_id, condition, right_column_id);
  } else {
    Fail("Never expected to end up here...");
  }
}

}  // namespace opossum
