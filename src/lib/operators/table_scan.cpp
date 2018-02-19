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
#include "storage/base_column.hpp"
#include "storage/chunk.hpp"
#include "storage/proxy_chunk.hpp"
#include "storage/reference_column.hpp"
#include "storage/table.hpp"
#include "table_scan/column_comparison_table_scan_impl.hpp"
#include "table_scan/is_null_table_scan_impl.hpp"
#include "table_scan/like_table_scan_impl.hpp"
#include "table_scan/single_column_table_scan_impl.hpp"
#include "type_cast.hpp"
#include "utils/assert.hpp"
#include "utils/performance_warning.hpp"

namespace opossum {

TableScan::TableScan(const std::shared_ptr<const AbstractOperator> in, ColumnID left_column_id,
                     const PredicateCondition predicate_condition, const AllParameterVariant right_parameter)
    : AbstractReadOnlyOperator{in},
      _left_column_id{left_column_id},
      _predicate_condition{predicate_condition},
      _right_parameter{right_parameter} {}

TableScan::~TableScan() = default;

void TableScan::set_excluded_chunk_ids(const std::vector<ChunkID>& chunk_ids) { _excluded_chunk_ids = chunk_ids; }

ColumnID TableScan::left_column_id() const { return _left_column_id; }

PredicateCondition TableScan::predicate_condition() const { return _predicate_condition; }

const AllParameterVariant& TableScan::right_parameter() const { return _right_parameter; }

const std::string TableScan::name() const { return "TableScan"; }

const std::string TableScan::description(DescriptionMode description_mode) const {
  std::string column_name = std::string("Col #") + std::to_string(_left_column_id);

  if (_input_table_left()) column_name = _input_table_left()->column_name(_left_column_id);

  std::string predicate_string = to_string(_right_parameter);

  const auto separator = description_mode == DescriptionMode::MultiLine ? "\n" : " ";
  return name() + separator + "(" + column_name + " " + predicate_condition_to_string.left.at(_predicate_condition) +
         " " + predicate_string + ")";
}

std::shared_ptr<AbstractOperator> TableScan::recreate(const std::vector<AllParameterVariant>& args) const {
  // Replace value in the new operator, if it’s a parameter and an argument is available.
  if (is_placeholder(_right_parameter)) {
    const auto index = boost::get<ValuePlaceholder>(_right_parameter).index();
    if (index < args.size()) {
      return std::make_shared<TableScan>(_input_left->recreate(args), _left_column_id, _predicate_condition,
                                         args[index]);
    }
  }
  return std::make_shared<TableScan>(_input_left->recreate(args), _left_column_id, _predicate_condition,
                                     _right_parameter);
}

std::shared_ptr<const Table> TableScan::_on_execute() {
  _in_table = _input_table_left();

  _init_scan();

  _output_table = Table::create_with_layout_from(_in_table);

  std::mutex output_mutex;

  const auto excluded_chunk_set = std::unordered_set<ChunkID>{_excluded_chunk_ids.cbegin(), _excluded_chunk_ids.cend()};

  auto jobs = std::vector<std::shared_ptr<AbstractTask>>{};
  jobs.reserve(_in_table->chunk_count() - excluded_chunk_set.size());

  for (ChunkID chunk_id{0u}; chunk_id < _in_table->chunk_count(); ++chunk_id) {
    if (excluded_chunk_set.count(chunk_id)) continue;

    auto job_task = std::make_shared<JobTask>([=, &output_mutex]() {
      const auto chunk_guard = _in_table->get_chunk_with_access_counting(chunk_id);
      // The actual scan happens in the sub classes of BaseTableScanImpl
      const auto matches_out = std::make_shared<PosList>(_impl->scan_chunk(chunk_id));

      // The output chunk is allocated on the same NUMA node as the input chunk. Also, the AccessCounter is
      // reused to track accesses of the output chunk. Accesses of derived chunks are counted towards the
      // original chunk.
      auto chunk_out = std::make_shared<Chunk>(chunk_guard->get_allocator(), chunk_guard->access_counter());

      /**
       * matches_out contains a list of row IDs into this chunk. If this is not a reference table, we can
       * directly use the matches to construct the reference columns of the output. If it is a reference column,
       * we need to resolve the row IDs so that they reference the physical data columns (value, dictionary) instead,
       * since we don’t allow multi-level referencing. To save time and space, we want to share position lists
       * between columns as much as possible. Position lists can be shared between two columns iff
       * (a) they point to the same table and
       * (b) the reference columns of the input table point to the same positions in the same order
       *     (i.e. they share their position list).
       */
      if (_in_table->get_type() == TableType::References) {
        const auto chunk_in = _in_table->get_chunk(chunk_id);

        auto filtered_pos_lists =
            std::map<std::shared_ptr<const PosList>, std::pair<std::shared_ptr<PosList>, PosListType>>{};

        for (ColumnID column_id{0u}; column_id < _in_table->column_count(); ++column_id) {
          auto column_in = chunk_in->get_column(column_id);

          auto ref_column_in = std::dynamic_pointer_cast<const ReferenceColumn>(column_in);
          DebugAssert(ref_column_in != nullptr, "All columns should be of type ReferenceColumn.");

          const auto pos_list_in = ref_column_in->pos_list();

          const auto table_out = ref_column_in->referenced_table();
          const auto column_id_out = ref_column_in->referenced_column_id();

          auto & [ filtered_pos_list, pos_list_type ] = filtered_pos_lists[pos_list_in];

          if (!filtered_pos_list) {
            filtered_pos_list = std::make_shared<PosList>();
            filtered_pos_list->reserve(matches_out->size());

            auto references_single_chunk = true;

            if (!matches_out->empty()) {
              const auto first_position_in_matches = matches_out->front().chunk_offset;
              const auto first_chunk_id = (*pos_list_in)[first_position_in_matches].chunk_id;

              for (const auto& match : *matches_out) {
                const auto row_id = (*pos_list_in)[match.chunk_offset];
                references_single_chunk = (row_id.chunk_id == first_chunk_id);
                filtered_pos_list->push_back(row_id);
              }
            }

            pos_list_type = references_single_chunk ? PosListType::SingleChunk : PosListType::MultiChunk;
          }

          auto ref_column_out =
              std::make_shared<ReferenceColumn>(table_out, column_id_out, filtered_pos_list, pos_list_type);
          chunk_out->add_column(ref_column_out);
        }
      } else {
        for (ColumnID column_id{0u}; column_id < _in_table->column_count(); ++column_id) {
          auto ref_column_out =
              std::make_shared<ReferenceColumn>(_in_table, column_id, matches_out, PosListType::SingleChunk);
          chunk_out->add_column(ref_column_out);
        }
      }

      std::lock_guard<std::mutex> lock(output_mutex);
      if (chunk_out->size() > 0 || _output_table->get_chunk(ChunkID{0})->size() == 0) {
        _output_table->emplace_chunk(std::move(chunk_out));
      }
    });

    jobs.push_back(job_task);
    job_task->schedule();
  }

  CurrentScheduler::wait_for_tasks(jobs);

  return _output_table;
}

void TableScan::_on_cleanup() { _impl.reset(); }

void TableScan::_init_scan() {
  DebugAssert(_in_table->chunk_count() > 0u, "Input table must contain at least 1 chunk.");

  if (_predicate_condition == PredicateCondition::Like || _predicate_condition == PredicateCondition::NotLike) {
    const auto left_column_type = _in_table->column_type(_left_column_id);
    Assert((left_column_type == DataType::String), "LIKE operator only applicable on string columns.");

    DebugAssert(is_variant(_right_parameter), "Right parameter must be variant.");

    const auto right_value = boost::get<AllTypeVariant>(_right_parameter);

    DebugAssert(!variant_is_null(right_value), "Right value must not be NULL.");

    const auto right_wildcard = type_cast<std::string>(right_value);

    _impl = std::make_unique<LikeTableScanImpl>(_in_table, _left_column_id, _predicate_condition, right_wildcard);

    return;
  }

  if (_predicate_condition == PredicateCondition::IsNull || _predicate_condition == PredicateCondition::IsNotNull) {
    _impl = std::make_unique<IsNullTableScanImpl>(_in_table, _left_column_id, _predicate_condition);
    return;
  }

  if (is_variant(_right_parameter)) {
    const auto right_value = boost::get<AllTypeVariant>(_right_parameter);

    _impl = std::make_unique<SingleColumnTableScanImpl>(_in_table, _left_column_id, _predicate_condition, right_value);
  } else /* is_column_name(_right_parameter) */ {
    const auto right_column_id = boost::get<ColumnID>(_right_parameter);

    _impl = std::make_unique<ColumnComparisonTableScanImpl>(_in_table, _left_column_id, _predicate_condition,
                                                            right_column_id);
  }
}

}  // namespace opossum
