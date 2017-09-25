#include "table_scan.hpp"

#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "table_scan/column_comparison_table_scan_impl.hpp"
#include "table_scan/is_null_table_scan_impl.hpp"
#include "table_scan/like_table_scan_impl.hpp"
#include "table_scan/single_column_table_scan_impl.hpp"

#include "scheduler/abstract_task.hpp"
#include "scheduler/current_scheduler.hpp"
#include "scheduler/job_task.hpp"

#include "storage/base_column.hpp"
#include "storage/chunk.hpp"
#include "storage/reference_column.hpp"
#include "storage/table.hpp"

#include "all_parameter_variant.hpp"
#include "type_cast.hpp"
#include "utils/assert.hpp"
#include "utils/performance_warning.hpp"

namespace opossum {

TableScan::TableScan(const std::shared_ptr<const AbstractOperator> in, ColumnID left_column_id,
                     const ScanType scan_type, const AllParameterVariant right_parameter,
                     const optional<AllTypeVariant> right_value2)
    : AbstractReadOnlyOperator{in},
      _left_column_id{left_column_id},
      _scan_type{scan_type},
      _right_parameter{right_parameter},
      _right_value2{right_value2} {}

TableScan::~TableScan() = default;

ColumnID TableScan::left_column_id() const { return _left_column_id; }

ScanType TableScan::scan_type() const { return _scan_type; }

const AllParameterVariant &TableScan::right_parameter() const { return _right_parameter; }

const optional<AllTypeVariant> &TableScan::right_value2() const { return _right_value2; }

const std::string TableScan::name() const { return "TableScan"; }

uint8_t TableScan::num_in_tables() const { return 1; }

uint8_t TableScan::num_out_tables() const { return 1; }

std::shared_ptr<AbstractOperator> TableScan::recreate(const std::vector<AllParameterVariant> &args) const {
  // Replace value in the new operator, if it’s a parameter and an argument is available.
  if (is_placeholder(_right_parameter)) {
    const auto index = boost::get<ValuePlaceholder>(_right_parameter).index();
    if (index < args.size()) {
      return std::make_shared<TableScan>(_input_left->recreate(args), _left_column_id, _scan_type, args[index],
                                         _right_value2);
    }
  }
  return std::make_shared<TableScan>(_input_left->recreate(args), _left_column_id, _scan_type, _right_parameter,
                                     _right_value2);
}

std::shared_ptr<const Table> TableScan::_on_execute() {
  if (auto between_output_table = __on_execute_between()) {
    return between_output_table;
  }

  _in_table = _input_table_left();

  _init_scan();
  _init_output_table();

  std::mutex output_mutex;

  auto jobs = std::vector<std::shared_ptr<AbstractTask>>{};
  jobs.reserve(_in_table->chunk_count());

  for (ChunkID chunk_id{0u}; chunk_id < _in_table->chunk_count(); ++chunk_id) {
    auto job_task = std::make_shared<JobTask>([=, &output_mutex]() {
      // The actual scan happens in the sub classes of ColumnScanBase
      const auto matches_out = std::make_shared<PosList>(_impl->scan_chunk(chunk_id));

      Chunk chunk_out;

      /**
       * matches_out contains a list of row IDs into this chunk. If this is not a reference table, we can
       * directly use the matches to construct the reference columns of the output. If it is a reference column,
       * we need to resolve the row IDs so that they reference the physical data columns (value, dictionary) instead,
       * since we don’t allow multi-level referencing. To save time and space, we want to share positions lists
       * between columns as much as possible. Position lists can be shared between two columns iff
       * (a) they point to the same table and
       * (b) the reference columns of the input table point to the same positions in the same order
       *     (i.e. they share their position list).
       */
      if (_is_reference_table) {
        const auto &chunk_in = _in_table->get_chunk(chunk_id);

        auto filtered_pos_lists = std::map<std::shared_ptr<const PosList>, std::shared_ptr<PosList>>{};

        for (ColumnID column_id{0u}; column_id < _in_table->col_count(); ++column_id) {
          auto column_in = chunk_in.get_column(column_id);

          auto ref_column_in = std::dynamic_pointer_cast<const ReferenceColumn>(column_in);
          DebugAssert(ref_column_in != nullptr, "All columns should be of type ReferenceColumn.");

          const auto pos_list_in = ref_column_in->pos_list();

          const auto table_out = ref_column_in->referenced_table();
          const auto column_id_out = ref_column_in->referenced_column_id();

          auto &filtered_pos_list = filtered_pos_lists[pos_list_in];

          if (!filtered_pos_list) {
            filtered_pos_list = std::make_shared<PosList>();
            filtered_pos_list->reserve(matches_out->size());

            for (const auto &match : *matches_out) {
              const auto row_id = (*pos_list_in)[match.chunk_offset];
              filtered_pos_list->push_back(row_id);
            }
          }

          auto ref_column_out = std::make_shared<ReferenceColumn>(table_out, column_id_out, filtered_pos_list);
          chunk_out.add_column(ref_column_out);
        }
      } else {
        for (ColumnID column_id{0u}; column_id < _in_table->col_count(); ++column_id) {
          auto ref_column_out = std::make_shared<ReferenceColumn>(_in_table, column_id, matches_out);
          chunk_out.add_column(ref_column_out);
        }
      }

      std::lock_guard<std::mutex> lock(output_mutex);
      _output_table->add_chunk(std::move(chunk_out));
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
  const auto &first_chunk = _in_table->get_chunk(ChunkID{0u});

  _is_reference_table = [&]() {
    // We assume if one column is a reference column, all are.
    const auto column = first_chunk.get_column(_left_column_id);
    const auto ref_column = std::dynamic_pointer_cast<ReferenceColumn>(column);
    return ref_column != nullptr;
  }();

  if (_scan_type == ScanType::OpLike) {
    const auto left_column_type = _in_table->column_type(_left_column_id);
    Assert((left_column_type == "string"), "LIKE operator only applicable on string columns.");

    DebugAssert(is_variant(_right_parameter), "Right parameter must be variant.");

    const auto right_value = boost::get<AllTypeVariant>(_right_parameter);

    DebugAssert(!is_null(right_value), "Right value must not be NULL.");

    const auto right_wildcard = type_cast<std::string>(right_value);

    _impl = std::make_unique<LikeTableScanImpl>(_in_table, _left_column_id, right_wildcard);

    return;
  }

  if (is_variant(_right_parameter)) {
    const auto right_value = boost::get<AllTypeVariant>(_right_parameter);

    if (is_null(right_value)) {
      _impl = std::make_unique<IsNullTableScanImpl>(_in_table, _left_column_id, _scan_type);

      return;
    }

    _impl = std::make_unique<SingleColumnTableScanImpl>(_in_table, _left_column_id, _scan_type, right_value);
  } else /* is_column_name(_right_parameter) */ {
    const auto right_column_id = boost::get<ColumnID>(_right_parameter);

    _impl = std::make_unique<ColumnComparisonTableScanImpl>(_in_table, _left_column_id, _scan_type, right_column_id);
  }
}

void TableScan::_init_output_table() {
  _output_table = std::make_shared<Table>();

  for (ColumnID column_id{0}; column_id < _in_table->col_count(); ++column_id) {
    _output_table->add_column_definition(_in_table->column_name(column_id), _in_table->column_type(column_id));
  }
}

std::shared_ptr<const Table> TableScan::__on_execute_between() {
  if (_scan_type != ScanType::OpBetween) {
    return nullptr;
  }

  DebugAssert(static_cast<bool>(_right_value2), "Scan type BETWEEN requires a right_value2");
  PerformanceWarning("TableScan executes BETWEEN as two separate selects");

  auto table_scan1 =
      std::make_shared<TableScan>(_input_left, _left_column_id, ScanType::OpGreaterThanEquals, _right_parameter);
  table_scan1->execute();

  auto table_scan2 =
      std::make_shared<TableScan>(table_scan1, _left_column_id, ScanType::OpLessThanEquals, *_right_value2);
  table_scan2->execute();

  return table_scan2->get_output();
}

}  // namespace opossum
