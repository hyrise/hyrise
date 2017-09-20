#include "index_column_scan.hpp"

#include <algorithm>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "storage/base_attribute_vector.hpp"
#include "storage/column_visitable.hpp"

#include "resolve_type.hpp"

namespace opossum {

IndexColumnScan::IndexColumnScan(const std::shared_ptr<AbstractOperator> in, const ColumnID column_id,
                                 const ScanType scan_type, const AllTypeVariant value,
                                 const optional<AllTypeVariant> value2)
    : AbstractReadOnlyOperator(in), _column_id(column_id), _scan_type(scan_type), _value(value), _value2(value2) {}

const std::string IndexColumnScan::name() const { return "IndexColumnScan"; }

uint8_t IndexColumnScan::num_in_tables() const { return 1; }

uint8_t IndexColumnScan::num_out_tables() const { return 1; }

std::shared_ptr<const Table> IndexColumnScan::_on_execute() {
  _impl = make_unique_by_column_type<AbstractReadOnlyOperatorImpl, IndexColumnScanImpl>(
      _input_table_left()->column_type(_column_id), _input_left, _column_id, _scan_type, _value, _value2);
  return _impl->_on_execute();
}

void IndexColumnScan::_on_cleanup() { _impl.reset(); }

// we need to use the impl pattern because the scan operator of the sort depends on the type of the column
template <typename T>
class IndexColumnScan::IndexColumnScanImpl : public AbstractReadOnlyOperatorImpl, public ColumnVisitable {
 public:
  // IndexColumnScan currently does not support ScanType::OpLike
  // creates a new table with reference columns
  IndexColumnScanImpl(const std::shared_ptr<const AbstractOperator> in, const ColumnID column_id,
                      const ScanType scan_type, const AllTypeVariant value, const optional<AllTypeVariant> value2)
      : _in_operator(in),
        _column_id(column_id),
        _scan_type(scan_type),
        _casted_value(type_cast<T>(value)),
        _casted_value2(value2 ? optional<T>(type_cast<T>(*value2)) : optional<T>(nullopt)) {}

  struct ScanContext : ColumnVisitableContext {
    ScanContext(std::shared_ptr<const Table> t, ChunkID c, std::vector<RowID> &mo,
                std::shared_ptr<std::vector<ChunkOffset>> co = nullptr)
        : table_in(t), chunk_id(c), matches_out(mo), chunk_offsets_in(std::move(co)) {}

    // constructor for use in ReferenceColumn::visit_dereferenced
    ScanContext(std::shared_ptr<BaseColumn>, const std::shared_ptr<const Table> referenced_table,
                std::shared_ptr<ColumnVisitableContext> base_context, ChunkID chunk_id,
                std::shared_ptr<std::vector<ChunkOffset>> chunk_offsets)
        : table_in(referenced_table),
          chunk_id(chunk_id),
          matches_out(std::static_pointer_cast<ScanContext>(base_context)->matches_out),
          chunk_offsets_in(chunk_offsets) {}

    std::shared_ptr<const Table> table_in;
    const ChunkID chunk_id;
    std::vector<RowID> &matches_out;
    std::shared_ptr<std::vector<ChunkOffset>> chunk_offsets_in;
  };

  std::shared_ptr<const Table> _on_execute() override {
    auto output = std::make_shared<Table>();

    auto in_table = _in_operator->get_output();
    for (ColumnID column_id{0}; column_id < in_table->col_count(); ++column_id) {
      output->add_column_definition(in_table->column_name(column_id), in_table->column_type(column_id));
    }

    // Definining all possible operators here might appear odd. Chances are, however, that we will not
    // have a similar comparison anywhere else. Index scans, for example, would not use an adaptable binary
    // predicate, but will have to use different methods (lower_range, upper_range, ...) based on the
    // chosen operator. For now, we can save us some dark template magic by using the switch below.
    // DO NOT copy this code, however, without discussing if there is a better way to avoid code duplication.

    // we need these copies so that they can be captured by the lambdas below
    T casted_value = _casted_value;

    switch (_scan_type) {
      case ScanType::OpEquals: {
        _value_comparator = [casted_value](T val) { return val == casted_value; };
        _value_id_comparator = [](ValueID found_vid, ValueID search_vid, ValueID) { return found_vid == search_vid; };
        break;
      }
      case ScanType::OpNotEquals: {
        _value_comparator = [casted_value](T val) { return val != casted_value; };
        _value_id_comparator = [](ValueID found_vid, ValueID search_vid, ValueID) { return found_vid != search_vid; };
        break;
      }
      case ScanType::OpLessThan: {
        _value_comparator = [casted_value](T val) { return val < casted_value; };
        _value_id_comparator = [](ValueID found_vid, ValueID search_vid, ValueID) { return found_vid < search_vid; };
        break;
      }
      case ScanType::OpLessThanEquals: {
        _value_comparator = [casted_value](T val) { return val <= casted_value; };
        _value_id_comparator = [](ValueID found_vid, ValueID search_vid, ValueID) { return found_vid < search_vid; };
        //                                                                                           ^
        //                                                               sic! see handle_dictionary_column for details
        break;
      }
      case ScanType::OpGreaterThan: {
        _value_comparator = [casted_value](T val) { return val > casted_value; };
        _value_id_comparator = [](ValueID found_vid, ValueID search_vid, ValueID) { return found_vid >= search_vid; };
        break;
      }
      case ScanType::OpGreaterThanEquals: {
        _value_comparator = [casted_value](T val) { return val >= casted_value; };
        _value_id_comparator = [](ValueID found_vid, ValueID search_vid, ValueID) { return found_vid >= search_vid; };
        break;
      }
      case ScanType::OpBetween: {
        DebugAssert(static_cast<bool>(_casted_value2), "No second value for BETWEEN comparison given");
        T casted_value2 = _casted_value2.value_or(T());
        _value_comparator = [casted_value, casted_value2](T val) {
          return casted_value <= val && val <= casted_value2;
        };
        _value_id_comparator = [](ValueID found_vid, ValueID search_vid, ValueID search_vid2) {
          return search_vid <= found_vid && found_vid < search_vid2;
        };
        break;
      }
      default:
        Fail(std::string("Unsupported operator."));
    }

    // We can easily distribute the table scanning work on individual chunks to multiple sub tasks,
    // we just need to synchronize access to the output table
    std::mutex output_mutex;
    std::vector<std::shared_ptr<AbstractTask>> jobs;
    jobs.reserve(in_table->chunk_count());

    for (ChunkID chunk_id{0}; chunk_id < in_table->chunk_count(); ++chunk_id) {
      jobs.emplace_back(std::make_shared<JobTask>([&in_table, chunk_id, &output_mutex, &output, this]() {
        const Chunk &chunk_in = in_table->get_chunk(chunk_id);
        Chunk chunk_out;
        auto base_column = chunk_in.get_column(_column_id);
        std::vector<RowID> matches_in_this_chunk;

        base_column->visit(*this, std::make_shared<ScanContext>(in_table, chunk_id, matches_in_this_chunk));

        // We now receive the visits in the handler methods below...

        // Even if we don't have any matches in this chunk, we need to correctly set the output columns.
        // If we would just return here, we would end up with a Chunk without Columns,
        // which makes the output unusable for further operations (-> OperatorsIndexColumnScanTest::ScanWithEmptyInput)

        // Ok, now we have a list of the matching positions relative to this chunk (ChunkOffsets). Next, we have to
        // transform them into absolute row ids. To save time and space, we want to share PosLists between columns
        // as much as possible. All ValueColumns and DictionaryColumns can share the same PosLists because they use
        // no
        // further  redirection. For ReferenceColumns, PosLists can be shared between two columns iff (a) they point
        // to the same table and (b) the incoming ReferenceColumns point to the same positions in the same order.
        // To make this check easier, we share PosLists between two ReferenceColumns iff they shared PosLists in
        // the incoming table as well. _filtered_pos_lists will hold a mapping from incoming PosList to outgoing
        // PosList. Because Value/DictionaryColumns do not have an incoming PosList, they are represented with
        // nullptr.
        std::map<std::shared_ptr<const PosList>, std::shared_ptr<PosList>> filtered_pos_lists;
        for (ColumnID column_id{0}; column_id < in_table->col_count(); ++column_id) {
          auto ref_col_in = std::dynamic_pointer_cast<ReferenceColumn>(chunk_in.get_column(column_id));
          std::shared_ptr<const PosList> pos_list_in;
          std::shared_ptr<const Table> referenced_table_out;
          ColumnID referenced_column_id;
          if (ref_col_in) {
            pos_list_in = ref_col_in->pos_list();
            referenced_table_out = ref_col_in->referenced_table();
            referenced_column_id = ref_col_in->referenced_column_id();
          } else {
            referenced_table_out = in_table;
            referenced_column_id = column_id;
          }

          // automatically creates the entry if it does not exist
          std::shared_ptr<PosList> &pos_list_out = filtered_pos_lists[pos_list_in];

          if (!pos_list_out) {
            pos_list_out = std::make_shared<PosList>();
            pos_list_out->reserve(matches_in_this_chunk.size());
            std::copy(matches_in_this_chunk.begin(), matches_in_this_chunk.end(), std::back_inserter(*pos_list_out));
          }

          auto ref_col_out =
              std::make_shared<ReferenceColumn>(referenced_table_out, referenced_column_id, pos_list_out);
          chunk_out.add_column(ref_col_out);
        }

        {
          std::lock_guard<std::mutex> lock(output_mutex);
          output->add_chunk(std::move(chunk_out));
        }
      }));
      jobs.back()->schedule();
    }

    CurrentScheduler::wait_for_tasks(jobs);

    return output;
  }

  void handle_value_column(BaseColumn &base_column, std::shared_ptr<ColumnVisitableContext> base_context) override {
    auto context = std::static_pointer_cast<ScanContext>(base_context);
    const auto &column = static_cast<ValueColumn<T> &>(base_column);
    const auto &values = column.values();
    auto &matches_out = context->matches_out;

    if (context->chunk_offsets_in) {
      // This ValueColumn is referenced by a ReferenceColumn (i.e., is probably filtered). We only return the matching
      // rows within the filtered column, together with their original position
      for (const ChunkOffset &offset_in_value_column : *(context->chunk_offsets_in)) {
        if (_value_comparator(values[offset_in_value_column])) {
          matches_out.emplace_back(RowID{context->chunk_id, offset_in_value_column});
        }
      }
    } else {
      // This ValueColumn has to be scanned in full. We directly insert the results into the list of matching rows.
      ChunkOffset chunk_offset = 0;
      for (const auto &value : values) {
        if (_value_comparator(value)) matches_out.emplace_back(RowID{context->chunk_id, chunk_offset});
        chunk_offset++;
      }
    }
  }

  void handle_reference_column(ReferenceColumn &column, std::shared_ptr<ColumnVisitableContext> base_context) override {
    column.visit_dereferenced<ScanContext>(*this, base_context);
  }

  void handle_dictionary_column(BaseColumn &base_column,
                                std::shared_ptr<ColumnVisitableContext> base_context) override {
    /*
     ValueID x;
     T A;
     optional<T> B;

     A ValueID x from the attribute vector is included in the result iff

     Operator          | Condition
     x == A            | dict.value_by_value_id(dict.lower_bound(A)) == A && x == dict.lower_bound(A)
     x != A            | dict.value_by_value_id(dict.lower_bound(A)) != A || x != dict.lower_bound(A)
     x <  A            | x < dict.lower_bound(A)
     x <= A            | x < dict.upper_bound(A)
     x >  A            | x >= dict.upper_bound(A)
     x >= A            | x >= dict.lower_bound(A)
     x between A and B | x >= dict.lower_bound(A) && x < dict.upper_bound(B)
     */

    auto context = std::static_pointer_cast<ScanContext>(base_context);
    const auto &column = static_cast<DictionaryColumn<T> &>(base_column);
    auto &matches_out = context->matches_out;

    // get the indices for this column
    auto in_table = _in_operator->get_output();
    const Chunk &chunk_in = in_table->get_chunk(context->chunk_id);
    auto col = chunk_in.get_column(_column_id);
    auto indices = chunk_in.get_indices_for(std::vector<std::shared_ptr<BaseColumn>>{col});

    if (!indices.empty()) {
      // with index

      // Get the index
      auto index = indices.front();

      // Get the (sorted) complete pos list from the index
      auto complete_pos_list = get_pos_list_from_index(index, _casted_value, _casted_value2);

      if (context->chunk_offsets_in) {
        // Sort the incoming filtering pos list (and copy it to not make unwanted modifications)
        auto filtering_list = context->chunk_offsets_in;
        std::sort(filtering_list->begin(), filtering_list->end());

        // Then, intersect them to get the filtered pos list
        auto intersected_list = std::vector<ChunkOffset>(filtering_list->size());
        std::set_intersection(complete_pos_list.begin(), complete_pos_list.end(), filtering_list->begin(),
                              filtering_list->end(), intersected_list.begin());

        for (ChunkOffset offset : intersected_list) {
          matches_out.emplace_back(RowID{context->chunk_id, offset});
        }
      } else {
        // This DictionaryColumn has to be scanned in full. We directly insert the results into the list of matching
        // rows.
        for (ChunkOffset offset : complete_pos_list) {
          matches_out.emplace_back(RowID{context->chunk_id, offset});
        }
      }
    } else {
      // without index

      ValueID search_vid;
      ValueID search_vid2 = INVALID_VALUE_ID;

      switch (_scan_type) {
        case ScanType::OpEquals:
        case ScanType::OpNotEquals:
        case ScanType::OpLessThan:
        case ScanType::OpGreaterThanEquals: {
          search_vid = column.lower_bound(_casted_value);
          break;
        }
        case ScanType::OpLessThanEquals:
        case ScanType::OpGreaterThan: {
          search_vid = column.upper_bound(_casted_value);
          break;
        }
        case ScanType::OpBetween: {
          search_vid = column.lower_bound(_casted_value);
          search_vid2 = column.upper_bound(*_casted_value2);
          break;
        }
        default:
          Fail("Unknown comparison type encountered");
      }

      if (_scan_type == ScanType::OpEquals && search_vid != INVALID_VALUE_ID &&
          column.value_by_value_id(search_vid) != _casted_value) {
        // the value is not in the dictionary and cannot be in the table
        return;
      }

      if (_scan_type == ScanType::OpNotEquals && search_vid != INVALID_VALUE_ID &&
          column.value_by_value_id(search_vid) != _casted_value) {
        // the value is not in the dictionary and cannot be in the table
        search_vid = INVALID_VALUE_ID;
      }

      const BaseAttributeVector &attribute_vector = *(column.attribute_vector());

      if (context->chunk_offsets_in) {
        for (const ChunkOffset &offset_in_dictionary_column : *(context->chunk_offsets_in)) {
          if (_value_id_comparator(attribute_vector.get(offset_in_dictionary_column), search_vid, search_vid2)) {
            matches_out.emplace_back(RowID{context->chunk_id, offset_in_dictionary_column});
          }
        }
      } else {
        // This DictionaryColumn has to be scanned in full. We directly insert the results into the list of matching
        // rows.
        for (ChunkOffset chunk_offset = 0; chunk_offset < column.size(); ++chunk_offset) {
          if (_value_id_comparator(attribute_vector.get(chunk_offset), search_vid, search_vid2)) {
            matches_out.emplace_back(RowID{context->chunk_id, chunk_offset});
          }
        }
      }
    }
  }

  std::vector<ChunkOffset> get_pos_list_from_index(std::shared_ptr<BaseIndex> index, T search_value,
                                                   optional<T> search_value_2) {
    BaseIndex::Iterator lower_bound, upper_bound;

    std::vector<ChunkOffset> result;

    switch (_scan_type) {
      case ScanType::OpEquals: {
        lower_bound = index->lower_bound({search_value});
        upper_bound = index->upper_bound({search_value});
        break;
      }
      case ScanType::OpNotEquals: {
        // first, get all values less than the search value
        lower_bound = index->cbegin();
        upper_bound = index->lower_bound({search_value});
        result.insert(result.cend(), lower_bound, upper_bound);

        // set range for second half to all values greater than the search value
        lower_bound = index->upper_bound({search_value});
        upper_bound = index->cend();
        break;
      }
      case ScanType::OpLessThan: {
        lower_bound = index->cbegin();
        upper_bound = index->lower_bound({search_value});
        break;
      }
      case ScanType::OpGreaterThanEquals: {
        lower_bound = index->lower_bound({search_value});
        upper_bound = index->cend();
        break;
      }
      case ScanType::OpLessThanEquals: {
        lower_bound = index->cbegin();
        upper_bound = index->upper_bound({search_value});
        break;
      }
      case ScanType::OpGreaterThan: {
        lower_bound = index->upper_bound({search_value});
        upper_bound = index->cend();
        break;
      }
      case ScanType::OpBetween: {
        lower_bound = index->lower_bound({search_value});
        upper_bound = index->upper_bound({*search_value_2});
        break;
      }
      default:
        Fail("Unknown comparison type encountered");
    }

    result.insert(result.end(), lower_bound, upper_bound);

    return result;
  }

  const std::shared_ptr<const AbstractOperator> _in_operator;
  ColumnID _column_id;
  ScanType _scan_type;
  std::function<bool(T)> _value_comparator;
  std::function<bool(ValueID, ValueID, ValueID)> _value_id_comparator;
  const T _casted_value;
  const optional<T> _casted_value2;
  // by adding a second, optional parameter to the function, we could easily support between as well
};

}  // namespace opossum
