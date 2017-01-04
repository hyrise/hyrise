#include "insert.hpp"

#include <memory>
#include <string>

namespace opossum {

Insert::Insert(std::shared_ptr<GetTable> get_table, std::vector<AllTypeVariant>&& values)
    : AbstractOperator(get_table), _table(std::const_pointer_cast<Table>(get_table->get_output())), _values(values) {}

const std::string Insert::name() const { return "Insert"; }

uint8_t Insert::num_in_tables() const { return 1; }

uint8_t Insert::num_out_tables() const { return 0; }

void Insert::execute() {
  const auto TID = 10;  // from TransactionManager
  // const auto last_CID = 5;  // from TransactionManager

  size_t row_id;
  size_t chunk_id;

  // Lock to get actual Table row.
  {
    std::lock_guard<std::mutex> lock(*_table->append_mtx);

    // Append row. Might create new chunk with new MVCC columns
    _table->append(_values);

    // get atomic chunk_id and row_id
    chunk_id = _table->chunk_count() - 1;
    row_id = _table->get_chunk(chunk_id)._TIDs.size() - 1;
  }

  // Set the mvcc column values for chunk:
  auto& chunk = _table->get_chunk(chunk_id);

  // Uncommitted:
  chunk._TIDs[row_id] = TID;

  // Commit in progress:
  const auto my_CID = 7;  // from TransactionManager
  chunk._begin_CIDs[row_id] = my_CID;
  chunk._TIDs[row_id] = 0;
  // TransactionManager.finishedCommit(my_CID);

  // Comitted!
}

std::shared_ptr<const Table> Insert::get_output() const { return nullptr; }
}  // namespace opossum
