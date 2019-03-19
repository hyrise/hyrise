#pragma once

#include <functional>
#include <memory>
#include <set>
#include <string>
#include <tuple>
#include <unordered_set>
#include <vector>
#include "boost/container/small_vector.hpp"

#include "all_type_variant.hpp"
#include "resolve_type.hpp"
#include "storage/constraints/table_constraint_definition.hpp"
#include "storage/mvcc_data.hpp"
#include "storage/segment_accessor.hpp"
#include "storage/table.hpp"
#include "types.hpp"

namespace opossum {

/**
 * @param table table against which the constraint should be checked
 * @param constraint constraint to be checked
 * @param snapshot_commit_id determines visibity. The constraint is checked as if this was the current CommitID
 * @param our_tid All values having this transactionID are also visible to the checker. Can be set to
 *                TransactionManager::UNUSED_TRANSACTION_ID
 */
bool constraint_satisfied(const Table& table, const TableConstraintDefinition& constraint,
                          const CommitID snapshot_commit_id, const TransactionID our_tid);

/**
 * Checks if the constraints of table_name are still satisfied if the given values are inserted. A start
 * chunk ID can be passed that makes the checker skip all chunks in the table_name table before this ChunkID. This
 * is done then the function is called during the commit process. The ChunkID is the one that got returned from
 * the first call to this function in the insert operator.
 *
 * @param table_name name of table against which the constraints should be checked
 * @param table_to_insert temporary table which contains the values to be checked if they can be inserted
 *        without violating a constraint
 * @param snapshot_commit_id determines visibity. The constraints are checked as if this was the current CommitID
 * @param our_tid All values having this transactionID are also visible to the checker. Can be set to
 *                TransactionManager::UNUSED_TRANSACTION_ID
 * @param start_chunk_id ChunkID from which the checker should start on the table_name table
 *
 * @return
 *         .first indicating if the constraints
 *         .second ChunkID of the first Chunk containing ValueSegments. This is rembered so that the immutable
 *                 chunks are not checked again during the commit process
 */
std::tuple<bool, ChunkID> check_constraints_for_values(const std::string& table_name,
                                                           const std::shared_ptr<const Table>& table_to_insert,
                                                           const CommitID snapshot_commit_id,
                                                           const TransactionID our_tid,
                                                           const ChunkID start_chunk_id = ChunkID{0});

}  // namespace opossum
