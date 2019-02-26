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

bool constraint_satisfied(const Table& table, const TableConstraintDefinition& constraint,
                          const CommitID snapshot_commit_id, const TransactionID our_tid);

/**
 * Checks if a constraint is still satisfied after inserting some values into a table.
 * A start chunk ID can be passed that makes the checker skip all chunks before this chunk ID.
 * Returns a tuple of a bool and a ChunkID. The chunk ID represents the ID of the first chunk
 * that is mutable. Together with the passed chunk ID this is used to skip checking compressed chunks
 * in the commit phase that have already been checked by the operator. The chunk ID of the segment
 * that contained the first value segment chunk is remembered by the operator and then again passed
 * to this function in the commit.
 */
std::tuple<bool, ChunkID> check_constraints_for_values(const std::string& table_name,
                                                           std::shared_ptr<const Table> table_to_insert,
                                                           const CommitID snapshot_commit_id,
                                                           const TransactionID our_tid,
                                                           const ChunkID start_chunk_id = ChunkID{0});

}  // namespace opossum
