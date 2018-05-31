#pragma once

namespace opossum {

/**
 * Cost that an AbstractCostModel assigns to an Operator/LQP node. The unit of the Cost is left to the Cost model and could be,
 * e.g., "Estimated Runtime" or "Estimated Memory Usage".
 */
using Cost = float;

}  // namespace opossum
