#pragma once

/**
 * Clustering to apply after loading the benchmark data:
 *   - "None" does not apply any clustering. For most TPC-H and TPC-DS that means that the order of the data generator
 *     is used.
 *   - "Pruning" is a clustering that improves the pruning rates in TPC-H.
 */
enum class ClusteringConfiguration { None, Pruning };
