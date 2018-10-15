#pragma once

#include <json.hpp>

#include <string>

#include "constant_mappings.hpp"
#include "storage/encoding_type.hpp"

namespace opossum {

    // Hard-coded to test server
    struct CalibrationConstantHardwareFeatures {
        size_t l1_size_kb = 32;
        size_t l1_block_size_kb = 0;
        size_t l2_size_kb = 256;
        size_t l2_block_size_kb = 0;
        size_t l3_size_kb = 38400;
        size_t l3_block_size_kb = 0;

        size_t memory_size_kb = 2113645780;
        size_t memory_access_bandwidth = 0;
        size_t memory_access_latency = 0;
        size_t num_cpu_cores = 60;
        size_t cpu_clock_speed_mhz = 3100;
        size_t num_numa_nodes = 4;

        size_t cpu_architecture = 0; // Should be ENUM
    };

    inline void to_json(nlohmann::json& j, const CalibrationConstantHardwareFeatures& s) {
        j = nlohmann::json{
                {"l1_size_kb", s.l1_size_kb},
                {"l1_block_size_kb", s.l1_block_size_kb},
                {"l2_size_kb", s.l1_size_kb},
                {"l2_block_size_kb", s.l1_block_size_kb},
                {"l3_size_kb", s.l1_size_kb},
                {"l3_block_size_kb", s.l1_block_size_kb},
                {"memory_size_kb", s.memory_size_kb},
                {"memory_access_bandwidth", s.memory_access_bandwidth},
                {"memory_access_latency", s.memory_access_latency},
                {"num_cpu_cores", s.num_cpu_cores},
                {"cpu_clock_speed_mhz", s.cpu_clock_speed_mhz},
                {"num_numa_nodes", s.num_numa_nodes},
                {"cpu_architecture", s.cpu_architecture}
        };
    }

    inline void from_json(const nlohmann::json& j, CalibrationConstantHardwareFeatures& s) {
        s.l1_size_kb = j.value("l1_size_kb", 0);
        s.l1_block_size_kb = j.value("l1_block_size_kb", 0);
        s.l2_size_kb = j.value("l1_size_kb", 0);
        s.l2_block_size_kb = j.value("l1_block_size_kb", 0);
        s.l3_size_kb = j.value("l1_size_kb", 0);
        s.l3_block_size_kb = j.value("l1_block_size_kb", 0);
        s.memory_size_kb = j.value("memory_size_kb", 0);
        s.memory_access_bandwidth = j.value("memory_access_bandwidth", 0);
        s.memory_access_latency = j.value("memory_access_latency", 0);
        s.num_cpu_cores = j.value("num_cpu_cores", 0);
        s.cpu_clock_speed_mhz = j.value("cpu_clock_speed_mhz", 0);
        s.num_numa_nodes = j.value("num_numa_nodes", 0);
        s.cpu_architecture = j.value("cpu_architecture", 0);
    }

}  // namespace opossum
