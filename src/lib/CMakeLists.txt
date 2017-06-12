# Configure protobuf and grpc
add_custom_command(
    OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/network/generated
    COMMAND [ -d ${CMAKE_CURRENT_SOURCE_DIR}/network/generated ] || mkdir ${CMAKE_CURRENT_SOURCE_DIR}/network/generated
)

add_custom_command(
    OUTPUT
        ${CMAKE_CURRENT_SOURCE_DIR}/network/generated/opossum.grpc.pb.cc
        ${CMAKE_CURRENT_SOURCE_DIR}/network/generated/opossum.grpc.pb.h
    COMMAND
        ${PROJECT_SOURCE_DIR}/third_party/grpc/bins/opt/protobuf/protoc
            --grpc_out=./network/generated
            --plugin=protoc-gen-grpc=${PROJECT_SOURCE_DIR}/third_party/grpc/bins/opt/grpc_cpp_plugin
            -I=\"./network/protos/\"
            ./network/protos/opossum.proto
    DEPENDS
        ${CMAKE_CURRENT_SOURCE_DIR}/network/generated
        ./network/protos/opossum.proto
    WORKING_DIRECTORY
        ${CMAKE_CURRENT_SOURCE_DIR}
)

add_custom_command(
    OUTPUT
        ${CMAKE_CURRENT_SOURCE_DIR}/network/generated/opossum.pb.cc
        ${CMAKE_CURRENT_SOURCE_DIR}/network/generated/opossum.pb.h
    COMMAND
        ${PROJECT_SOURCE_DIR}/third_party/grpc/bins/opt/protobuf/protoc
            --cpp_out=./network/generated
            -I=\"./network/protos/\"
            ./network/protos/opossum.proto
    DEPENDS
        ${CMAKE_CURRENT_SOURCE_DIR}/network/generated
        ./network/protos/opossum.proto
    WORKING_DIRECTORY
        ${CMAKE_CURRENT_SOURCE_DIR}
)

# https://cmake.org/cmake/help/v3.0/command/add_custom_command.html:
#   Do not list the output in more than one independent target that may build in parallel or the two instances of the
#   rule may conflict (instead use add_custom_target to drive the command and make the other targets depend on that one).
# This implements the solution proposed by
#    http://cmake.3232098.n2.nabble.com/How-to-have-generated-source-compiled-multiple-ways-td7585202.html
add_custom_target(
    serialize_protobuf_and_grpc_generation
    DEPENDS
        ${CMAKE_CURRENT_SOURCE_DIR}/network/generated/opossum.grpc.pb.cc
        ${CMAKE_CURRENT_SOURCE_DIR}/network/generated/opossum.grpc.pb.h
        ${CMAKE_CURRENT_SOURCE_DIR}/network/generated/opossum.pb.cc
        ${CMAKE_CURRENT_SOURCE_DIR}/network/generated/opossum.pb.h
)

# Protobuf library with overridden deprecated warning
add_library(
    opossumProtobuf
    STATIC
    network/generated/opossum.pb.cc
    network/generated/opossum.grpc.pb.cc
)

target_compile_options(opossumProtobuf PRIVATE -Wno-unused-parameter -Wno-deprecated-declarations)

# Sources and libraries shared among the different builds of the lib
set(
    SOURCES
    all_parameter_variant.hpp
    all_type_variant.hpp
    common.hpp
    concurrency/commit_context.cpp
    concurrency/commit_context.hpp
    concurrency/transaction_context.cpp
    concurrency/transaction_context.hpp
    concurrency/transaction_manager.cpp
    concurrency/transaction_manager.hpp
    import_export/binary.hpp
    import_export/csv.hpp
    import_export/csv_converter.hpp
    import_export/csv_non_rfc_parser.cpp
    import_export/csv_non_rfc_parser.hpp
    import_export/csv_rfc_parser.cpp
    import_export/csv_rfc_parser.hpp
    import_export/csv_writer.cpp
    import_export/csv_writer.hpp
    network/operator_translator.cpp
    network/operator_translator.hpp
    network/request_handler.cpp
    network/request_handler.hpp
    network/response_builder.hpp
    network/server.cpp
    network/server.hpp
    network/server_configuration.hpp
    operators/abstract_join_operator.cpp
    operators/abstract_join_operator.hpp
    operators/abstract_operator.cpp
    operators/abstract_operator.hpp
    operators/abstract_read_only_operator.hpp
    operators/abstract_read_write_operator.hpp
    operators/aggregate.cpp
    operators/aggregate.hpp
    operators/commit_records.cpp
    operators/commit_records.hpp
    operators/delete.cpp
    operators/delete.hpp
    operators/difference.cpp
    operators/difference.hpp
    operators/export_binary.cpp
    operators/export_binary.hpp
    operators/export_csv.cpp
    operators/export_csv.hpp
    operators/get_table.cpp
    operators/get_table.hpp
    operators/import_binary.cpp
    operators/import_binary.hpp
    operators/import_csv.cpp
    operators/import_csv.hpp
    operators/index_column_scan.cpp
    operators/index_column_scan.hpp
    operators/insert.cpp
    operators/insert.hpp
    operators/join_hash.cpp
    operators/join_hash.hpp
    operators/join_nested_loop_a.cpp
    operators/join_nested_loop_a.hpp
    operators/join_nested_loop_b.cpp
    operators/join_nested_loop_b.hpp
    operators/limit.cpp
    operators/limit.hpp
    operators/print.cpp
    operators/print.hpp
    operators/product.cpp
    operators/product.hpp
    operators/projection.cpp
    operators/projection.hpp
    operators/rollback_records.cpp
    operators/rollback_records.hpp
    operators/sort.cpp
    operators/sort.hpp
    operators/table_scan.cpp
    operators/table_scan.hpp
    operators/table_wrapper.cpp
    operators/table_wrapper.hpp
    operators/term.hpp
    operators/termfactory.hpp
    operators/union_all.cpp
    operators/union_all.hpp
    operators/update.cpp
    operators/update.hpp
    operators/validate.cpp
    operators/validate.hpp
    resolve_type.hpp
    scheduler/abstract_scheduler.cpp
    scheduler/abstract_scheduler.hpp
    scheduler/abstract_task.cpp
    scheduler/abstract_task.hpp
    scheduler/current_scheduler.cpp
    scheduler/current_scheduler.hpp
    scheduler/job_task.cpp
    scheduler/job_task.hpp
    scheduler/node_queue_scheduler.cpp
    scheduler/node_queue_scheduler.hpp
    scheduler/operator_task.cpp
    scheduler/operator_task.hpp
    scheduler/processing_unit.cpp
    scheduler/processing_unit.hpp
    scheduler/task_queue.cpp
    scheduler/task_queue.hpp
    scheduler/topology.cpp
    scheduler/topology.hpp
    scheduler/worker.cpp
    scheduler/worker.hpp
    storage/base_attribute_vector.hpp
    storage/base_column.hpp
    storage/chunk.cpp
    storage/chunk.hpp
    storage/column_visitable.hpp
    storage/copyable_atomic.hpp
    storage/dictionary_column.cpp
    storage/dictionary_column.hpp
    storage/dictionary_compression.cpp
    storage/dictionary_compression.hpp
    storage/fitted_attribute_vector.hpp
    storage/index/adaptive_radix_tree/adaptive_radix_tree_index.cpp
    storage/index/adaptive_radix_tree/adaptive_radix_tree_index.hpp
    storage/index/adaptive_radix_tree/adaptive_radix_tree_nodes.cpp
    storage/index/adaptive_radix_tree/adaptive_radix_tree_nodes.hpp
    storage/index/base_index.hpp
    storage/index/group_key/composite_group_key_index.cpp
    storage/index/group_key/composite_group_key_index.hpp
    storage/index/group_key/group_key_index.hpp
    storage/index/group_key/variable_length_key.cpp
    storage/index/group_key/variable_length_key.hpp
    storage/index/group_key/variable_length_key_base.cpp
    storage/index/group_key/variable_length_key_base.hpp
    storage/index/group_key/variable_length_key_proxy.cpp
    storage/index/group_key/variable_length_key_proxy.hpp
    storage/index/group_key/variable_length_key_store.cpp
    storage/index/group_key/variable_length_key_store.hpp
    storage/reference_column.cpp
    storage/reference_column.hpp
    storage/scoped_locking_ptr.hpp
    storage/storage_manager.cpp
    storage/storage_manager.hpp
    storage/table.cpp
    storage/table.hpp
    storage/untyped_dictionary_column.hpp
    storage/value_column.cpp
    storage/value_column.hpp
    tasks/chunk_compression_task.cpp
    tasks/chunk_compression_task.hpp
    type_cast.cpp
    type_cast.hpp
    type_comparison.hpp
    types.hpp
    uid_allocator.hpp
    utils/cuckoo_hashtable.hpp
    utils/helper.cpp
    utils/helper.hpp
    utils/murmur_hash.cpp
    utils/murmur_hash.hpp
)

set(
    LIBRARIES
    opossumProtobuf
    protobuf
    grpc++
    grpc
    pthread
    z
    tbb
)

if(${NUMA_FOUND})
    set(LIBRARIES ${LIBRARIES} ${NUMA_LIBRARY})
endif()


# Configure the regular opossum library used for tests/server/playground...
add_library(opossum STATIC ${SOURCES})
target_link_libraries(opossum ${LIBRARIES})
add_dependencies(opossum serialize_protobuf_and_grpc_generation)

# Configure the lib used for asan
add_library(opossumAsanLib STATIC ${SOURCES})
target_link_libraries(
    opossumAsanLib
    ${LIBRARIES}
    -lgcov
    -fsanitize=address
)
set_target_properties(opossumAsanLib PROPERTIES COMPILE_FLAGS "-fsanitize=address -fno-omit-frame-pointer")
add_dependencies(opossumAsanLib serialize_protobuf_and_grpc_generation)

# Configure the lib used for coverage
add_library(opossumCoverageLib STATIC ${SOURCES})
target_link_libraries(
    opossumCoverageLib
    ${LIBRARIES}
    -lgcov
    --coverage
)
set_target_properties(opossumCoverageLib PROPERTIES COMPILE_FLAGS "-fprofile-arcs -ftest-coverage")
add_dependencies(opossumCoverageLib serialize_protobuf_and_grpc_generation)
