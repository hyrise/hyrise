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
    common.hpp
    network/server_configuration.hpp
    network/response_builder.hpp
    network/request_handler.hpp
    network/operator_translator.hpp
    network/server.hpp
    network/operator_translator.cpp
    network/request_handler.cpp
    network/server.cpp
    types.cpp
    type_comparison.hpp
    storage/fitted_attribute_vector.hpp
    storage/chunk.hpp
    storage/chunk.cpp
    storage/table.hpp
    storage/reference_column.cpp
    storage/base_column.hpp
    storage/index/group_key/composite_group_key_index.hpp
    storage/index/group_key/variable_length_key_proxy.cpp
    storage/index/group_key/variable_length_key_proxy.hpp
    storage/index/group_key/variable_length_key_store.hpp
    storage/index/group_key/variable_length_key_store.cpp
    storage/index/group_key/variable_length_key.hpp
    storage/index/group_key/variable_length_key.cpp
    storage/index/group_key/group_key_index.hpp
    storage/index/group_key/composite_group_key_index.cpp
    storage/index/group_key/variable_length_key_base.cpp
    storage/index/group_key/variable_length_key_base.hpp
    storage/index/adaptive_radix_tree/adaptive_radix_tree_index.cpp
    storage/index/adaptive_radix_tree/adaptive_radix_tree_nodes.cpp
    storage/index/adaptive_radix_tree/adaptive_radix_tree_index.hpp
    storage/index/adaptive_radix_tree/adaptive_radix_tree_nodes.hpp
    storage/index/base_index.hpp
    storage/value_column.hpp
    storage/dictionary_column.hpp
    storage/column_visitable.hpp
    storage/storage_manager.hpp
    storage/storage_manager.cpp
    storage/reference_column.hpp
    storage/untyped_dictionary_column.hpp
    storage/table.cpp
    storage/base_attribute_vector.hpp
    concurrency/transaction_context.hpp
    concurrency/transaction_manager.hpp
    concurrency/transaction_manager.cpp
    concurrency/commit_context.hpp
    concurrency/transaction_context.cpp
    concurrency/commit_context.cpp
    import_export/csv_non_rfc_parser.cpp
    import_export/csv_converter.hpp
    import_export/csv_non_rfc_parser.hpp
    import_export/binary.hpp
    import_export/csv_writer.cpp
    import_export/csv.hpp
    import_export/csv_writer.hpp
    import_export/csv_rfc_parser.hpp
    import_export/csv_rfc_parser.cpp
    uid_allocator.hpp
    utils/murmur_hash.hpp
    utils/cuckoo_hashtable.hpp
    utils/murmur_hash.cpp
    types.hpp
    scheduler/topology.hpp
    scheduler/node_queue_scheduler.hpp
    scheduler/current_scheduler.hpp
    scheduler/processing_unit.hpp
    scheduler/job_task.cpp
    scheduler/abstract_scheduler.cpp
    scheduler/processing_unit.cpp
    scheduler/abstract_task.hpp
    scheduler/worker.cpp
    scheduler/topology.cpp
    scheduler/worker.hpp
    scheduler/node_queue_scheduler.cpp
    scheduler/task_queue.hpp
    scheduler/task_queue.cpp
    scheduler/operator_task.hpp
    scheduler/operator_task.cpp
    scheduler/job_task.hpp
    scheduler/current_scheduler.cpp
    scheduler/abstract_task.cpp
    scheduler/abstract_scheduler.hpp
    operators/get_table.hpp
    operators/import_binary.cpp
    operators/projection.cpp
    operators/join_nested_loop_a.hpp
    operators/union_all.hpp
    operators/sort.hpp
    operators/sort.cpp
    operators/join_hash.cpp
    operators/abstract_operator.hpp
    operators/print.cpp
    operators/index_column_scan.hpp
    operators/export_binary.hpp
    operators/delete.cpp
    operators/aggregate.hpp
    operators/rollback_records.hpp
    operators/table_scan.hpp
    operators/print.hpp
    operators/import_binary.hpp
    operators/export_csv.cpp
    operators/insert.cpp
    operators/import_csv.hpp
    operators/commit_records.cpp
    operators/product.cpp
    operators/validate.hpp
    operators/import_csv.cpp
    operators/insert.hpp
    operators/update.cpp
    operators/abstract_read_only_operator.hpp
    operators/validate.cpp
    operators/abstract_join_operator.hpp
    operators/export_binary.cpp
    operators/table_wrapper.hpp
    operators/export_csv.hpp
    operators/join_nested_loop_a.cpp
    operators/abstract_join_operator.cpp
    operators/difference.hpp
    operators/join_nested_loop_b.hpp
    operators/commit_records.hpp
    operators/difference.cpp
    operators/projection.hpp
    operators/union_all.cpp
    operators/termfactory.hpp
    operators/abstract_read_write_operator.hpp
    operators/abstract_operator.cpp
    operators/term.hpp
    operators/delete.hpp
    operators/join_hash.hpp
    operators/table_scan.cpp
    operators/join_nested_loop_b.cpp
    operators/table_wrapper.cpp
    operators/update.hpp
    operators/get_table.cpp
    operators/product.hpp
    operators/rollback_records.cpp
    operators/aggregate.cpp
    operators/index_column_scan.cpp
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

# Configure the lib used for asan
add_library(opossumAsanLib STATIC ${SOURCES})
target_link_libraries(
    opossumAsanLib
    ${LIBRARIES}
    -lgcov
    -fsanitize=address
)
set_target_properties(opossumAsanLib PROPERTIES COMPILE_FLAGS "-fsanitize=address -fno-omit-frame-pointer")

# Configure the lib used for coverage
add_library(opossumCoverageLib STATIC ${SOURCES})
target_link_libraries(
    opossumCoverageLib
    ${LIBRARIES}
    -lgcov
    --coverage
)
set_target_properties(opossumCoverageLib PROPERTIES COMPILE_FLAGS "-fprofile-arcs -ftest-coverage")
