enable_language(ASM)

function(EMBED_LLVM OUTPUT_FILE)
    cmake_parse_arguments(llvm "" "EXPORT_MACRO" "" ${ARGN})
    set(INPUT_FILES "${llvm_UNPARSED_ARGUMENTS}")

    set(CPP_BUNDLE_FILE "${CMAKE_CURRENT_BINARY_DIR}/embed_llvm/jit_llvm_bundle.cpp")
    set(LLVM_BUNDLE_FILE "${CMAKE_CURRENT_BINARY_DIR}/embed_llvm/jit_llvm_bundle.bc")
    set(ASM_FILE "${CMAKE_CURRENT_BINARY_DIR}/embed_llvm/jit_llvm_bundle.s")

    file(WRITE ${CPP_BUNDLE_FILE} "")
    foreach(FILE ${INPUT_FILES})
        message(STATUS "Adding ${FILE} to LLVM bundle")
        get_filename_component(ABSOLUTE_FILE ${FILE} ABSOLUTE)
        file(APPEND ${CPP_BUNDLE_FILE} "#include \"${ABSOLUTE_FILE}\"\n")
    endforeach()

    set(FLAGS -std=c++17 -O3 -fwhole-program-vtables -flto ${CMAKE_CXX_FLAGS})
    add_custom_command(
        OUTPUT ${LLVM_BUNDLE_FILE}
        COMMAND ${CMAKE_CXX_COMPILER} ${FLAGS} -DSOURCE_PATH_SIZE -I${CMAKE_CURRENT_SOURCE_DIR} -I${PROJECT_SOURCE_DIR}/third_party/sql-parser/src -emit-llvm -o ${LLVM_BUNDLE_FILE} ${CPP_BUNDLE_FILE}
        DEPENDS ${CPP_BUNDLE_FILE} ${INPUT_FILES})
    set_source_files_properties(${LLVM_BUNDLE_FILE} PROPERTIES GENERATED TRUE)

    file(WRITE ${ASM_FILE} "
        .global _ZN7opossum15jit_llvm_bundleE
        .global __ZN7opossum15jit_llvm_bundleE
        .global _ZN7opossum20jit_llvm_bundle_sizeE
        .global __ZN7opossum20jit_llvm_bundle_sizeE
        _ZN7opossum15jit_llvm_bundleE:
        __ZN7opossum15jit_llvm_bundleE:
        .incbin \"${LLVM_BUNDLE_FILE}\"
        1:
        _ZN7opossum20jit_llvm_bundle_sizeE:
        __ZN7opossum20jit_llvm_bundle_sizeE:
        .8byte 1b - _ZN7opossum15jit_llvm_bundleE"
    )
    set_source_files_properties(${ASM_FILE} PROPERTIES GENERATED TRUE OBJECT_DEPENDS ${LLVM_BUNDLE_FILE})
    set(${OUTPUT_FILE} ${ASM_FILE} PARENT_SCOPE)
endfunction()
