enable_language(ASM)

# This function helps with embedding the LLVM bitcode representation (LLVM-IR) of some source files into a
# compiled binary to make them available to the just-in-time compilation engine.
#
# Usage: embed_llvm(OUTPUT_FILE ${INPUT_FILES})
# The LLVM bitcode can then be embedded into some binary, by adding ${OUTPUT_FILE} to its sources
#
#
# How it works:
# 1. The file "embed_llvm/jit_llvm_bundle.cpp" is generated. It #include(s) all input files - one per line.
#    This is necessary to compile all source files as a single unit while ensuring that both absolute and relative
#    includes are resolved properly.
# 2. The "embed_llvm/jit_llvm_bundle.cpp" is compiled to a binary LLVM-IR representation (hence the -emit-llvm flag).
#    Only Clang supports this feature, so this function should only be called if CMAKE_CXX_COMPILER is set to Clang 5.0
#    or newer. The compilation result is stored in "embed_llvm/jit_llvm_bundle.bc".
# 3. To embed this file as a resource into a binary, an auxilary assembly file is generated. The file defines the
#    global symbols "opossum::jit_llvm_bundle" and "opossum::jit_llvm_bundle_size" (due to differences in name mangling
#    between macOS and Linux, each symbol must be exported twice).
# 4. The path to the assembly file is returned to the parent scope via the OUTPUT_FILE variable. Any binary linking this
#    file can access the embedded bitcode in the following way:
#
#    extern char opossum::jit_llvm_bundle;
#    extern size_t opossum::jit_llvm_bundle_size;
#
#    std::string bitcode_string(&opossum::jit_llvm_bundle, opossum::jit_llvm_bundle_size);

function(EMBED_LLVM OUTPUT_FILE)
    # Parsing all remaining arguments as input files
    cmake_parse_arguments(llvm "" "EXPORT_MACRO" "" ${ARGN})
    set(INPUT_FILES "${llvm_UNPARSED_ARGUMENTS}")

    # Setting up paths for the files that are being generated
    set(CPP_BUNDLE_FILE "${CMAKE_CURRENT_BINARY_DIR}/embed_llvm/jit_llvm_bundle.cpp")
    set(LLVM_BUNDLE_FILE "${CMAKE_CURRENT_BINARY_DIR}/embed_llvm/jit_llvm_bundle.bc")
    set(ASM_FILE "${CMAKE_CURRENT_BINARY_DIR}/embed_llvm/jit_llvm_bundle.s")

    # Step 1: Including all input files
    file(WRITE ${CPP_BUNDLE_FILE} "")
    foreach(FILE ${INPUT_FILES})
        message(STATUS "Adding ${FILE} to LLVM bundle")
        get_filename_component(ABSOLUTE_FILE ${FILE} ABSOLUTE)
        file(APPEND ${CPP_BUNDLE_FILE} "#include \"${ABSOLUTE_FILE}\"\n")
    endforeach()

    # Step 2: Compiling input files to LLVM-IR
    set(FLAGS -std=c++17 -O3 -fwhole-program-vtables -flto ${CMAKE_CXX_FLAGS})
    add_custom_command(
        OUTPUT ${LLVM_BUNDLE_FILE}
        COMMAND ${CMAKE_CXX_COMPILER} ${FLAGS} -c -emit-llvm -DSOURCE_PATH_SIZE -I${CMAKE_CURRENT_SOURCE_DIR} -I${PROJECT_SOURCE_DIR}/third_party/sql-parser/src -o ${LLVM_BUNDLE_FILE} ${CPP_BUNDLE_FILE}
        DEPENDS ${CPP_BUNDLE_FILE} ${INPUT_FILES})
    set_source_files_properties(${LLVM_BUNDLE_FILE} PROPERTIES GENERATED TRUE)

    # Step 3: Generating the auxilary assembly file
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
