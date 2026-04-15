cmake_minimum_required(VERSION 3.12)
project(partition_pred_test CXX)

add_executable(partition_pred_test partition_pred_test.cpp)
target_compile_definitions(partition_pred_test PUBLIC DEFINE_DEFAULT_ENGINE_BASE)
target_include_directories(partition_pred_test PUBLIC
    "${CMAKE_CURRENT_SOURCE_DIR}/../../../target/ffi-headers")
target_link_directories(partition_pred_test PUBLIC
    "${CMAKE_CURRENT_SOURCE_DIR}/../../../target/debug")
target_link_libraries(partition_pred_test PUBLIC delta_kernel_ffi)
set_target_properties(partition_pred_test PROPERTIES CXX_STANDARD 14 CXX_STANDARD_REQUIRED ON)

if(MSVC)
    target_compile_options(partition_pred_test PRIVATE /W3 /WX)
else()
    target_compile_options(partition_pred_test PRIVATE -Wall -Wextra -Wpedantic -g)
endif()
