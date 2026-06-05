find_package(Python 3.12 REQUIRED COMPONENTS Development.Module)

add_subdirectory(${CMAKE_SOURCE_DIR}/third_party/nanobind ${CMAKE_CURRENT_BINARY_DIR}/nanobind)
nanobind_add_module(_chronos NB_STATIC module.cpp)
target_link_libraries(_chronos PRIVATE chronos::core)