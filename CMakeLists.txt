cmake_minimum_required(VERSION 3.23 FATAL_ERROR)

if (CMAKE_BINARY_DIR STREQUAL CMAKE_SOURCE_DIR)
    message(FATAL_ERROR "In-source builds are not allowed. Please create a separate build directory and run CMake from there.")
endif()

project(chronos
    VERSION 0.1.0
    DESCRIPTION "High-performance streaming RPC engine."
    LANGUAGES CXX
)

if (NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    set (CMAKE_BUILD_TYPE "Release" CACHE STRING "Choose the type of build." FORCE)
endif()

set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS "Debug" "Release" "RelWithDebInfo" "MinSizeRel")
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

add_subdirectory(cpp)
add_subdirectory(bindings/python)
