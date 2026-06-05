add_library(chronos_core STATIC
    src/chronos.cpp
)

target_compile_features(chronos_core PUBLIC cxx_std_23)
target_include_directories(chronos_core PUBLIC 
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)

add_library(chronos::core ALIAS chronos_core)