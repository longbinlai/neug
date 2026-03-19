# LIF Neural Simulation Extension for NeuG
# Provides Leaky Integrate-and-Fire neural network simulation capabilities

# Collect source files
file(GLOB LIF_SIM_EXTENSION_SOURCES
    "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cc"
    "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp")
set(LIF_SIM_EXTENSION_OBJECT_FILES ${LIF_SIM_EXTENSION_SOURCES})

# Use the extension build system
build_extension_lib("lif_sim")

# Set include directories
target_include_directories(neug_lif_sim_extension PRIVATE
    ${CMAKE_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CMAKE_BINARY_DIR}
)

if(ARROW_INCLUDE_DIRS)
    target_include_directories(neug_lif_sim_extension PRIVATE SYSTEM ${ARROW_INCLUDE_DIRS})
endif()

target_link_libraries(neug_lif_sim_extension PRIVATE
    neug
)

# Build test
if(BUILD_TEST)
    add_extension_test(NAME lif_sim
        TEST_SRCS ${CMAKE_CURRENT_SOURCE_DIR}/tests/lif_sim_test.cpp
    )
endif()
