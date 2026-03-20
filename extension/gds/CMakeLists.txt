# Collect source files
file(GLOB GDS_EXTENSION_SOURCES
    "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cc"
    "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp")
set(GDS_EXTENSION_OBJECT_FILES ${GDS_EXTENSION_SOURCES})

# Use the extension build system
build_extension_lib("gds")

# Set include directories
target_include_directories(neug_gds_extension PRIVATE
    ${CMAKE_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CMAKE_CURRENT_BINARY_DIR})

if(ARROW_INCLUDE_DIRS)
    target_include_directories(neug_gds_extension PRIVATE SYSTEM ${ARROW_INCLUDE_DIRS})
endif()

target_link_libraries(neug_gds_extension PRIVATE
    neug
)

# Build test by compiling extension sources directly
if(BUILD_TEST)
    add_extension_test(NAME gds
        TEST_SRCS ${CMAKE_CURRENT_SOURCE_DIR}/tests/gds_test.cpp
    )
endif()