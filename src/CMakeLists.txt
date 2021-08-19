file (GLOB pactree_SRCS "*.h" "*.cpp")
add_library(pactree STATIC
    ${pactree_SRCS}
)
target_include_directories(pactree PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
)
add_executable (main main.cpp)
target_link_libraries (main pactree numa jemalloc pdlart pmemobj pmem)
INSTALL(TARGETS pactree 
        ARCHIVE DESTINATION ${CMAKE_SOURCE_DIR}
        )
