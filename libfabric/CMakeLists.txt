cmake_minimum_required(VERSION 2.8)
project(oficp C)

set(C_STANDARD 99)

find_library(LIBFABRIC_LIB
        NAMES libfabric.so
        PATHS "/usr/lib/*" "/usr/lib64/*")

include_directories(${PROJECT_SOURCE_DIR}/include
        /usr/include/rdma)

file(GLOB headers ${PROJECT_SOURCE_DIR}/include/*.h)
file(GLOB sources ${PROJECT_SOURCE_DIR}/src/*.c)

add_executable(oficp main.c ${sources} ${headers})

target_link_libraries(oficp
        ${LIBFABRIC_LIB})
