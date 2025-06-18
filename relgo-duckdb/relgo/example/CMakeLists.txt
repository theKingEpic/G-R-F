cmake_minimum_required(VERSION 2.8.12)
project(example-c++)

set(CMAKE_CXX_STANDARD 17)

# add_compile_options(-fsanitize=address,undefined -static-libasan -static-libubsan)
# add_link_options(-fsanitize=address,undefined -static-libasan -static-libubsan)

include_directories(../../src/include)
link_directories(../../build/release/src)

add_executable(main main.cpp)
target_link_libraries(main duckdb)

target_include_directories(main
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/src/include  # 项目内头文件
        ${DUCKDB_INCLUDE_DIR}                    # 第三方库头文件路径
)