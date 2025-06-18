cmake_minimum_required(VERSION 3.1)

set(TARGET_NAME relgo)
set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
if(POLICY CMP0063)
    cmake_policy(SET CMP0063 NEW)
endif()

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -frtti")
add_subdirectory(relgo/third_party/yaml-cpp EXCLUDE_FROM_ALL)

set(CMAKE_CXX_STANDARD 17)
set(JAVA_AWT_INCLUDE_PATH NotNeeded)
find_package(JNI QUIET)
if (JNI_FOUND)
    include_directories(SYSTEM ${JAVA_INCLUDE_PATH})
    include_directories(SYSTEM ${JAVA_INCLUDE_PATH2})
else()
    message(FATAL_ERROR "JNI not found")
endif()

find_package(Protobuf REQUIRED)
include_directories(${Protobuf_INCLUDE_DIRS})

set(PLAN_CONVERTOR_PROTO_DIR  ${CMAKE_CURRENT_SOURCE_DIR}/relgo/proto/)
# proto files of gie compiler
set(CODE_GEN_PROTOBUF_FILES
       ${PLAN_CONVERTOR_PROTO_DIR}/algebra.proto
        ${PLAN_CONVERTOR_PROTO_DIR}/common.proto
        ${PLAN_CONVERTOR_PROTO_DIR}/expr.proto
        ${PLAN_CONVERTOR_PROTO_DIR}/physical.proto
        ${PLAN_CONVERTOR_PROTO_DIR}/results.proto
        ${PLAN_CONVERTOR_PROTO_DIR}/schema.proto
        ${PLAN_CONVERTOR_PROTO_DIR}/type.proto
        ${PLAN_CONVERTOR_PROTO_DIR}/stored_procedure.proto
        ${PLAN_CONVERTOR_PROTO_DIR}/basic_type.proto
)

file(MAKE_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/relgo/proto_generated)

# proto gen for gie proto
protobuf_generate(APPEND_PATH
        TARGET ${LOCAL_EXE_NAME}
        LANGUAGE cpp
        OUT_VAR PROTO_SRCS_GIE
        PROTOS ${CODE_GEN_PROTOBUF_FILES}
        IMPORT_DIRS ${PLAN_CONVERTOR_PROTO_DIR}
        PROTOC_OUT_DIR ${CMAKE_CURRENT_SOURCE_DIR}/relgo/proto_generated
)

add_library(ir_proto SHARED ${PROTO_SRCS_GIE})

target_link_libraries(ir_proto
    PUBLIC ${Protobuf_LIBRARIES}
)

include_directories(relgo/graph relgo/graph_index relgo/index relgo/interface relgo/operators relgo/optimizer relgo/parser relgo/utils relgo/graph_index relgo/third_party/nlohmann_json)
include_directories(src/include)

file (GLOB EXTENSION_SOURCES "relgo/graph/*.cpp" "relgo/index/*.cpp" "relgo/operators/*.cpp" "relgo/optimizer/*.cpp" "relgo/parser/*.cpp" "relgo/utils/*.cpp" "relgo/graph_index/*.cpp" "relgo/planner/*.cpp" "relgo/pgq2cypher/*.cpp" "relgo/graph_operators/src/*.cpp" "relgo/create_index/*.cpp", "relgo/relgo_statement/*.cpp" "src/*.cpp")

set(EXTENSION_SOURCES ${EXTENSION_SOURCES})

# create extension targets
build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

target_include_directories(
  ${EXTENSION_NAME}
  PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>/relgo/graph 
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>/relgo/graph_index
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>/relgo/index 
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>/relgo/interface 
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>/relgo/operators 
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>/relgo/optimizer 
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>/relgo/parser 
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>/relgo/utils
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>/relgo/planner
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>/relgo/pgq2cypher
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>/relgo/graph_operators/includes
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>/relgo/create_index
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>/relgo/relgo_statement
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>/src/include
        )
target_link_libraries(${EXTENSION_NAME} ir_proto yaml-cpp ${CMAKE_JNI_LINKER_FLAGS} ${JAVA_JVM_LIBRARY} ${JNI_LIBRARIES} ${Protobuf_LIBRARIES})

target_include_directories(
  ${LOADABLE_EXTENSION_NAME}
  PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>/relgo/graph 
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>/relgo/graph_index
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>/relgo/index 
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>/relgo/interface 
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>/relgo/operators 
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>/relgo/optimizer 
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>/relgo/parser 
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>/relgo/utils
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>/relgo/planner
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>/relgo/pgq2cypher
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>/relgo/graph_operators/includes
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>/relgo/create_index
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>/relgo/relgo_statement
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>/src/include
        )
target_link_libraries(${LOADABLE_EXTENSION_NAME} PUBLIC ir_proto yaml-cpp ${CMAKE_JNI_LINKER_FLAGS} ${JAVA_JVM_LIBRARY} ${JNI_LIBRARIES} ${Protobuf_LIBRARIES})

install(
  TARGETS yaml-cpp ir_proto ${EXTENSION_NAME} 
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")