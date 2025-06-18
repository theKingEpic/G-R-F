# RelGo

# Prerequisite

Prepare dataset.
```
mkdir resource
```
The example dataset, queries, and parameters are available at `relgo/` of `https://github.com/Louyk14/gstest.git`.
Please copy the contents of `relgo/` to the created `resource` folder.


Install Java 11, Protobuf, [nlohmann_json](https://json.nlohmann.me/integration/package_managers/#macports), [GOpt](https://github.com/shirly121/GraphScope/tree/ir_fix_type_infer).

## for MacOS
Install java11 
```
java <= 11.0.19 will cause unexpected errors, java==11.0.27 is recommended
e.g., brew install openjdk@11

nano ~/.zshrc
export JAVA_HOME="java/home/path"
export PATH="/java/home/path/to/bin:$PATH"

source ~/.zshrc
```

Install nlohmann_json:
```
brew install nlohmann-json
```

Install Protobuf
```
Download source code from https://github.com/protocolbuffers/protobuf/releases?page=9.
// e.g., Protocol Buffers v21.3

cd protobuf-21.3
./autogen.sh
./configure --host=arm64-apple-darwin --prefix=$HOME/protobuf
make -j4
make install

nano ~/.zshrc

// add protobuf to $PATH and $CMAKE_PREFIX_PATH
export PATH="$HOME/protobuf/bin:$PATH"
export CMAKE_PREFIX_PATH="$HOME/protobuf/include:$CMAKE_PREFIX_PATH"
export CMAKE_PREFIX_PATH="$HOME/protobuf/bin:$CMAKE_PREFIX_PATH"
export CMAKE_PREFIX_PATH="$HOME/protobuf/lib:$CMAKE_PREFIX_PATH"

source ~/.zshrc
```

Install GOpt
```
git clone -b ir_fix_type_infer https://github.com/shirly121/GraphScope.git
cd GraphScope/interactive_engine
mvn clean package -DskipTests -Pgraph-planner-jni
cd assembly/target
tar xvzf graph-planner-jni.tar.gz

replace the folder `resource/graph-planner-jni` with the obtained folder
```

## for Linux
Install java11
```
java>=11.0.25 is recommended
```

Install nlohmann_json
```
sudo apt-get install nlohmann-json3-dev
```

Install Protobuf
```
protobuf v3.21.12 is recommended
```

Install GOpt
```
Same as for Mac
```

# Step 1: Clone DuckDB
```
git clone -b v0.9.2 https://github.com/duckdb/duckdb.git
cd duckdb
git checkout ad9f533b4676d1bb7b38152fadd41d3496354b3a
```

# Step 2: Add RelGo as a third party
Copy this project to the `third_party` folder of the DuckDB Project.

# Step 3: Modify the DuckDB code
There are two optional ways to modify the DuckDB code.

## Option (1)
Copy the file `relgo root/patch/relgo_changes.patch` to the root of DuckDB and execute 
```
git apply patch/relgo_changes.patch
```

## Option (2)

### 1. Modify the CMakeLists.txt
i. add jni/protobuf and third party in `root/CMakeLists.txt`
```
find_package(Protobuf REQUIRED)
include_directories(${Protobuf_INCLUDE_DIRS})

set(JAVA_AWT_INCLUDE_PATH NotNeeded)
find_package(JNI QUIET)
if (JNI_FOUND)
    include_directories(SYSTEM ${JAVA_INCLUDE_PATH})
    include_directories(SYSTEM ${JAVA_INCLUDE_PATH2})
else()
    message(FATAL_ERROR "JNI not found")
endif()

add_subdirectory(examples/embedded-c++)
```

ii. add third_party in `root/src/CMakeLists.txt`
```
set(DUCKDB_LINK_LIBS
      ${DUCKDB_SYSTEM_LIBS}
      duckdb_fsst
      duckdb_fmt
      duckdb_pg_query
      duckdb_re2
      duckdb_miniz
      duckdb_utf8proc
      duckdb_hyperloglog
      duckdb_fastpforlib
      duckdb_mbedtls
      duckdb_relgo) --- this is new
```

iii. add third_party in `root/third_party/CMakeLists.txt`
```.
if(NOT AMALGAMATION_BUILD)
  add_subdirectory(fmt)
  add_subdirectory(libpg_query)
  add_subdirectory(re2)
  add_subdirectory(miniz)
  add_subdirectory(utf8proc)
  add_subdirectory(hyperloglog)
  add_subdirectory(fastpforlib)
  add_subdirectory(mbedtls)
  add_subdirectory(fsst)
  add_subdirectory(relgo) --- this is new
endif()
```

### 2. Change the scope of variables and functions
i. In `root/src/include/duckdb/storage/table/row_group.hpp`, change the scope of variables `collection`, `version_info`, `columns`, `row_group_lock`, `stats_lock`, `column_pointers`, `is_loaded`, `deletes_pointers`, and `deletes_is_loaded` from private to public.

ii. In `root/src/include/duckdb/main/client_context.hpp`, change the scope of functions from `ParseStatements` to `PendingQueryInternal` from private to protected.

### 3. Minor Changes
i. In `root/src/include/duckdb/common/enums/logical_operator_type.hpp`, add a new type to LogicalOperatorType, i.e.,

```
enum class LogicalOperatorType : uint8_t {
    ...
    LOGICAL_CREATE_RAI_INDEX = 138,
    ...
}
```

ii. In `root/src/include/duckdb/main/connection.hpp`, add a new included file as follows:
```
#include "../../third_party/relgo/relgo_statement/relgo_client_context.hpp"
```
Then, change the type of `context` of class `Connection` from  `shared_ptr<ClientContext>` to `shared_ptr<RelGoClientContext>`.
```
class Connection {
    ...
    shared_ptr<RelGoClientContext> context;
    ...
};
```
Similarly, in `root/src/main/connection.cpp`, initialize object typed Connection with RelGoClientContext
```
Connection::Connection(DatabaseInstance &database) : context(make_shared<RelGoClientContext>(database.shared_from_this())) {
    ...
}
```

## Step 4: Execute the code
In `root/examples/embedded-c++/main.cpp`, change the file to try RelGo.
Specifically, replace `main.cpp` and `CMakeLists.txt` with those in folder `examples`.