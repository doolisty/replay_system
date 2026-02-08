cmake_minimum_required(VERSION 3.14)
project(ReplaySystem VERSION 1.0.0 LANGUAGES CXX)

# C++20 标准
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# 生成 compile_commands.json，供 clangd 等工具识别 C++ 标准与编译选项
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# quill (logging)
add_subdirectory(3rdparty/quill)

# 编译选项
if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    add_compile_options(-Wall -Wextra -Wpedantic)
    if(CMAKE_BUILD_TYPE STREQUAL "Release")
        add_compile_options(-O3 -march=native)
    endif()
elseif(MSVC)
    add_compile_options(/W4)
    if(CMAKE_BUILD_TYPE STREQUAL "Release")
        add_compile_options(/O2)
    endif()
endif()

# 查找线程库
find_package(Threads REQUIRED)

# 源文件
set(COMMON_SOURCES
    src/common/Message.hpp
    src/common/RingBuffer.hpp
    src/common/SpinLock.hpp
    src/common/Logging.hpp
    src/common/Types.hpp
    src/common/CpuAffinity.hpp
)

set(SERVER_SOURCES
    src/server/MktDataServer.hpp
    src/server/MktDataServer.cpp
)

set(CLIENT_SOURCES
    src/client/MktDataClient.hpp
    src/client/MktDataClient.cpp
)

set(RECORDER_SOURCES
    src/recorder/MktDataRecorder.hpp
    src/recorder/MktDataRecorder.cpp
)

set(REPLAY_SOURCES
    src/replay/ReplayEngine.hpp
    src/replay/ReplayEngine.cpp
)

set(CHANNEL_SOURCES
    src/channel/IChannel.hpp
    src/channel/SharedMemChannel.hpp
    src/channel/FileChannel.hpp
)

# 主程序库
add_library(replay_lib STATIC
    ${SERVER_SOURCES}
    ${CLIENT_SOURCES}
    ${RECORDER_SOURCES}
    ${REPLAY_SOURCES}
)

target_include_directories(replay_lib PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/src
)

target_link_libraries(replay_lib PUBLIC
    Threads::Threads
    quill::quill
)

# 主可执行文件
add_executable(replay_system src/main.cpp)
target_link_libraries(replay_system PRIVATE replay_lib)

# 测试配置
option(BUILD_TESTS "Build unit tests" ON)

if(BUILD_TESTS)
    # 尝试查找 Google Test
    find_package(GTest QUIET)
    
    if(GTest_FOUND)
        enable_testing()
        
        add_executable(test_recovery test/test_recovery.cpp test/test_main.cpp)
        target_link_libraries(test_recovery PRIVATE replay_lib GTest::gtest GTest::gtest_main)
        target_include_directories(test_recovery PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
        
        add_executable(test_consistency test/test_consistency.cpp test/test_main.cpp)
        target_link_libraries(test_consistency PRIVATE replay_lib GTest::gtest GTest::gtest_main)
        target_include_directories(test_consistency PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
        
        add_executable(test_stress test/test_stress.cpp test/test_main.cpp)
        target_link_libraries(test_stress PRIVATE replay_lib GTest::gtest GTest::gtest_main)
        target_include_directories(test_stress PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
        
        add_executable(test_benchmark test/test_benchmark.cpp test/test_main.cpp)
        target_link_libraries(test_benchmark PRIVATE replay_lib GTest::gtest GTest::gtest_main)
        target_include_directories(test_benchmark PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
        
        add_test(NAME RecoveryTest COMMAND test_recovery)
        add_test(NAME ConsistencyTest COMMAND test_consistency)
        add_test(NAME StressTest COMMAND test_stress)
        add_test(NAME BenchmarkTest COMMAND test_benchmark)
    else()
        message(STATUS "Google Test not found. Building tests without GTest framework.")
        
        add_executable(test_recovery test/test_recovery.cpp test/test_main.cpp)
        target_link_libraries(test_recovery PRIVATE replay_lib)
        target_include_directories(test_recovery PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
        
        add_executable(test_consistency test/test_consistency.cpp test/test_main.cpp)
        target_link_libraries(test_consistency PRIVATE replay_lib)
        target_include_directories(test_consistency PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
        
        add_executable(test_stress test/test_stress.cpp test/test_main.cpp)
        target_link_libraries(test_stress PRIVATE replay_lib)
        target_include_directories(test_stress PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
        
        add_executable(test_benchmark test/test_benchmark.cpp test/test_main.cpp)
        target_link_libraries(test_benchmark PRIVATE replay_lib)
        target_include_directories(test_benchmark PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
    endif()
endif()

# 多进程方案（可选）
option(BUILD_MULTIPROCESS "Build multi-process executables" OFF)

if(BUILD_MULTIPROCESS)
    add_executable(ipc_server multiprocess/ipc_server.cpp)
    target_link_libraries(ipc_server PRIVATE replay_lib)
    target_include_directories(ipc_server PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
    
    add_executable(ipc_client multiprocess/ipc_client.cpp)
    target_link_libraries(ipc_client PRIVATE replay_lib)
    target_include_directories(ipc_client PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
    
    add_executable(ipc_recorder multiprocess/ipc_recorder.cpp)
    target_link_libraries(ipc_recorder PRIVATE replay_lib)
    target_include_directories(ipc_recorder PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
    
    # POSIX 共享内存（Linux）
    if(UNIX AND NOT APPLE)
        target_link_libraries(ipc_server PRIVATE rt)
        target_link_libraries(ipc_client PRIVATE rt)
        target_link_libraries(ipc_recorder PRIVATE rt)
    endif()
endif()

# 创建数据目录
file(MAKE_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/data)

# 安装配置
install(TARGETS replay_system RUNTIME DESTINATION bin)

# 打印配置信息
message(STATUS "")
message(STATUS "=== ReplaySystem Configuration ===")
message(STATUS "Build type: ${CMAKE_BUILD_TYPE}")
message(STATUS "C++ Standard: ${CMAKE_CXX_STANDARD}")
message(STATUS "Build tests: ${BUILD_TESTS}")
message(STATUS "Build multiprocess: ${BUILD_MULTIPROCESS}")
message(STATUS "==================================")
message(STATUS "")
