cmake_minimum_required(VERSION 3.20)

project(mini_rpc LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)


add_library(mini_rpc
        src/RpcMessage.cc
        src/RpcServer.cc
        src/RpcClient.cc
        src/TaskThreadPool.cc
)

target_include_directories(mini_rpc PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/src/include
)

add_subdirectory(
        ${CMAKE_CURRENT_SOURCE_DIR}/../mini_muduo
        ${CMAKE_CURRENT_BINARY_DIR}/mini_muduo_build
)

target_link_libraries(mini_rpc PUBLIC mini_muduo)

target_compile_options(mini_rpc PRIVATE
        -Wall
        -Wextra
        -Wpedantic
)

function(add_example name)
    add_executable(${name} examples/${name}.cc)
    target_link_libraries(${name} PRIVATE mini_rpc)
    target_compile_options(${name} PRIVATE -Wall -Wextra -Wpedantic)
endfunction()

add_example(rpc_echo_example)
add_example(rpc_echo_server_example)
add_example(rpc_echo_client_example)
add_example(rpc_calculator_example)
add_example(rpc_echo_benchmark)


include(CTest)
if (BUILD_TESTING)
    function(add_mini_test name)
        add_executable(${name} tests/${name}.cc)
        target_link_libraries(${name} PRIVATE mini_rpc)
        add_test(NAME ${name} COMMAND ${name})
    endfunction()

    add_mini_test(rpc_message_test)
    add_mini_test(rpc_server_test)
    add_mini_test(task_thread_pool_test)
endif ()


function(add_calculator_library library_name file_name)
    add_library(${library_name} STATIC
            calculator/${file_name}.cc
    )
    target_include_directories(${library_name} PUBLIC
            ${CMAKE_CURRENT_SOURCE_DIR}/calculator
    )
    target_compile_features(${library_name} PUBLIC cxx_std_17)
endfunction()

add_calculator_library(calculator_protocol CalculatorProtocol)
add_calculator_library(calculator_client CalculatorClient)
add_calculator_library(calculator_service CalculatorService)

target_link_libraries(calculator_client PUBLIC mini_rpc calculator_protocol)
target_link_libraries(calculator_service PUBLIC mini_rpc calculator_protocol)


if (BUILD_TESTING)
    add_executable(calculator_protocol_test
            tests/calculator_protocol_test.cc
    )

    target_link_libraries(calculator_protocol_test PRIVATE
            calculator_client
            calculator_service
    )

    add_test(
            NAME calculator_protocol_test
            COMMAND calculator_protocol_test
    )
endif ()

target_link_libraries(rpc_calculator_example PRIVATE calculator_client calculator_service)