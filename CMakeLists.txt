cmake_minimum_required(VERSION 3.16)
project(wa_hub_suite LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
add_compile_options(-Wall -Wextra -Wpedantic)

include(GNUInstallDirs)

# Dependencies
find_package(Threads REQUIRED)
find_package(CURL REQUIRED)                    # wa-hub needs libcurl
find_package(nlohmann_json 3.2.0 QUIET)        # header-only

# Fetch nlohmann/json if not installed
if(NOT nlohmann_json_FOUND)
  include(FetchContent)
  FetchContent_Declare(nlohmann_json
    GIT_REPOSITORY https://github.com/nlohmann/json.git
    GIT_TAG v3.11.3
  )
  FetchContent_MakeAvailable(nlohmann_json)
endif()

# Outputs to ./bin for local runs
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

# Binaries
add_executable(wa-hub  src/wa-hub.cpp)
target_link_libraries(wa-hub PRIVATE CURL::libcurl nlohmann_json::nlohmann_json Threads::Threads)
target_compile_definitions(wa-hub PRIVATE _FILE_OFFSET_BITS=64)

add_executable(wa-sub  src/wa-sub.cpp)
target_link_libraries(wa-sub PRIVATE nlohmann_json::nlohmann_json)
target_compile_definitions(wa-sub PRIVATE _FILE_OFFSET_BITS=64)

# Install: binaries only
install(TARGETS wa-hub wa-sub RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})

# Optional: install helper tool (OFF by default)
option(INSTALL_TOOLS "Install tools/* helpers" OFF)
if(INSTALL_TOOLS)
  if(EXISTS "${CMAKE_SOURCE_DIR}/tools/wa-fifo-append.sh")
    install(PROGRAMS ${CMAKE_SOURCE_DIR}/tools/wa-fifo-append.sh
            DESTINATION ${CMAKE_INSTALL_BINDIR}
            RENAME wa-fifo-append)
  endif()
endif()
