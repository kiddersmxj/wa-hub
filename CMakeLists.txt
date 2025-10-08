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

add_executable(wa-runner  src/wa-runner.cpp)
target_link_libraries(wa-runner PRIVATE CURL::libcurl nlohmann_json::nlohmann_json Threads::Threads)
target_compile_definitions(wa-runner PRIVATE _FILE_OFFSET_BITS=64)

# Install: binaries only
install(TARGETS wa-hub wa-sub wa-runner RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})

# Optional: install helper tool (OFF by default)
option(INSTALL_TOOLS "Install tools/* helpers" OFF)
if(INSTALL_TOOLS)
  if(EXISTS "${CMAKE_SOURCE_DIR}/tools/wa-fifo-append.sh")
    install(PROGRAMS ${CMAKE_SOURCE_DIR}/tools/wa-fifo-append.sh
            DESTINATION ${CMAKE_INSTALL_BINDIR}
            RENAME wa-fifo-append)
  endif()
endif()

# Define configurable variables (they can be set or overridden before invoking install)
# set(PRGM_CONF_INSTALL_DIR ".config/wa-hub" 
#     CACHE STRING "Directory (relative to the user home) where the configuration file will be installed")
#   set(PRGM_CONF_FILE_NAME ".hamza.conf" 
#     CACHE STRING "The desired hamza configuration file name")
#   set(PRGM_SOURCE_FILE "${CMAKE_SOURCE_DIR}/config.conf"
#     CACHE FILEPATH "Path to the source hamza configuration file")

# install(CODE "
#     # Determine the original user
#     if(DEFINED ENV{SUDO_USER} AND NOT \"\$ENV{SUDO_USER}\" STREQUAL \"root\")
#         set(ORIGINAL_USER \"\$ENV{SUDO_USER}\")
#     else()
#         set(ORIGINAL_USER \"\$ENV{USER}\")
#     endif()

#     # Get the home directory of the original user
#     execute_process(
#         COMMAND sh -c \"getent passwd \${ORIGINAL_USER} | cut -d: -f6\"
#         OUTPUT_VARIABLE USER_HOME
#         OUTPUT_STRIP_TRAILING_WHITESPACE
#     )

#     if(NOT USER_HOME)
#         message(FATAL_ERROR \"Failed to determine home directory for user '\${ORIGINAL_USER}'\")
#     endif()

#     # Inject the user-defined values into the install script.
#     set(CONFIG_INSTALL_DIR \"${PRGM_CONF_INSTALL_DIR}\")
#     set(CONFIG_FILE_NAME \"${PRGM_CONF_FILE_NAME}\")
#     set(SOURCE_FILE \"${PRGM_SOURCE_FILE}\")

#     # Construct the destination directory and file path.
#     set(CONFIG_DIR \"\${USER_HOME}/\${CONFIG_INSTALL_DIR}\")
#     set(CONFIG_FILE \"\${CONFIG_DIR}/\${CONFIG_FILE_NAME}\")

#     # Ensure that the destination directory is created (including any intermediate directories)
#     file(MAKE_DIRECTORY \"\${CONFIG_DIR}\")

#     if(NOT EXISTS \"\${CONFIG_FILE}\")
#         message(\"Installing configuration file to \${CONFIG_FILE}\")

#         # Copy the source file to the destination directory with the desired file permissions.
#         file(COPY \"\${SOURCE_FILE}\" DESTINATION \"\${CONFIG_DIR}\" 
#              FILE_PERMISSIONS OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ)

#         # If the source file name differs from the desired target name, then rename it accordingly.
#         get_filename_component(SOURCE_BASENAME \"\${SOURCE_FILE}\" NAME)
#         if(NOT \"\${SOURCE_BASENAME}\" STREQUAL \"\${CONFIG_FILE_NAME}\")
#             file(RENAME \"\${CONFIG_DIR}/\${SOURCE_BASENAME}\" \"\${CONFIG_FILE}\")
#         endif()
#     else()
#         message(\"Configuration file already exists at \${CONFIG_FILE}\")
#     endif()
# ")
