cmake_minimum_required(VERSION 3.10)
project(fft)

SET(EXECUTABLE_OUTPUT_PATH ${PROJECT_BINARY_DIR}/bin)

set(CMAKE_CXX_STANDARD 14)
# 如果想要生成的可执行文件拥有符号表，可以gdb调试，就直接加上这句
add_definitions("-Wall -g")

find_package(Boost REQUIRED)
find_package(Gflags REQUIRED)

include(FetchContent)
set(FETCHCONTENT_QUIET false)
FetchContent_Declare(
        photon
        GIT_REPOSITORY https://gitee.com/mirrors/photonlibos.git
        GIT_TAG v0.6.1
)
set(ENABLE_URING ON CACHE INTERNAL "Enable iouring")
set(PHOTON_INCLUDE_DIR ${photon_SOURCE_DIR}/include/)
FetchContent_MakeAvailable(photon)

# 第三方库
set(INC ${PROJECT_SOURCE_DIR})
set(GAM ${PROJECT_SOURCE_DIR}/../include  ${PROJECT_SOURCE_DIR}/../lib/libcuckoo/cityhash-1.1.1/src ${PROJECT_SOURCE_DIR}/../lib/libcuckoo/src)

# 规定.h头文件路径
include_directories(${INC}
        ${Boost_INCLUDE_DIR}
        ${GAM}
        ${PHOTON_INCLUDE_DIR}
        /usr/local/include  /user/include SYSTEM
        )
link_directories(/usr/local/lib /usr/lib)

FILE(GLOB MAIN_SRC main.cpp fft.cpp rpc/client.cpp rpc/server.cpp rpc/client_fft.cpp)

set(BOOST boost_filesystem boost_system boost_thread pthread)
set(GAM ${PROJECT_SOURCE_DIR}/../src/libgalloc.a ${PROJECT_SOURCE_DIR}/../lib/libcuckoo/cityhash-1.1.1/src/.libs/libcityhash.a ibverbs)
set(Photon photon_static gflags)

# 生成可执行文件
add_executable(${PROJECT_NAME} ${MAIN_SRC} )
target_link_libraries(${PROJECT_NAME} ${LIBRARIES} ${BOOST} ${GAM} ${Photon})

# 链接操作
#add_library(${PROJECT_NAME}_static STATIC ${LIB_SRC})