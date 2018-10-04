PROJ_DIR=~/programs/gam/code
GAM_CORE=${PROJ_DIR}/src
TPCC_DIR=${PROJ_DIR}/database/tpcc
TEST_DIR=${PROJ_DIR}/database/test
CUR_DIR=`pwd`
cd ${GAM_CORE} && make clean && make -j && cd ${TPCC_DIR} && make clean && make -j && cd ${TEST_DIR} && make clean && make -j && cd ${CUR_DIR}
#cd ${GAM_CORE} && make clean && make -j && cd ${TPCC_DIR} && make clean && make -j && cd ${CUR_DIR}
