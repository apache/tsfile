# build_type=MinSizeRel
build_type=Debug
build_test=0
build_bench=0
use_cpp11=1
enable_cov=0
debug_se=0
run_cov_only=0
env_for_cyber=0

shell_dir=$(cd "$(dirname "$0")";pwd)

function print_config()
{
  echo "build_type=$build_type"
  echo "build_test=$build_test"
  echo "use_cpp11=$use_cpp11"
  echo "enable_cov=$enable_cov"
  echo "enable_asan=$enable_asan"
}

function run_test_for_cov()
{
  # sh ${shell_dir}/scripts/regression_unittest.sh
  sh ${shell_dir}/test/libtsfile_test/run_sdk_tests.sh 
}

parse_options()
{
  while test $# -gt 0
  do
    case "$1" in
    clean)
      do_clean=1;;
    run_cov)
      run_cov_only=1;;
    -t=*)
      build_type=`get_key_value "$1"`;;
    -t)
      shift
      build_type=`get_key_value "$1"`;;
    -a=*)
      enable_asan=`get_key_value "$1"`;;
    -a)
      shift
      enable_asan=`get_key_value "$1"`;;
    -c=*)
      enable_cov=`get_key_value "$1"`;;
    -c)
      shift
      enable_cov=`get_key_value "$1"`;;
    #-h | --help)
    #  usage
    #  exit 0;;
    #*)
    #  echo "Unknown option '$1'"
    #  exit 1;;
    esac
    shift
  done
}

parse_options $*
print_config

if [[ ${run_cov_only} -eq 1 ]]
then
  do_cov
  # exit after run coverage test.
  exit
fi

if [ ${build_test} -eq 1 ]
then
  compile_gtest
  echo "building status: build gtest success."
fi

if [[ ${env_for_cyber} -eq 1 ]]
then
  export PATH=$PATH:~/dev/gcc-linaro-5.5.0-2017.10-x86_64_arm-linux-gnueabi/bin
  export CROSS_COMPILE=arm-linux-gnueabi-
  export ARCH=arm
  export CC=${CROSS_COMPILE}gcc
  export CXX=${CROSS_COMPILE}g++
  echo "set up gcc for cyber"
else
  unset CC
  unset CXX
fi

echo "build using: build_type=$build_type"
if [ ${build_type} == "Debug" ]
then
  mkdir -p build/Debug
  cd build/Debug
elif [ ${build_type} == "Release" ]
then
  mkdir -p build/Release
  cd build/Release
elif [ ${build_type} == "RelWithDebInfo" ]
then
  mkdir -p build/relwithdebinfo
  cd build/relwithdebinfo
elif [ ${build_type} == "MinSizeRel" ]
then
  mkdir -p build/minsizerel
  cd build/minsizerel
else
  echo ""
  echo "unknow build type: ${build_type}, valid build types(case intensive): Debug, Release, RelWithDebInfo, MinSizeRel"
  echo ""
  exit 1
fi

cmake ../../                           \
  -DGTEST=$gtest_project_dir           \
  -DZLIB=$zlib_project_dir/install     \
  -DLZ4LIB=$lz4lib_project_dir         \
  -DBUILD_TEST=$build_test             \
  -DCMAKE_BUILD_TYPE=$build_type       \
  -DUSE_CPP11=$use_cpp11               \
  -DENABLE_COV=$enable_cov             \
  -DENABLE_ASAN=$enable_asan           \
  -DDEBUG_SE=$debug_se                 \
  -DBUILD_TSFILE_ONLY=$build_tsfile_only

VERBOSE=1 make
VERBOSE=1 make install







