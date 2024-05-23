build_type=Debug
env_for_cyber=0
use_cpp11=1

if [[ ${env_for_cyber} -eq 1 ]]
then
  export PATH=$PATH:~/dev/gcc-linaro-5.5.0-2017.10-x86_64_arm-linux-gnueabi/bin
  export CROSS_COMPILE=arm-linux-gnueabi-
  export ARCH=arm
  export CC=${CROSS_COMPILE}gcc
  export CXX=${CROSS_COMPILE}g++
  echo "set up gcc for cyber"
fi


if [ ${build_type} = "Debug" ]
then
  mkdir -p build/Debug
  cd build/Debug
  use_sdk_debug=1
else
  mkdir -p build/Release
  cd build/Release
  use_sdk_debug=0
fi

echo "use_sdk_debug=${use_sdk_debug}"
cmake ../../  \
  -DUSE_SDK_DEBUG=$use_sdk_debug \
  -DUSE_CPP11=$use_cpp11
make
