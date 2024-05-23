build_type=Debug
use_cpp11=1
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
  -DUSE_CPP11=$use_cpp11
make