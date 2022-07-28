set -x
rootDir=/hdd1
baseDirName=dyna-mast
baseDirectory="$rootDir/$baseDirName"

help=false
startPhase=0
endPhase=5
dryRun=false
options=":hdb:e:r:"


while getopts $options option
do
    case $option in
        h) help=true;;
        r) baseDirectory=$OPTARG;;
        b) startPhase=$OPTARG;;
        e) endPhase=$OPTARG;;
        d) dryRun=true;;
        \? ) echo "Unknown option: -$OPTARG" >&2; exit 1;;
        :  ) echo "Missing option argument for -$OPTARG" >&2; exit 1;;
        *  ) echo "Unimplemented option: -$OPTARG" >&2; exit 1;;
    esac
done
shift $((OPTIND -1))

packageInstall() {
  sudo add-apt-repository --yes ppa:ubuntu-toolchain-r/test
  sudo apt-get update
  sudo apt-get --yes install cmake ant gradle libsnappy-dev zlib1g-dev libbz2-dev liblz4-dev libunwind-dev autoconf automake libdouble-conversion-dev gdb libc6-dbg clang-format libboost-all-dev libx11-dev gcc-8 g++-8
  sudo update-alternatives --remove-all gcc
  sudo update-alternatives --remove-all g++q

  sudo update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-8 10
  sudo update-alternatives --install /usr/bin/g++ g++ /usr/bin/g++-8 10

  sudo update-alternatives --install /usr/bin/cc cc /usr/bin/gcc 20
  sudo update-alternatives --set cc /usr/bin/gcc

  sudo update-alternatives --install /usr/bin/c++ c++ /usr/bin/g++ 20
  sudo update-alternatives --set c++ /usr/bin/g++

  # install python3.6 and dependencies to build LibTorch
  sudo add-apt-repository ppa:deadsnakes/ppa --yes
  sudo apt update
  sudo apt-get --yes install python3.6

  wget https://bootstrap.pypa.io/get-pip.py
  sudo python3.6 get-pip.py
  rm get-pip.py

  sudo python3.6 -m pip install setuptools
  sudo python3.6 -m pip install pyyaml
  sudo python3.6 -m pip install typing_extensions
  sudo python3.6 -m pip install dataclasses
}

moveToBaseDirectory() {
  cd $baseDirectory
}

setupBaseDirectory() {
  sudo mkdir -p $baseDirectory
  cd $rootDir
  sudo chmod a+w $baseDirName
  moveToBaseDirectory
}

getAllRepositories() {
  moveToBaseDirectory

  git clone --recursive git@github.com:mtabebe/Adapt-HTAP.git
  git clone git@github.com:mtabebe/oltpbench.git

  git clone git@github.com:edenhill/librdkafka.git
  git clone git@github.com:facebook/folly.git

  wget https://archive.apache.org/dist/kafka/0.10.1.0/kafka_2.11-0.10.1.0.tgz
  tar -xzf kafka_2.11-0.10.1.0.tgz
  wget http://www-us.apache.org/dist/thrift/0.12.0/thrift-0.12.0.tar.gz
  tar -xzf thrift-0.12.0.tar.gz

  git clone git@github.com:gflags/gflags.git
  git clone git@github.com:google/glog.git

  wget http://dlib.net/files/dlib-19.21.tar.bz2
  tar -xf dlib-19.21.tar.bz2

  git clone git@github.com:pytorch/pytorch.git
}

buildMakeInstall() {
  make -j $makeParallel
  sudo make install
}

runCmake() {
  flags="$1"

  cmake ${flags}
}

runConfigure() {
  flags="$1"

  ./configure $flags
}

buildConfigure() {
  flags="${1:-""}"


  runConfigure "$flags"
  buildMakeInstall
}


buildCmake() {
  flags=${1:-""}

  runCmake "$flags"
  buildMakeInstall
}

buildGoogle() {
  buildCmake "-DBUILD_SHARED_LIBS=ON"
}

buildRdKafka() {
  moveToBaseDirectory
  cd librdkafka
  buildConfigure
  moveToBaseDirectory
}

buildThrift() {
  moveToBaseDirectory
  cd thrift-0.12.0
  ./bootstrap.sh
  flags="--with-c_glib --with-cpp --with-java"
  buildConfigure "${flags}"
  moveToBaseDirectory
}

buildGFlags() {
  moveToBaseDirectory
  cd gflags
  buildGoogle
  moveToBaseDirectory
}

buildGLog() {
  moveToBaseDirectory
  cd glog
  buildGoogle
  moveToBaseDirectory
}

buildGTest() {
  moveToBaseDirectory
	cd Adapt-HTAP/code
  cd googletest

  cd googletest
  buildGoogle

  cd ../googlemock
  buildGoogle

  moveToBaseDirectory
}

buildFolly() {
  moveToBaseDirectory

  cd folly/

  buildCmake "."

  moveToBaseDirectory
}

buildDlib() {
  moveToBaseDirectory
  cd dlib-19.21/dlib
  mkdir build
  cd build
  buildCmake ".."
  buildMakeInstall
  moveToBaseDirectory
}

buildLibTorch() {
  moveToBaseDirectory
  cd pytorch

  alias python=python3.6

  # Step #1: Remove deprecated python3 package check
  # See: https://github.com/shanemcandrewai/pytorch-setup#remove-deprecated-find_packagepythonlibs-30

  sed -i 's/find_package(PythonLibs 3.0)/find_package(Python COMPONENTS Interpreter)/' ./cmake/Dependencies.cmake
  sed -i 's/PYTHONLIBS_VERSION_STRING/PYTHON_VERSION/' ./cmake/Dependencies.cmake

  # Step #2: Leverage suggested environment variables to minimize build

  # See: https://discuss.pytorch.org/t/issues-linking-with-libtorch-c-11-abi/29510/2
  export GLIBCXX_USE_CXX11_ABI=1

  # See: https://github.com/pytorch/pytorch/blob/v1.7.0/CONTRIBUTING.md#c-development-tips
  export DEBUG=1
  export BUILD_TEST=0
  export USE_CUDA=0
  export USE_DISTRIBUTED=0
  export USE_MKLDNN=0
  export USE_FBGEMM=0
  export USE_NNPACK=0
  export USE_QNNPACK=0
  export USE_XNNPACK=0
  export BUILD_CAFFE2_OPS=0

  # Step #3: Invoke build scripts accordingly
  python3.6 setup.py build --cmake-only
  cmake --build build --target install

  # Step #4: Extract include headers and shared object libs accordingly
  # Headers
  sudo cp -r torch/include/c10 /usr/local/include
  sudo cp -r torch/include/ATen /usr/local/include
  sudo cp -r torch/include/torch /usr/local/include

  # Shared objects
  sudo cp build/lib/libc10.so /usr/local/lib
  sudo cp build/lib/libtorch.so /usr/local/lib
  sudo cp build/lib/libtorch_cpu.so /usr/local/lib

  sudo ldconfig
}


buildThirdParty() {
  buildRdKafka
  buildThrift
  buildGFlags
  buildGLog
  buildGTest
  buildFolly
  buildDlib
  buildLibTorch
}

buildAdaptHTAP() {
  moveToBaseDirectory

  cd Adapt-HTAP/code
  sudo gradle build --parallel
  # cd ../code; gradleTasks=$(sudo gradle tasks | grep DebugNonOpt  | cut -d "-" -f 1 | paste -sd "" -); sudo gradle $gradleTasks; cd ../deployment/
}

buildOltpbench() {
  moveToBaseDirectory
  cd oltpbench
  ant clean
  ant build

}

usage() {
	if [ $help = true ]; then
    echo "$0 [-h -d -b N -e M -r R]"
    echo "sets up the machine in phases"
    echo "-b N: begins in phase N"
    echo "-e M: end in phase M"
    echo "-d: only logs what commands will be run but does not run them"
    echo "-r R: use R as the root directory"
    echo "-h: prints help message"
    echo "phases:"
    echo -e "\t0:downloads ubunutu packages"
    echo -e "\t1:creates the base directory"
    echo -e "\t2:downloads repositories from github"
    echo -e "\t3:builds third party repositories"
    echo -e "\t4:builds Adapt HTAP"
    echo -e "\t5:builds our benchmark repositories"
    exit 0
	fi
}

exitIf() {
  cond=$1
  if [ "$endPhase" -eq "$cond" ]; then
    exit 0
  fi
}

runIfNotDry() {
  command=$1

	if [ $dryRun = false ]; then
    $command
  fi
}

runCommandIfInPhase() {
  phase=$1
  command=$2
  desc="$3"

  if [ "$startPhase" -le "$phase" ]; then
    echo $desc
    runIfNotDry $command
  fi
  exitIf $phase
}

usage

runCommandIfInPhase 0 packageInstall "downloading packages"
runCommandIfInPhase 1 setupBaseDirectory "creating base directory"
runCommandIfInPhase 2 getAllRepositories "downloading repositories from github"
runCommandIfInPhase 3 buildThirdParty "building third party repositories"
runCommandIfInPhase 4 buildAdaptHTAP "build Adapt HTAP"
runCommandIfInPhase 5 buildOltpbench "build oltpbench"

