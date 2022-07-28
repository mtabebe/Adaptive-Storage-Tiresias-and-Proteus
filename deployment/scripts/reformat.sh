rootDir=/hdd1
baseDirName=dyna-mast
baseDirectory="$rootDir/$baseDirName"

help=false
options=":hr:"
while getopts $options option
do
    case $option in
        h) help=true;;
				r) baseDirectory=$OPTARG;;
        \? ) echo "Unknown option: -$OPTARG" >&2; exit 1;;
        :  ) echo "Missing option argument for -$OPTARG" >&2; exit 1;;
        *  ) echo "Unimplemented option: -$OPTARG" >&2; exit 1;;
    esac
done
shift $((OPTIND -1))

repoName="Adapt-HTAP"
projectDir="$baseDirectory/$repoName"

usage() {
	if [ $help = true ]; then
    echo "$0 [-h -r R]"
    echo "Reformats code to clang format rules"
    echo "-r R: use R as the root directory"
    exit 0
  fi
}

usage


curDir=$(pwd)

codeDir="$projectDir/code"
srcDir="$codeDir/src"
testDir="$codeDir/test"
driverDir="$codeDir/drivers"
styleFile="$codeDir/.clang-format"
echo "styleFile=$styleFile"

find "$srcDir" "$testDir" "$driverDir" -iname *.h -o -iname *.cpp -o -iname *.tcc | grep -v "gen-cpp" | xargs -I {} sh -c "echo {}; clang-format -i -style=file {}"

# reformat oltpbench
# find src/com/oltpbenchmark/hdb/ src/com/oltpbenchmark/benchmarks/hdb*/ -iname *.java | xargs -I {} sh -c "echo {}; clang-format -i {}"
