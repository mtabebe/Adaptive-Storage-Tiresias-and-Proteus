rootDir=/hdd1
baseDirName=dyna-mast
baseDirectory="$rootDir/$baseDirName"

help=false

outDir="$rootDir/dyna-mast-out"
confFile="$baseDirectory/Adapt-HTAP/deployment/configs/site_manager.cfg"
rewriteBin="$baseDirectory/Adapt-HTAP/code/build/exe/rewrite_persistent_data/release/rewrite_persistent_data"

startPhase=0
endPhase=0
dryRun=false
verbosity=5
options=":hdb:e:o:c:s:v:"

while getopts $options option
do
    case $option in
        h) help=true;;
        b) startPhase=$OPTARG;;
        e) endPhase=$OPTARG;;
        d) dryRun=true;;
        o) outDir=$OPTARG;;
        c) confFile=$OPTARG;;
        s) rewriteBin=$OPTARG;;
        v) verbosity=$OPTARG;;
        \? ) echo "Unknown option: -$OPTARG" >&2; exit 1;;
        :  ) echo "Missing option argument for -$OPTARG" >&2; exit 1;;
        *  ) echo "Unimplemented option: -$OPTARG" >&2; exit 1;;
    esac
done
shift $((OPTIND -1))

usage() {
	if [ $help = true ]; then
    echo "$0 [-h -d -b N -e M -o O -c C -s S -v V]"
    echo "Rewrites the persistent data"
    echo "-b N: begins in phase N"
    echo "-e M: end in phase M"
    echo "-d: only logs what commands will be run but does not run them"
    echo "-o O: output to directory O"
    echo "-c C: uses file C as a config file"
    echo "-s S: uses binary S as the rewriter"
    echo "-v V: use logging verbosity V"
    echo "-h: prints help message"
    echo "phases:"
    echo -e "\t0:rewrite the persistent data"
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

runRewrite() {
  now=$(date +%b-%d-%H-%M-%S)
  mkdir -p $outDir

  label="rewrite"
  flagFile="${outDir}/${label}_flags_${now}.cfg"
  cp $confFile $flagFile
  ln -sf $flagFile "${outDir}/${label}_flags.cfg"

  logFile="${outDir}/${label}_logs_${now}.log"
  ln -sf  $logFile "${outDir}/${label}_logs.log"

  echo "$rewriteBin --flagfile=$flagFile --logtostderr=1 --v="$verbosity" > $logFile 2>&1"
  $rewriteBin --flagfile=$flagFile --logtostderr=1 --v="$verbosity" > $logFile 2>&1

  ## get the constants, and results out into files
  constantsFile="${outDir}/${label}_constants_${now}.cfg"
  cat $logFile | grep constants.cpp | cut -d "]" -f 2- | grep ":" > $constantsFile
  ln -sf $constantsFile "${outDir}/${label}_constants.cfg"
}

usage

runCommandIfInPhase 0 runRewrite "rewritest the persistent data"


