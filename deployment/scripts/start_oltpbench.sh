rootDir=/hdd1
baseDirName=dyna-mast
baseDirectory="$rootDir/$baseDirName"

help=false

outDir="$rootDir/dyna-mast-out"
confFile="$baseDirectory/Adapt-HTAP/deployment/configs/benchmarkConfig.xml"
oltpDir="$baseDirectory/oltpbench"
oltpBin="$oltpDir/oltpbenchmark"
benchName="adaptycsb"
sampleSize=5

startPhase=0
endPhase=1
dryRun=false
background=false
options=":hdzb:e:o:c:s:n:"

while getopts $options option
do
    case $option in
        h) help=true;;
        b) startPhase=$OPTARG;;
        e) endPhase=$OPTARG;;
        d) dryRun=true;;
        o) outDir=$OPTARG;;
        c) confFile=$OPTARG;;
        s) oltpBin=$OPTARG;;
        n) benchName=$OPTARG;;
        z) background=true;;
        \? ) echo "Unknown option: -$OPTARG" >&2; exit 1;;
        :  ) echo "Missing option argument for -$OPTARG" >&2; exit 1;;
        *  ) echo "Unimplemented option: -$OPTARG" >&2; exit 1;;
    esac
done
shift $((OPTIND -1))

usage() {
	if [ $help = true ]; then
    echo "$0 [-h -d -b N -e M -o O -c C -s S -n N]"
    echo "Runs the oltpbenchmark"
    echo "-b N: begins in phase N"
    echo "-e M: end in phase M"
    echo "-d: only logs what commands will be run but does not run them"
    echo "-o O: output to directory O"
    echo "-c C: uses file C as a config file"
    echo "-s S: uses binary S as the benchmark"
    echo "-n N: uses benchmark N"
    echo "-h: prints help message"
    echo "-z: background start"
    echo "phases:"
    echo -e "\t0:creates and loads the data"
    echo -e "\t1:runs the benchmark"
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

executeOltpBench() {
  op="$1"
  now=$(date +%b-%d-%H-%M-%S)
  mkdir -p $outDir

  label="oltpbench-${benchName}-${op}"

  xmlFile="${outDir}/${label}_conf_${now}.xml"
  cp $confFile $xmlFile
  ln -sf $xmlFile "${outDir}/${label}_conf.xml"

  logFile="${outDir}/${label}_logs_${now}.log"
  ln -sf  $logFile "${outDir}/${label}_logs.log"

  outputFile="${outDir}/${label}_results_${now}"
  ln -sf "$outputFile.res" "${outDir}/${label}_results.res"

  curDir=$( pwd )

  cd $oltpDir
  if [ $background = true ]; then
      echo "$oltpBin "--${op}=true" --config "$xmlFile" --bench $benchName --output "$outputFile" --sample $sampleSize > $logFile 2>&1 &"
      $oltpBin "--${op}=true" --config "$xmlFile" --bench $benchName --output "$outputFile" --sample $sampleSize > $logFile 2>&1 &
  else
      echo "$oltpBin "--${op}=true" --config "$xmlFile" --bench $benchName --output "$outputFile" --sample $sampleSize > $logFile 2>&1"
      $oltpBin "--${op}=true" --config "$xmlFile" --bench $benchName --output "$outputFile" --sample $sampleSize > $logFile 2>&1
  fi
  cd $curDir
}

createAndLoad() {
  executeOltpBench "create"
  executeOltpBench "load"
}

runBenchmark() {
  executeOltpBench "execute"
}

usage

runCommandIfInPhase 0 createAndLoad "Creates and loads the data"
runCommandIfInPhase 1 runBenchmark "runs the benchmark"


