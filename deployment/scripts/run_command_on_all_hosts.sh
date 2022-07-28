rootDir=/hdd1
baseDirName=dyna-mast
baseDirectory="$rootDir/$baseDirName"

help=false
dryRun=false
hostsFile="$baseDirectory/Adapt-HTAP/deployment/configs/all_hosts.cfg"

logDir=/tmp/multi-command-log

startPhase=0
endPhase=0

options=":hdc:l:b:e:"
while getopts $options option
do
    case $option in
        h) help=true;;
        c) hostsFile=$OPTARG;;
        l) logDir=$OPTARG;;
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

execCommand="$1"

runCommandAtAllSites() {
  mkdir -p $logDir
  command="$execCommand"
  while read host; do
    logFile=$logDir/$host
    echo "running: ${execCommand}, on ${host}"
    ssh -n "$host" "$execCommand" > $logFile 2>&1 &
  done < $hostsFile
  wait
}

usage() {
	if [ $help = true ]; then
    echo "$0 [-h -d -b N -e M -c C -l L] command"
    echo "Runs command on hosts"
    echo "-c C: runs on hosts listed in file C"
    echo "-l L: stores output to directory L"
    echo "-b N: begins in phase N"
    echo "-e M: end in phase M"
    echo "-d: only logs what commands will be run but does not run them"
    echo "-h: prints help message"
    echo "phases:"
    echo -e "\t0:runs commands"
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

runCommandIfInPhase 0 runCommandAtAllSites "running command"

