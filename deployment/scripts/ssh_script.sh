rootDir=/hdd1
baseDirName=dyna-mast
baseDirectory="$rootDir/$baseDirName"

help=false
dryRun=false
hostsFile="$baseDirectory/Adapt-HTAP/deployment/configs/all_hosts.cfg"

startPhase=0
endPhase=0

doKeyGen=false

options=":hdkc:b:e:"
while getopts $options option
do
    case $option in
        h) help=true;;
        k) doKeyGen=true;;
        c) hostsFile=$OPTARG;;
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

sshToMachine() {
  command="$execCommand"
  while read host; do
    echo "ssh to $host"
	  if [ $doKeyGen = true ]; then
      ssh-keygen -f "/home/${USER}/.ssh/known_hosts" -R "$host"
    fi
    ssh -n "$host" "exit"
  done < $hostsFile
  wait
}

usage() {
	if [ $help = true ]; then
    echo "$0 [-h -d -b N -e M -c C -k]"
    echo "Sets up ssh on hosts"
    echo "-c C: Uses configuration file C as the hostsname"
    echo "-k: does keygen"
    echo "-b N: begins in phase N"
    echo "-e M: end in phase M"
    echo "-d: only logs what commands will be run but does not run them"
    echo "-h: prints help message"
    echo "phases:"
    echo -e "\t0:sshs to each machine"
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

runCommandIfInPhase 0 sshToMachine "runs command to get ssh"

