rootDir=/hdd1
baseDirName=dyna-mast
baseDirectory=$rootDir/$baseDirName
configFile=$baseDirectory/Adapt-HTAP/deployment/configs/kafka_server.properties
kafkaName=kafka_2.11-0.10.1.0
kafkaDir=$baseDirectory/$kafkaName

serverConfig=$kafkaDir/config/server.properties
zookeeperConfig=$kafkaDir/config/zookeeper.properties
zookeeperStart=$kafkaDir/bin/zookeeper-server-start.sh
zookeeperStop=$kafkaDir/bin/zookeeper-server-stop.sh
serverStart=$kafkaDir/bin/kafka-server-start.sh
serverStop=$kafkaDir/bin/kafka-server-stop.sh

kafkaOutDir="/hdd1"

help=false
dryRun=false
startZk=false
startPhase=0
endPhase=2

options=":hdzk:b:e:"

while getopts $options option
do
    case $option in
        h) help=true;;
        b) startPhase=$OPTARG;;
        e) endPhase=$OPTARG;;
        d) dryRun=true;;
        k) kafkaOutDir=$OPTARG;;
        z) startZk=true;;
        \? ) echo "Unknown option: -$OPTARG" >&2; exit 1;;
        :  ) echo "Missing option argument for -$OPTARG" >&2; exit 1;;
        *  ) echo "Unimplemented option: -$OPTARG" >&2; exit 1;;
    esac
done
shift $((OPTIND -1))

setupProperties() {
  cp $configFile $serverConfig
  rm -rf $kafkaOutDir/kafka-logs
  rm -rf /tmp/zookeeper/*
  mkdir -p /$kafkaOutDir/kafka-logs
}

startZookeeper() {
  $zookeeperStart $zookeeperConfig > /tmp/zk-log.log 2>&1 &
  sleep 2
}

startServer() {
  export KAFKA_HEAP_OPTS="-Xms6g -Xmx6g"
  export KAFKA_JVM_PERFORMANCE_OPTS="-XX:MetaspaceSize=96m -XX:+UseG1GC -XX:MaxGCPauseMillis=20
       -XX:InitiatingHeapOccupancyPercent=35 -XX:G1HeapRegionSize=16M
       -XX:MinMetaspaceFreeRatio=50 -XX:MaxMetaspaceFreeRatio=80"
  $serverStart $serverConfig > /tmp/kafka-log.log 2>&1 &
  sleep 2
}

stopServer() {
  $serverStop
}

stopZookeeper() {
  $zookeeperStop
}

forceStop() {
  ps aux | grep zookeeper | grep -v grep | awk '{print $2}' | xargs -I {} sh -c "sudo kill -9 {}";
}

stopAll() {
  stopServer
  stopZookeeper
  forceStop
}

copyIn() {
  setupProperties
}

startAll() {
  if [ $startZk = true ]; then
    startZookeeper
  fi
  startServer
}

usage() {
	if [ $help = true ]; then
    echo "$0 [-h -z -k K -d -b N -e M]"
    echo "Initializes kafka"
    echo "-z: Starts Zookeeper"
    echo "-k K: Creates kafka  in dir K"
    echo "-b N: begins in phase N"
    echo "-e M: end in phase M"
    echo "-d: only logs what commands will be run but does not run them"
    echo "-h: prints help message"
    echo "phases:"
    echo -e "\t0:stops kafka and zookeeper"
    echo -e "\t1:copies in kafka config"
    echo -e "\t2:starts kafka and zookeeper"

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

runCommandIfInPhase 0 stopAll "stop kafka and zookeeper"
runCommandIfInPhase 1 copyIn "copy in kafka config.."
runCommandIfInPhase 2 startAll "start kafka and zookeeper"
