rootDir=/hdd1
baseDirName=dyna-mast
baseDirectory=$rootDir/$baseDirName
kafkaName=kafka_2.11-0.10.1.0
kafkaDir=$baseDirectory/$kafkaName

help=false

kafkaHost="localhost"
replication=1
partitions=1
topic="dyna-mast-updates"

options=":hk:r:p:t:"

while getopts $options option
do
    case $option in
        h) help=true;;
        k) kafkaHost=$OPTARG;;
        r) replication=$OPTARG;;
        p) partitions=$OPTARG;;
        t) topic=$OPTARG;;
        \? ) echo "Unknown option: -$OPTARG" >&2; exit 1;;
        :  ) echo "Missing option argument for -$OPTARG" >&2; exit 1;;
        *  ) echo "Unimplemented option: -$OPTARG" >&2; exit 1;;
    esac
done
shift $((OPTIND -1))

create_topics() {
  echo "$kafkaDir/bin/kafka-topics.sh --delete --zookeeper $kafkaHost:2181 --topic ${topic}"
  $kafkaDir/bin/kafka-topics.sh --delete --zookeeper $kafkaHost:2181 --topic ${topic}
  echo "$kafkaDir/bin/kafka-topics.sh --create --zookeeper $kafkaHost:2181 --replication-factor ${replication} --partitions ${partitions} --topic ${topic}"
  $kafkaDir/bin/kafka-topics.sh --create --zookeeper $kafkaHost:2181 --replication-factor ${replication} --partitions ${partitions} --topic ${topic}
}

usage() {
	if [ $help = true ]; then
    echo "$0 [-h -k K -r R -p P -t T]"
    echo "Creates topics on kafka"
    echo "-k K: Creates topic on host K"
    echo "-r R: uses replication factor R"
    echo "-p P: uses P partitions"
    echo "-t T: uses topic T"
    echo "-h: prints help message"
    exit 0
	fi
}


usage

create_topics

