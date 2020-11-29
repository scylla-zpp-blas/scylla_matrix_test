#!/bin/bash

# =================================================================================
# Boilerplate for parsing options, mostly from https://stackoverflow.com/a/29754866
# =================================================================================

# -allow a command to fail with !’s side effect on errexit
# -use return value from ${PIPESTATUS[0]}, because ! hosed $?
! getopt --test > /dev/null
if [[ ${PIPESTATUS[0]} -ne 4 ]]; then
    echo 'I’m sorry, `getopt --test` failed in this environment.'
    exit 1
fi

# ===================================================
# List of possible options (both short and long form)
# ===================================================
OPTIONS=hn:
LONGOPTS=help,number:,data:

# -regarding ! and PIPESTATUS see above
# -temporarily store output to be able to check for errors
# -activate quoting/enhanced mode (e.g. by writing out “--options”)
# -pass arguments only via   -- "$@"   to separate them correctly
! PARSED=$(getopt --options=$OPTIONS --longoptions=$LONGOPTS --name "$0" -- "$@")
if [[ ${PIPESTATUS[0]} -ne 0 ]]; then
    # e.g. return value is 1
    #  then getopt has complained about wrong arguments to stdout
    exit 2
fi
# read getopt’s output this way to handle the quoting right:
eval set -- "$PARSED"

# ===================================================
# End of boilerplate - our script begins
# ===================================================

function help {
    echo -e "\nUsage: $0 [OPTIONS] -- [Additional args to scylla]"
    echo -e "\nRun scylla cluster in docker containers"
    echo -e "\nOptions:"
    echo -e "  -d, --data        Location of folder with data of cluster"
    echo -e "  -n, --number      Amount of instances to run (default: 3)"
    exit 0
}



instances=2
data_folder=""


while true; do
    case "$1" in
        -h|--help)
            help
            shift
            ;;
        -n|--number)
            instances="$2"
            shift 2
            ;;
        -d|--data)
            data_folder="$2"
            shift 2
            ;;
        --)
            shift
            break
            ;;
        *)
            echo "Unexpected error (option $1)"
            exit 3
            ;;
    esac
done

function cleanup_silent {
    echo -e "\tCleanup: Stopping instances"
    for i in $(seq 1 $instances); do
      docker stop "scylla_zpp_$i" &> /dev/null || true
    done

    echo -e "\tCleanup: removing network"
    docker network rm scylla_zpp &> /dev/null || true
}


function cleanup {
    echo -e "\tCleanup: Stopping instances"
    for i in $(seq 1 $instances); do
      local name="scylla_zpp_$i"
      echo -e "\t Stopping $name"
      docker stop "scylla_zpp_$i" > /dev/null
    done

    echo -e "\tCleanup: removing network"
    docker network rm scylla_zpp > /dev/null
}

function get_first_ip {
  docker inspect --format='{{ .NetworkSettings.Networks.scylla_zpp.IPAddress }}' scylla_zpp_1 2> /dev/null || true
}

echo "Cleaning up in case anything was left from previous run"
cleanup_silent

# Registering cleanup procedure
trap cleanup SIGINT

echo "Creating docker network"
docker network create --driver bridge scylla_zpp

echo "Starting scylla instances"
for i in $(seq 1 $instances); do
    name="scylla_zpp_$i"
    seeds="$(get_first_ip)"
    mount_arg="$([ -n "$data_folder" ] && echo "-v \"$data_folder/node-$i:/var/lib/scylla\"")"
    echo "Starting $name with seeds=$seeds"

    docker run \
        --rm \
        --name "$name" \
        --network scylla_zpp \
        -d scylladb/scylla \
        --seeds="$seeds" \
        ${mount_arg:+"$mount_arg"}
done

echo "Scylla cluster started!"
echo "IP of first instance: $(get_first_ip)"
echo "Press Ctrl+C to stop cluster"

# Waiting for Ctrl + C before continuing
( trap exit SIGINT ; read -r -d '' _ </dev/tty )
