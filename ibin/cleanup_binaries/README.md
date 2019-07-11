## Prerequisites

### Install Ammonite (to run Scala scripts)

Install `ammonite` to run Scala scripts:
```
sudo sh -c '(echo "#!/usr/bin/env sh" && curl -L https://github.com/lihaoyi/Ammonite/releases/download/1.6.9/2.13-1.6.9) > /usr/local/bin/amm && chmod +x /usr/local/bin/amm' && amm
```

## Run

1. Log in into CF: `cf login -a $CF_URL`
2. Login into jumpbox and find an IP of one of Zookeeper machines for the selected cluster
3. Run `./main.sh -j $JUMPBOX_NAME -z $ZOOKEEPER_VM_IP`


## Debugging/Testing

1. `postgresql.sh` - checks `cf env spark-service` and extracts all the meta information
needed to open a tunnel and connect to PostgreSQL. Opens a tunnel in background,
connects to DB and queries `libs` table to find all known binaries.

2. `zookeeper.sc` - Scala script (should be run with a help of Ammonite), which
uses Curator library (more or less code from Jobserver ZookeeperUtils.scala) to
list directory or delete selected files in Zookeeper.

3. `tunnel-to-zookeeper.sh` - script to open a tunnel to Zookeeper machine
though the jumpbox (e.g. makes tunnel to localhost:9999 and after that
zkCli or Scala with Curator library can be used)

4. `main.sh` - puts together scripts above