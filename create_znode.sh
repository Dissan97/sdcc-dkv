#!/bin/sh

# Wait for Zookeeper to start
sleep 5

# Create the /election znode
echo "create /election ''" | zkCli.sh -server localhost:2181
