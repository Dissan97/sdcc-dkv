# SDCC SIMPLE KEY VALUE DATASTORE
## AUTOR: Dissan Uddin Ahmed
## ACCADEMIC YEAR: 2023/2024

Simple distributed key-value store with consistency
guarantees
- Programming language: Go
- Implement a simple distributed key-value store with
  consistency guarantees
  • Operations: get(key), put(key, value), delete(key)
  • Clients can request operations to any replica
  • Provide consistency guarantees
  • Sequential consistency using logical scalar clocks
  • Causal consistency using logical vector clocks
- Test solution (at least 3 replicas)
- Deployment using Docker Compose and EC2 instance
- emulation of network delays to simulate delays
  in the message transmission among different replicas

## code directories and file
[dkv](dkv)
- [main.go](dkv/main.go): contain main of server-container
- [data](dkv/data): contain dataStore logic
- [data_http](dkv/data_http): contain how server http is implemented
- [causal_order](dkv/dkv_order/causal_order) how multicast causal is implemented
- [totally_order](dkv/dkv_order/totally_order) how multicast sequential is implemented
- [replica](dkv/replica) how node logic is implemented
- [overlay](dkv/overlay) how the network is organized
## Docker file info

[docker-compose.yaml](docker-compose.yaml)

- services: name of service is the server name
- ports "host:container" container export port 8080 for http connections
- command to launch a server are described in [main.go](dkv/main.go)
  

## Run Server

Docker engine is needed on server machine

### Build docker files
[Dockerfile](Dockerfile)
```
  docker build . -t dkv 
```

### 3 server sequential 
[docker-compose.yaml](docker-compose.yaml)
```
  docker-compose up -d 
```

### 3 server causal
[docker-compose_causal.yaml](docker-compose_causal.yaml)
```
  docker-compose -f docker-compose_causal.yaml up -d 
```

### 5 server sequential
[docker-compose_5.yaml](docker-compose_5.yaml)
```
  docker-compose -f docker-compose_5.yaml up -d 
```

### 5 server causal
[docker-compose_causal_5.yaml](docker-compose_causal_5.yaml)
```
  docker-compose -f docker-compose_causal_5.yaml up -d 
```

## Run Client
go to
[client-directory](client)

```
  source launch.sh "server1:http:port1;server2:port2..." 
```

## Results on latency and throughput

Tested on Amazon region eu-shout-1
instance Ec2 {t4g.large} (arm) 2-core 8gb ram

[test_functions.go](client/test_dkv/test_functions.go)

- [latencies](client/resources/results/test_latencies)
- [throughput](client/resources/results/test_throughput)