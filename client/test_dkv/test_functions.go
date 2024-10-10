package test_dkv

import (
	"client/utils"
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"gopkg.in/yaml.v3"
	"hash"
	"log"
	"math"
	"os"
	"strings"
	"sync"
	"time"
)

type DockerCompose struct {
	Version  string             `yaml:"version"`
	Services map[string]Service `yaml:"services"`
	Networks map[string]Network `yaml:"networks"`
}

type Service struct {
	Image         string   `yaml:"image"`
	Hostname      string   `yaml:"hostname"`
	ContainerName string   `yaml:"container_name"`
	Command       string   `yaml:"command"`
	Ports         []string `yaml:"ports"`
	Networks      []string `yaml:"networks"`
}

type Network struct {
	Driver string `yaml:"driver"`
}

type TestEnv struct {
	Servers []string
	Keys    []string
	Hash    func() hash.Hash
	Threads int
}

func (te *TestEnv) Init(servers []string, filename string, threads int) {
	te.Servers = servers
	data, err := os.ReadFile(filename)
	if err != nil {
		log.Fatalf("error reading file: %s %v", filename, err)
	}
	var compose DockerCompose
	err = yaml.Unmarshal(data, &compose)
	if err != nil {
		log.Fatalf("error unmarshalling YAML: %v", err)
	}
	te.Keys = make([]string, 0)

	// Accessing and printing the parsed data
	fmt.Printf("Version: %s\n", compose.Version)
	for serviceName, service := range compose.Services {

		var hf func() hash.Hash
		if strings.Contains(service.Command, "--hash=md5") {
			hf = md5.New
		} else if strings.Contains(service.Command, "--hash=sha1") {
			hf = sha1.New
		} else if strings.Contains(service.Command, "--hash=sha256") {
			hf = sha256.New
		} else {
			log.Fatal("didn't found hash")
		}
		te.Hash = hf
		te.Keys = append(te.Keys, hex.EncodeToString(te.Hash().Sum([]byte(serviceName))))

	}
	for networkName, network := range compose.Networks {
		fmt.Printf("Network: %s, Driver: %s\n", networkName, network.Driver)
	}
	te.Threads = threads

}

func (te *TestEnv) TestFunctions() {

	meanPut := make(map[string]float64)
	meanGet := make(map[string]float64)
	meanDel := make(map[string]float64)
	meanPutStr := make(map[string]string)
	meanGetStr := make(map[string]string)
	meanDelStr := make(map[string]string)

	iteration := 10
	count := float64(0)
	// testing put meantime
	for _, server := range te.Servers {
		meanPut[server] = 0
		count = 0
		log.Printf("Testing server %s PUT\n", server)
		for i := 0; i < iteration; i++ {

			for _, key := range te.Keys {

				start := float64(time.Now().UnixNano())
				_, err := utils.PerformPut(server, key, fmt.Sprintf("%s_%d", server, i))
				if err != nil {
					log.Printf("error putting key %s in server %s: %v", key, server, err)
					continue
				}
				end := float64(time.Now().UnixNano())
				count++
				meanPut[server] += (end - start) / math.Pow(10, 9)
			}

		}
		meanPut[server] = meanPut[server] / count
	}
	for k, v := range meanPut {
		meanPutStr[k] = fmt.Sprintf("%.3f s", v)
	}
	log.Printf("Testing servers PUT: %v\n", meanPutStr)
	// testing get meantime
	for _, server := range te.Servers {
		meanGet[server] = 0
		count = 0
		log.Printf("Testing server %s GET\n", server)
		for i := 0; i < iteration; i++ {

			for _, key := range te.Keys {

				start := float64(time.Now().UnixNano())
				_, err := utils.PerformGet(server, key)
				if err != nil {
					log.Printf("error getting key %s in server %s: %v", key, server, err)
					continue
				}
				end := float64(time.Now().UnixNano())
				count++
				meanGet[server] += (end - start) / math.Pow(10, 9)
			}

		}
		meanGet[server] = meanGet[server] / count
	}
	for k, v := range meanGet {
		meanGetStr[k] = fmt.Sprintf("%.3f s", v)
	}

	log.Printf("Testing servers GET: %v\n", meanGetStr)

	// testing del meantime

	for _, server := range te.Servers {
		meanDel[server] = 0
		count = 0
		log.Printf("Testing server %s DEL\n", server)
		for i := 0; i < iteration; i++ {

			for _, key := range te.Keys {

				start := float64(time.Now().UnixNano())
				_, err := utils.PerformDelete(server, key)
				if err != nil {
					log.Printf("error deleting key %s in server %s: %v", key, server, err)
					continue
				}
				end := float64(time.Now().UnixNano())
				count++
				meanDel[server] += (end - start) / math.Pow(10, 9)
				if i < iteration-1 {
					_, err = utils.PerformPut(server, key, fmt.Sprintf("%s_%d", server, i))
					if err != nil {
						log.Printf("error putting key %s in server %s: %v", key, server, err)
					}
				}
			}
		}
		meanDel[server] = meanDel[server] / count
	}
	for k, v := range meanDel {
		meanDelStr[k] = fmt.Sprintf("%.3f s", v)
	}
	log.Printf("Testing servers DEL: %v\n", meanDelStr)

}

func (te *TestEnv) StressTestPut() {
	wg := sync.WaitGroup{}
	wg.Add(te.Threads)
	times := make([]float64, te.Threads)
	mean := float64(0)
	for i := 0; i < te.Threads; i++ {
		go func(id int, wg *sync.WaitGroup) {
			defer wg.Done()
			times[id] = 0
			start := float64(time.Now().UnixNano())

			for _, key := range te.Keys {
				_, err := utils.PerformPut(te.Servers[id%len(te.Servers)], key, fmt.Sprintf("theread_%d", id))
				if err != nil {
					log.Printf("error putting key %s in server %s: %v", key,
						te.Servers[id%len(te.Servers)], err)
				}
			}

			end := float64(time.Now().UnixNano())
			times[id] += (end - start) / math.Pow(10, 9)
		}(i, &wg)
	}

	wg.Wait()
	for _, val := range times {
		mean += val
	}
	mean /= float64(te.Threads)
	log.Printf("Testing stress test threads %d took %.3f seconds\n", te.Threads, mean)
}
