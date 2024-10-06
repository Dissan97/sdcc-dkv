package utils

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"net/http"
)

// PerformPut sends a PUT request to the specified server with key and value.
func PerformPut(server string, key string, value string) error {
	url := fmt.Sprintf("http://%s/api/datastore", server)

	// Prepare the data to send as JSON
	data := fmt.Sprintf(`{"key":"%s", "value":"%s"}`, key, value)
	req, err := http.NewRequest("PUT", url, bytes.NewBuffer([]byte(data)))
	if err != nil {
		log.Printf("Error creating PUT request: %v", err)
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("Error performing PUT request: %v", err)
		return err
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			log.Printf("Error closing body: %v", err)
		}
	}(resp.Body)

	// Read the response using io.ReadAll
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Printf("Error reading PUT response: %v", err)
		return err
	}

	fmt.Println("Response from server:", string(body))
	return nil
}

// PerformGet sends a GET request to retrieve the value for the specified key.
func PerformGet(server string, key string) error {
	url := fmt.Sprintf("http://%s/api/datastore?key=%s", server, key)

	resp, err := http.Get(url)
	if err != nil {
		log.Printf("Error performing GET request: %v", err)
		return err
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			log.Printf("Error closing body: %v", err)
		}
	}(resp.Body)

	// Read the response using io.ReadAll
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Printf("Error reading GET response: %v", err)
		return err
	}

	fmt.Println("Response from server:", string(body))
	return nil
}

// PerformDelete sends a DELETE request to remove the specified key.
func PerformDelete(server string, key string) error {
	url := fmt.Sprintf("http://%s/api/datastore?key=%s", server, key)

	req, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		log.Printf("Error creating DELETE request: %v", err)
		return err
	}

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("Error performing DELETE request: %v", err)
		return err
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			log.Printf("Error closing body: %v", err)
		}
	}(resp.Body)

	// Read the response using io.ReadAll
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Printf("Error reading DELETE response: %v", err)
		return err
	}

	fmt.Println("Response from server:", string(body))
	return nil
}
