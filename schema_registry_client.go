package avro

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"time"
)

//SchemaRegistryClient is not concurrent

type SchemaRegistryClient struct {
	Url      string
	CertFile string
	KeyFile  string
	KeyPass  string
	CAFile   string
	cache1   map[uint32]Schema
	cache2   map[string]map[Fingerprint]uint32
}

type schemaResponse struct {
	Schema string
}

func (c *SchemaRegistryClient) Get(schemaId uint32) (Schema, error) {
	if c.cache1 == nil {
		c.cache1 = make(map[uint32]Schema)
	}
	result := c.cache1[schemaId]
	if result == nil {
		httpClient, err := c.getHttpClient()
		if err != nil {
			return nil, err
		}

		var url = c.Url + "/schemas/ids/" + strconv.Itoa(int(schemaId))
		ctx, _ := context.WithTimeout(context.Background(), 15*time.Second)
		req, _ := http.NewRequest("GET", url, nil)
		resp, err := httpClient.Do(req.WithContext(ctx))
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()
		if resp.StatusCode != 200 {
			return nil, fmt.Errorf("unexpected response from the schema registry: %v", resp.StatusCode)
		}
		if body, err := ioutil.ReadAll(resp.Body); err != nil {
			return nil, err
		} else {
			response := new(schemaResponse)
			json.Unmarshal(body, response)
			if result, err = ParseSchema(response.Schema); err != nil {
				return nil, err
			} else {
				c.cache1[schemaId] = result
			}
		}

	}
	return result, nil
}
func (c *SchemaRegistryClient) GetSchemaId(schema Schema, subject string) (uint32, error) {
	if c.cache2 == nil {
		c.cache2 = make(map[string]map[Fingerprint]uint32)
	}
	var s map[Fingerprint]uint32
	if s = c.cache2[subject]; s == nil {
		s = make(map[Fingerprint]uint32)
		c.cache2[subject] = s
	}

	if f, err := schema.Fingerprint(); err != nil {
		return 0, err
	} else {
		result, ok := s[*f]
		if !ok {
			request := make(map[string]string)
			request["schema"] = schema.String()
			if schemaJson, err := json.Marshal(request); err != nil {
				return 0, err
			} else {
				log.Printf("Registering schema for subject %q schema: %v", subject, schema.GetName())
				var url = c.Url + "/subjects/" + subject + "/versions"
				j := make(map[string]uint32)
				if resp, err := http.Post(url, "application/json", bytes.NewReader(schemaJson)); err != nil {
					return 0, err
				} else if resp.StatusCode != 200 {
					return 0, fmt.Errorf(resp.Status)
				} else if data, err := ioutil.ReadAll(resp.Body); err != nil {
					return 0, err
				} else if err := json.Unmarshal(data, &j); err != nil {
					return 0, err
				} else {
					result = j["id"]
					log.Printf("Got Schema ID: %v", result)
					s[*f] = result
				}
			}
		}
		return result, nil
	}

}

func (c *SchemaRegistryClient) getHttpClient() (*http.Client, error) {
	transport := new(http.Transport)
	if c.CertFile != "" {
		if c.KeyFile == "" {
			return nil, fmt.Errorf("KeyFile not configured")
		}

		transport.TLSClientConfig = new(tls.Config)

		var cert tls.Certificate

		if certBlock, err := ioutil.ReadFile(c.CertFile); err != nil {
			return nil, err
		} else if pemData, err := ioutil.ReadFile(c.KeyFile); err != nil {
			return nil, err
		} else if v, _ := pem.Decode(pemData); v == nil {
			return nil, fmt.Errorf("no RAS key found in file: %v", c.KeyFile)
		} else if v.Type == "RSA PRIVATE KEY" {
			var pkey []byte
			if x509.IsEncryptedPEMBlock(v) {
				println("encrypted")
				pkey, _ = x509.DecryptPEMBlock(v, []byte(c.KeyPass))
				pkey = pem.EncodeToMemory(&pem.Block{
					Type:  v.Type,
					Bytes: pkey,
				})
			} else {
				println("unencrypted")
				pkey = pem.EncodeToMemory(v)
			}
			println("got private key")
			if cert, err = tls.X509KeyPair(certBlock, pkey); err != nil {
				return nil, err
			}
		} else {
			return nil, fmt.Errorf("only 'RSA PRIVATE KEY` supported, got: %q", v.Type)
		}
		//
		//if keyBlock, err := x509.DecryptPEMBlock(block, []byte(c.KeyPass)); err != nil {
		//	return nil, err
		//} else if cert, err = tls.X509KeyPair(certBlock, keyBlock); err != nil {
		//	return nil, err
		//} else {
		//
		//}

		transport.TLSClientConfig.Certificates = []tls.Certificate{cert}

		if c.CAFile != "" {
			caCert, err := ioutil.ReadFile(c.CAFile)
			if err != nil {
				log.Fatal(err)
			}
			caCertPool := x509.NewCertPool()
			caCertPool.AppendCertsFromPEM(caCert)
			transport.TLSClientConfig.RootCAs = caCertPool
		}

		transport.TLSClientConfig.BuildNameToCertificate()
	}

	return &http.Client{Transport: transport}, nil
}
