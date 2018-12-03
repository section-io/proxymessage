package proxymessage

import (
	"crypto/sha1"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"time"

	redis "gopkg.in/redis.v5"
)

const defaultRegistrationTimeoutSeconds = 60
const defaultListKeyPrefix = "pod-"
const defaultListKeySuffix = "list"
const defaultBrpopTimeout = 5 * time.Minute

var debugLog = false

// Client is the message client for proxy pod handlers
type Client struct {
	redisClient                *redis.Client
	registrationKey            string
	registrationTimeoutSeconds int
	listKey                    string

	lastRegistrationSuccess time.Time
	// Successful messages come in via this InboundMessageChannel
	InboundMessageChannel chan string
	// InfoEventCallback is called with informational string messages related to the redis connection
	InfoEventCallback InfoEventCallback
}

// This function is called with informational strings
type InfoEventCallback func(event string)

func sha1FromString(s string) string {
	h := sha1.New()
	h.Write([]byte(s))
	bs := h.Sum(nil)
	return fmt.Sprintf("%x", bs)
}

// NewClient creates a new proxy message client
func NewClient(redisAddress, registrationKey, listKeyPrefix, listKeySuffix string, registrationTimeoutSeconds int) *Client {

	debugLog = os.Getenv("DEBUG") != ""

	if registrationTimeoutSeconds == 0 {
		registrationTimeoutSeconds = defaultRegistrationTimeoutSeconds
	}
	if listKeyPrefix == "" {
		listKeyPrefix = defaultListKeyPrefix
	}
	if listKeySuffix == "" {
		listKeySuffix = defaultListKeySuffix
	}
	listKey := listKeyPrefix + "-" + listKeySuffix

	defaultCallback := func(a string) {}

	pmc := &Client{
		redisClient: redis.NewClient(&redis.Options{
			Addr: redisAddress,
		}),
		registrationKey:            registrationKey,
		registrationTimeoutSeconds: registrationTimeoutSeconds,
		listKey:                    listKey,
		InboundMessageChannel:      make(chan string),
		InfoEventCallback:          defaultCallback,
	}

	go pmc.registerLoop()

	go pmc.receiveLoop()

	return pmc
}

// NewClientFromEnvVars creates a new proxy message client from environment variables
func NewClientFromEnvVars() *Client {
	redisHost := os.Getenv("REDIS_HOST")
	if redisHost == "" {
		log.Panicln("'REDIS_HOST' environment variable is required")
	}

	redisPort := os.Getenv("REDIS_PORT")
	if redisPort == "" {
		redisPort = "6379"
	}

	redisAddress := redisHost + ":" + redisPort

	proxyRegoKey := os.Getenv("PROXY_REGO_KEY")
	if proxyRegoKey == "" {
		log.Panic("PROXY_REGO_KEY environment variable is required.")
	}

	listKeyPrefix := os.Getenv("LIST_KEY_PREFIX")
	if listKeyPrefix == "" {
		log.Panic("LIST_KEY_PREFIX environment variable is required.")
	}

	listKeySuffix := os.Getenv("LIST_KEY_SUFFIX")
	if listKeySuffix == "" {
		log.Panic("LIST_KEY_SUFFIX environment variable is required.")
	}
	listKeySuffix = sha1FromString(listKeySuffix)

	var registrationTimeoutSeconds int
	var err error
	timeoutString := os.Getenv("MESSAGE_CLIENT_REGISTRATION_TIMEOUT_SECONDS")
	if timeoutString != "" {
		registrationTimeoutSeconds, err = strconv.Atoi(timeoutString)
		if err != nil {
			log.Panic("If MESSAGE_CLIENT_REGISTRATION_TIMEOUT_SECONDS is set it must be a valid integer.")
		}
	}

	return NewClient(redisAddress, proxyRegoKey, listKeyPrefix, listKeySuffix, registrationTimeoutSeconds)
}

func (pmc *Client) registerLoop() {
	pmc.registerListKey()

	tickChannel := time.Tick(time.Duration(pmc.registrationTimeoutSeconds * int(time.Second)))
	for tickTime := range tickChannel {
		res := pmc.registerListKey()
		if debugLog {
			log.Println("Register call complete: ", tickTime, res)
		}
	}
}

func (pmc *Client) registerListKey() bool {
	//Returns count of "added" should only be "1" on first add (not updates)
	_, err := pmc.redisClient.ZAdd(pmc.registrationKey, *&redis.Z{Score: float64(time.Now().UTC().Unix()), Member: pmc.listKey}).Result()

	if err != nil {
		log.Println("Registration failure:", err)
		return false
	}
	pmc.lastRegistrationSuccess = time.Now()
	//fmt.Printf("Rego Ok! %s %d\n", pmc.lastRegistrationSuccess, added)
	return true //added != 0
}

func (pmc *Client) receiveLoop() {

	brpopTimeout := defaultBrpopTimeout

	//Allow the timeout to be overriden via environment variable
	if os.Getenv("REDIS_BRPOP_TIMEOUT_SECONDS") != "" {
		timeoutSeconds, err := strconv.Atoi(os.Getenv("REDIS_BRPOP_TIMEOUT_SECONDS"))
		if err != nil {
			log.Printf("REDIS_BRPOP_TIMEOUT_SECONDS value '%s' is not a valid int, setting timeout to default %d: %#v\n", os.Getenv("REDIS_BRPOP_TIMEOUT_SECONDS"), defaultBrpopTimeout, err)
		} else {
			log.Printf("Overriding redis BRPOP timeout to REDIS_BRPOP_TIMEOUT_SECONDS value of %d seconds", timeoutSeconds)
			brpopTimeout = time.Duration(timeoutSeconds) * time.Second
		}
	}

	redisErrorCount := 0
	for {
		rawMessage, brpopErr := pmc.redisClient.BRPop(brpopTimeout, pmc.listKey).Result()

		if brpopErr != nil {
			if brpopErr == redis.Nil {
				// Timeout was reached at the server after brpopTimeout, this is not an error state
				// it just means that no messages were received during the timeout window and the
				// brpop will be restarted.
				if debugLog {
					log.Println("nil response from redis BRPOP, timeout expired")
				}
				redisErrorCount = 0
				continue
			}

			opError, ok := brpopErr.(*net.OpError)
			if ok {
				// Timeout was caused by the redis server not sending any message at all
				// This could be indicative of a connection error but is recoverable
				if opError.Op == "read" && opError.Timeout() && opError.Temporary() {
					msg := fmt.Sprintf("Client-side timeout from redis, redis did not respond: %v", opError)
					log.Print(msg)
					if pmc.InfoEventCallback != nil {
						pmc.InfoEventCallback(msg)
					}
					continue
				}
			}

			log.Println("BRPop error:", brpopErr)
			// avoid CPU spin when Redis errors consecutively
			redisErrorCount++

			// TODO beacon error for alerting if redisErrorCount exceeds some threshold
			if redisErrorCount > 3 {
				var sleepSeconds int
				if redisErrorCount > 30 {
					sleepSeconds = 30
				} else {
					sleepSeconds = redisErrorCount
				}

				log.Printf("sleeping for %d seconds after %d consecutive Redis errors\n", sleepSeconds, redisErrorCount)
				time.Sleep(time.Duration(sleepSeconds) * time.Second)
			}
			continue
		}
		redisErrorCount = 0

		if len(rawMessage) != 2 {
			log.Println("Unexpected BRPop result length:", len(rawMessage))
			continue
		}
		if rawMessage[0] != pmc.listKey {
			log.Println("Unexpected BRPop result key:", rawMessage[0])
			continue
		}

		pmc.InboundMessageChannel <- rawMessage[1]
	}
}

// GetListKey returns the list key the client is listening on
func (pmc *Client) GetListKey() string {
	return pmc.listKey
}
