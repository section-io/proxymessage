package proxymessage

import (
	"expvar"
	"log"
	"strconv"
	"time"

	redis "gopkg.in/redis.v5"
)

// Environment is the proxy message environment maintainer
type Environment struct {
	redisClient                *redis.Client
	envDestKey                 string
	envStackKey                string
	proxyBaseKey               string
	registrationTimeoutSeconds int

	InboundMessageChannel chan string
}

var (
	evEmptyOldProxyRegistration     = expvar.NewInt("evEmptyOldProxyRegistration")
	evDeletedProxyRegistrations     = expvar.NewInt("evDeletedProxyRegistrations")
	evDeletedProxyRegistrationFails = expvar.NewInt("evDeletedProxyRegistrationFails")
)

// NewEnvironment creates a new proxy message environment client
func NewEnvironment(redisAddress, envDestKey, envStackKey, proxyBaseKey string, registrationTimeoutSeconds int) *Environment {
	if registrationTimeoutSeconds == 0 {
		registrationTimeoutSeconds = defaultRegistrationTimeoutSeconds
	}

	pmc := &Environment{
		redisClient: redis.NewClient(&redis.Options{
			Addr: redisAddress,
		}),
		envDestKey:   envDestKey,
		envStackKey:  envStackKey,
		proxyBaseKey: proxyBaseKey,

		registrationTimeoutSeconds: registrationTimeoutSeconds,

		InboundMessageChannel: make(chan string),
	}

	if err := pmc.redisClient.Ping().Err(); err != nil {
		log.Panicf("Cannot ping source redis: %#v\n", err)
	}

	go pmc.cleanupLoop()
	go pmc.receiveLoop()

	return pmc
}

//Shutdown shuts down the environment
func (pmc *Environment) Shutdown() bool {
	//TODO Work out how to cancel blocking redis reads
	return true
}

// RegisterProxySet registers the proxy name set
func (pmc *Environment) RegisterProxySet(proxyNames []string) {
	// Use pipeline to atomically replace contents of redis set
	newStackKey := pmc.envStackKey + "_new"
	var diffResult *redis.StringSliceCmd
	_, err := pmc.redisClient.TxPipelined(func(p *redis.Pipeline) error {
		for _, proxyName := range proxyNames {
			p.SAdd(newStackKey, proxyName)
		}
		diffResult = p.SDiff(pmc.envStackKey, newStackKey)
		p.Rename(newStackKey, pmc.envStackKey)
		return nil
	})

	if err != nil {
		log.Panicf("Error updating proxy name set: %#v\n", err)
	}

	removed := diffResult.Val()
	if len(removed) > 0 {
		//TODO clean up proxy registration set & queues - after proxy is given time to shutdown. Maybe once all registrations timeout? Maybe create secondary cleanup list?
		log.Println("Proxy removal detected:", removed)
	}
}

func (pmc *Environment) cleanupLoop() {
	pmc.cleanup()
	tickChannel := time.Tick(time.Duration(pmc.registrationTimeoutSeconds * int(time.Second)))
	for tickTime := range tickChannel {
		res := pmc.cleanup()
		log.Println("Cleanup call complete:", tickTime, res)
	}
}

func (pmc *Environment) cleanup() bool {
	proxies, err := pmc.redisClient.SMembers(pmc.envStackKey).Result()
	if err != nil {
		log.Println("SMembers failure:", err)
		return false
	}

	for _, proxy := range proxies {
		proxyRegoKey := GetProxyRegoKey(pmc.proxyBaseKey, proxy)

		err := pmc.redisClient.Watch(func(tx *redis.Tx) error {
			//Get registrations older than timeout
			zRangeBy := &redis.ZRangeBy{
				Min: "-inf",
				Max: strconv.FormatInt(time.Now().UTC().Add(-time.Duration(pmc.registrationTimeoutSeconds*int(time.Second))).Unix(), 10),
			}
			queueKeys, err := tx.ZRangeByScore(proxyRegoKey, *zRangeBy).Result()
			if err != nil && err != redis.Nil {
				return err
			}

			if len(queueKeys) == 0 {
				evEmptyOldProxyRegistration.Add(1)
				return nil
			}

			_, err = tx.Pipelined(func(pipe *redis.Pipeline) error {
				for _, queueKey := range queueKeys {
					pipe.ZRem(proxyRegoKey, queueKey)
					pipe.Del(queueKey)
					evDeletedProxyRegistrations.Add(1)
				}
				return nil
			})
			return err
		}, proxyRegoKey)
		if err == redis.TxFailedErr {
			evDeletedProxyRegistrationFails.Add(1)
		}

		if err != nil {
			log.Println("Watch error:", err)
			continue
		}

	}
	//TODO - Cleanup queues for removed proxies. Probably have to keep a list of deleted proxies.

	return true
}

func (pmc *Environment) receiveLoop() {
	for {
		rawMessage, brpopErr := pmc.redisClient.BRPop(0, pmc.envDestKey).Result()

		if brpopErr != nil {
			log.Println("BRPop error:", brpopErr)
			continue
		}
		if len(rawMessage) != 2 {
			log.Println("Unexpected BRPop result length:", len(rawMessage))
			continue
		}
		if rawMessage[0] != pmc.envDestKey {
			log.Println("Unexpected BRPop result key:", rawMessage[0])
			continue
		}

		pmc.InboundMessageChannel <- rawMessage[1]
	}
}
