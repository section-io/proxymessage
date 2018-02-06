package proxymessage

import (
	"encoding/json"
	"expvar"
	"log"
	"strconv"
	"strings"
	"time"

	redis "gopkg.in/redis.v5"
)

//const defaultRegistrationTimeoutSeconds = 60
const defaultSourceKey = "queue"
const maxProxyListLength = 1000 //MAX_PROXY_LIST_LENGTH
const maxDeadListLength = 1000
const envQueueLimit = 10

// Server is the main proxymessage broker
type Server struct {
	redisClient                *redis.Client
	registrationKey            string
	registrationTimeoutSeconds int

	sourceKey string

	deadMessageQueueKey string

	UpdateEnvironmentMessages chan string
}

var (
	evInboundCount                     = expvar.NewInt("ProxyMessageServerInboundCount")
	evTrimmedProxyMessageOutbound      = expvar.NewInt("ProxyMessageServerTrimmedProxyMessageOutboundCount")
	evTrimmedUpdateEnvironmentOutbound = expvar.NewInt("ProxyMessageServerTrimmedUpdateEnvironmentOutboundCount")
	evInboundUnmarshalErrorCount       = expvar.NewInt("ProxyMessageServerInboundUnmarshalErrorCount")
	evEmptyProxyMessageOutbound        = expvar.NewInt("ProxyMessageServerevEmptyProxyDestinationOutboundCount")
)

// NewServer creates a new ProxyMessageServer
func NewServer(redisAddress string, sourceKey string, registrationTimeoutSeconds int) *Server {
	if registrationTimeoutSeconds == 0 {
		registrationTimeoutSeconds = defaultRegistrationTimeoutSeconds
	}
	if sourceKey == "" {
		sourceKey = defaultSourceKey
	}

	pms := &Server{
		redisClient: redis.NewClient(&redis.Options{
			Addr: redisAddress,
		}),
		sourceKey:                  sourceKey,
		deadMessageQueueKey:        sourceKey + "-dead",
		registrationTimeoutSeconds: registrationTimeoutSeconds,

		UpdateEnvironmentMessages: make(chan string),
	}

	go pms.queueListenLoop()

	return pms
}

type jsonBusMessageSummary struct {
	RepositoryName string `json:"repo_name"` /// "env#{request[:environmentid]}",
	BranchName     string `json:"branch_name"`
	MessageType    string `json:"message_type"`
	ProxyName      string `json:"proxy_name"`
}

//Shutdown down the server
func (pms *Server) Shutdown() {
	close(pms.UpdateEnvironmentMessages)
	//TODO work out how to cancel the blocking redis pops.
}

//GetEnvDestKey gets a ENV_DEST_KEY
func (pms *Server) GetEnvDestKey(environmentID string) string {
	return getEnvDestKey(environmentID, pms.sourceKey)
}

//GetEnvStackKey gets a ENV_STACK_KEY
func (pms *Server) GetEnvStackKey(environmentID string) string {
	return getEnvStackKey(environmentID, pms.sourceKey)
}

//GetProxyKeyBase gets a PROXY_KEY_BASE
func (pms *Server) GetProxyKeyBase(environmentID string) string {
	return getProxyKeyBase(environmentID, pms.sourceKey)
}

func (pms *Server) pushToDead(msg string) {
	deadSize := pms.redisClient.LPush(pms.deadMessageQueueKey, msg).Val()
	if deadSize > maxDeadListLength {
		pms.redisClient.LTrim(pms.deadMessageQueueKey, 0, maxDeadListLength).Result()
	}
}

func (pms *Server) queueListenLoop() {
	for {
		rawMessage, brpopErr := pms.redisClient.BRPop(0, pms.sourceKey).Result()

		if brpopErr != nil {
			log.Println("BRPop error:", brpopErr)
			continue
		}
		if len(rawMessage) != 2 {
			log.Println("Unexpected BRPop result length:", len(rawMessage))
			continue
		}
		if rawMessage[0] != pms.sourceKey {
			log.Println("Unexpected BRPop result key:", rawMessage[0])
			continue
		}

		var messageRouting jsonBusMessageSummary //map[string]interface{}
		jsonDecodeErr := json.Unmarshal([]byte(rawMessage[1]), &messageRouting)

		if jsonDecodeErr != nil {
			log.Println("Unmarshal error:", jsonDecodeErr)
			pms.pushToDead(rawMessage[1])
			evInboundUnmarshalErrorCount.Add(1)
			continue
		}

		evInboundCount.Add(1)

		//unsupported repository name
		if messageRouting.RepositoryName == "" {
			log.Println("unsupported repository name")
			pms.pushToDead(rawMessage[1])
			continue
		}

		//unsupported branch name
		if messageRouting.BranchName == "" {
			log.Println("unsupported branch name")
			pms.pushToDead(rawMessage[1])
			continue
		}

		if messageRouting.MessageType == "updateenvironment" {
			//Message from git
			//Should probably be queued per-environment
			log.Println("Handling MessageType: ", messageRouting.MessageType)

			envID := getEnvironmentID(messageRouting.RepositoryName, messageRouting.BranchName)

			log.Println("Getting getEnvDestKey")
			envDestKey := getEnvDestKey(envID, pms.sourceKey)

			//Put message in queue for environment provisioner
			log.Println("Putting message in EP queue")
			newQueueSize, lpushErr := pms.redisClient.LPush(envDestKey, rawMessage[1]).Result()
			if lpushErr != nil {
				log.Printf("Failure to LPush to EnvDestKey %s. Error: %s\n", envDestKey, lpushErr)
				continue
			}
			if newQueueSize > envQueueLimit {
				log.Println("Env Queue length exceeded trimming:", envDestKey)
				pms.redisClient.LTrim(envDestKey, 0, envQueueLimit).Result()
				evTrimmedUpdateEnvironmentOutbound.Add(1)
			}

			//kubectl apply environment-provisioner.yaml passing ENV_DEST_KEY, ENV_STACK_KEY and PROXY_KEY_BASE as environment variables
			// MUST WAIT on environment-provisioner.yaml coming up and
			//Perform SMEMBERS $ENV_STACK_KEY and treat each result as message.proxyname and forward as per proxymessage...
			log.Println("Placing message in channel UpdateEnvironmentMessages")
			pms.UpdateEnvironmentMessages <- rawMessage[1]
			log.Println("Placed message in channel UpdateEnvironmentMessages")

		} else if messageRouting.MessageType == "proxymessage" {
			// Proxy message - fan out
			envID := getEnvironmentID(messageRouting.RepositoryName, messageRouting.BranchName)

			proxyKeyBase := getProxyKeyBase(envID, pms.sourceKey)
			proxyRegoKey := getProxyRegoKey(proxyKeyBase, messageRouting.ProxyName)

			//ZRANGEBYSCORE $PROXY_REGO_KEY $max_age_unix_timestamp +inf and foreach RESULT
			t := &redis.ZRangeBy{
				Min: strconv.FormatInt(time.Now().UTC().Add(time.Duration(-pms.registrationTimeoutSeconds*int(time.Second))).Unix(), 10),
				Max: "+inf",
			}
			queueKeys, zRangeByScoreErr := pms.redisClient.ZRangeByScore(proxyRegoKey, *t).Result()

			if zRangeByScoreErr != nil {
				log.Println("Fetch zRangeByScoreErr error:", zRangeByScoreErr)
				continue
			}

			if len(queueKeys) == 0 {
				log.Println("Cannot forward message. No proxies registered:", proxyRegoKey)
				evEmptyProxyMessageOutbound.Add(1)
				continue
			}

			proxyListKeyPrefix := getProxyListKeyPrefix(proxyKeyBase, messageRouting.ProxyName)
			for _, queueKey := range queueKeys {
				if !strings.HasPrefix(queueKey, proxyListKeyPrefix) {
					log.Printf("Warning: Destination key %s doesn't use expected prefix %s from registration %s\n", queueKey, proxyListKeyPrefix, proxyRegoKey)
					continue
				}
				length, lpushErr := pms.redisClient.LPush(queueKey, rawMessage[1]).Result()
				if lpushErr != nil {
					log.Println("Fetch LPush error:", lpushErr)
					continue
				}
				if length > maxProxyListLength {
					log.Println("Queue length exceeded trimming:", queueKey)
					pms.redisClient.LTrim(queueKey, 0, maxProxyListLength-1).Result()
					evTrimmedProxyMessageOutbound.Add(1)
				}
				//TODO Pipeline for less round trips (perf)
				_, expireErr := pms.redisClient.Expire(queueKey, time.Duration(2*pms.registrationTimeoutSeconds*int(time.Second))).Result()
				if expireErr != nil {
					log.Println("Expiry error:", expireErr)
				}

			}
		} else {
			// Unhandled message
			log.Println("Unhandled message type:", messageRouting.MessageType)
		}
	}

}

func getEnvironmentID(RepositoryName, BranchName string) string {
	return RepositoryName + "/" + BranchName
}
