package proxymessage

import (
	//"fmt"
	"testing"
	//"strings"

	"time"

	redis "gopkg.in/redis.v5"
)

//These tests require a local redis server at 6379

func TestProxyMessageClientServer2client1message(t *testing.T) {
	prefix := "TestProxyMessageClientServer2client1message"
	queueName := prefix + "queue"
	redisAddress := redisHost + ":" + redisPort
	envID := "env321"
	branch := "mabranch"
	proxyName := "proxyName"
	message := "{\"repo_name\":\"" + envID + "\",\"branch_name\":\"" + branch + "\",\"message_type\":\"proxymessage\",\"proxy_name\":\"" + proxyName + "\"}"

	pms := NewServer(redisAddress, queueName, 0)
	defer pms.Shutdown()

	proxyKeyBase := getProxyKeyBase(envID+"/"+branch, queueName)
	proxyRegoKey := getProxyRegoKey(proxyKeyBase, proxyName)
	proxyListKeyPrefix := getProxyListKeyPrefix(proxyKeyBase, proxyName)

	pmc1 := NewClient(redisAddress, proxyRegoKey, proxyListKeyPrefix, "suffix1", 0)
	pmc2 := NewClient(redisAddress, proxyRegoKey, proxyListKeyPrefix, "suffix2", 0)

	//Should be registered within 2 seconds
	dur, _ := time.ParseDuration("2s")
	time.Sleep(dur)

	redisClient := redis.NewClient(&redis.Options{Addr: redisAddress})
	redisClient.LPush(queueName, message).Val()

	//Only wait a second for messages to propogate
	time.AfterFunc(time.Second, func() {
		close(pmc1.InboundMessageChannel)
		close(pmc2.InboundMessageChannel)
	})

	//Check message got thru
	{
		expected := message
		actual, ok := <-pmc1.InboundMessageChannel
		if !ok || actual != expected {
			t.Errorf("Expected message  '%s' but was '%s'.", expected, actual)
		}
	}
	{
		expected := message
		actual, ok := <-pmc2.InboundMessageChannel
		if !ok || actual != expected {
			t.Errorf("Expected message  '%s' but was '%s'.", expected, actual)
		}
	}

	//Check no extra messages
	{
		actual, ok := <-pmc1.InboundMessageChannel
		if ok {
			t.Errorf("pmc1 Unexpected extra message '%s'.", actual)
		}
	}
	{
		actual, ok := <-pmc2.InboundMessageChannel
		if ok {
			t.Errorf("pmc2 Unexpected extra message '%s'.", actual)
		}
	}

}

func TestProxyMessageClientServer2ClientManyMessage(t *testing.T) {
	prefix := "TestProxyMessageClientServer2ClientManyMessage"
	queueName := prefix + "queue"
	redisAddress := redisHost + ":" + redisPort
	envID := "321"
	branch := "dabranch"
	proxyName := "proxyName"
	message := "{\"repo_name\":\"" + envID + "\",\"branch_name\":\"" + branch + "\",\"message_type\":\"proxymessage\",\"proxy_name\":\"" + proxyName + "\"}"
	messageCount := 1000
	expected := float64(500) // 500 messages a second from startup seems easy enough

	proxyKeyBase := getProxyKeyBase(envID+"/"+branch, queueName)
	proxyRegoKey := getProxyRegoKey(proxyKeyBase, proxyName)
	proxyListKeyPrefix := getProxyListKeyPrefix(proxyKeyBase, proxyName)

	pmc1 := NewClient(redisAddress, proxyRegoKey, proxyListKeyPrefix, "suffix1", 0)
	pmc2 := NewClient(redisAddress, proxyRegoKey, proxyListKeyPrefix, "suffix2", 0)

	//Should be registered within 2 seconds
	dur, _ := time.ParseDuration("2s")
	time.Sleep(dur)

	//Preload Redis queue with messages
	redisClient := redis.NewClient(&redis.Options{Addr: redisAddress})
	for i := 0; i < messageCount; i++ {
		redisClient.LPush(queueName, message).Val()
	}

	s1 := make(chan bool)
	s2 := make(chan bool)
	go func() {
		for i := 0; i < messageCount; i++ {
			<-pmc1.InboundMessageChannel
		}
		close(s1)
	}()
	go func() {
		for i := 0; i < messageCount; i++ {
			<-pmc2.InboundMessageChannel
		}
		close(s2)
	}()

	pms := NewServer(redisAddress, queueName, 0)
	defer pms.Shutdown()

	startTime := time.Now()
	//Wait for completion signals
	<-s1
	<-s2
	elapsed := time.Now().Sub(startTime)

	actual := float64(messageCount) / elapsed.Seconds()

	//Check message rate
	if actual < expected {
		t.Errorf(" Unexpected  message rate '%f' vs '%f", actual, expected)
	}
}

func TestProxyMessageServerUpdateEnv(t *testing.T) {
	prefix := "TestProxyMessageServerUpdateEnv"
	queueName := prefix + "queue"
	redisAddress := redisHost + ":" + redisPort
	envID := "321"
	message := "{\"repo_name\":\"env" + envID + "\",\"branch_name\":\"Production\",\"message_type\":\"updateenvironment\"}"

	pms := NewServer(redisAddress, queueName, 0)

	redisClient := redis.NewClient(&redis.Options{Addr: redisAddress})
	redisClient.LPush(queueName, message).Val()

	//Only wait a second for messages to propogate
	time.AfterFunc(time.Second, func() {
		pms.Shutdown()
	})

	//Check message got thru
	{
		expected := message
		actual, ok := <-pms.UpdateEnvironmentMessages
		if !ok || actual != expected {
			t.Errorf("Expected message  '%s' but was '%s'.", expected, actual)
		}
	}

	//Check no extra messages
	{
		actual, ok := <-pms.UpdateEnvironmentMessages
		if ok {
			t.Errorf("Unexpected extra message '%s'.", actual)
		}
	}

}
