package proxymessage

import (
	"reflect"
	"sort"
	"testing"
	"time"

	redis "gopkg.in/redis.v5"
)

//These tests require a local redis server at 6379

func TestEnvironmentCleanupOldQueue(t *testing.T) {
	prefix := "TestEnvironmentCleanupOldQueue"
	redisAddress := redisHost + ":" + redisPort

	envStackKey := prefix + "envStackKey"
	envDestKey := prefix + "envDestKey"
	proxyBaseKey := prefix + "proxyBaseKey"
	proxyName := prefix + "proxyName"
	redisClient := redis.NewClient(&redis.Options{Addr: redisAddress})
	redisClient.SAdd(envStackKey, proxyName)
	registrationKey := getProxyRegoKey(proxyBaseKey, proxyName)
	listKey := getProxyListKeyPrefix(proxyBaseKey, proxyName) + "_A"
	redisClient.ZAdd(registrationKey, *&redis.Z{Score: float64(time.Now().UTC().Unix()) - 90, Member: listKey}).Result()
	redisClient.LPush(listKey, "listitem")

	e := NewEnvironment(redisAddress, envDestKey, envStackKey, proxyBaseKey, 60)
	dur, _ := time.ParseDuration("2s")
	time.Sleep(dur)
	e.Shutdown()

	// Check queue registration deletion
	{
		expected := int64(0)
		actual, _ := redisClient.ZCard(registrationKey).Result()
		if actual != expected {
			t.Errorf("Expected message '%v' but was '%v'.", expected, actual)
		}
	}

	// Check queue deletion
	{
		expected := false
		actual, _ := redisClient.Exists(listKey).Result()
		if actual != expected {
			t.Errorf("Expected message '%v' but was '%v'.", expected, actual)
		}
	}
}

func TestEnvironmentReceive(t *testing.T) {
	prefix := "TestEnvironmentReceive"

	redisAddress := redisHost + ":" + redisPort

	envStackKey := prefix + "envStackKey"
	envDestKey := prefix + "envDestKey"
	proxyBaseKey := prefix + "proxyBaseKey"
	message := "messageForYou"
	redisClient := redis.NewClient(&redis.Options{Addr: redisAddress})
	redisClient.LPush(envDestKey, message)

	e := NewEnvironment(redisAddress, envDestKey, envStackKey, proxyBaseKey, 60)

	var r string
	go func() { r = <-e.InboundMessageChannel }()

	dur, _ := time.ParseDuration("2s")
	time.Sleep(dur)
	e.Shutdown()

	// Check queue registration deletion
	{
		expected := message
		actual := r
		if actual != expected {
			t.Errorf("Expected message '%v' but was '%v'.", expected, actual)
		}
	}

}

func TestEnvironmentAddProxy(t *testing.T) {
	prefix := "TestEnvironmentAddProxy"
	redisAddress := redisHost + ":" + redisPort

	envStackKey := prefix + "envStackKey"
	envDestKey := prefix + "envDestKey"
	proxyBaseKey := prefix + "proxyBaseKey"

	proxyList1 := []string{"A", "B"}
	proxyList2 := []string{"A"}

	redisClient := redis.NewClient(&redis.Options{Addr: redisAddress})

	e := NewEnvironment(redisAddress, envDestKey, envStackKey, proxyBaseKey, 60)
	e.RegisterProxySet(proxyList1)

	// Check proxy addition
	{
		expected := proxyList1
		actual := redisClient.SMembers(envStackKey).Val()
		sort.Strings(actual)

		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("Expected message '%v' but was '%v'.", expected, actual)
		}
	}

	// Check proxy deletion
	e.RegisterProxySet(proxyList2)
	{
		expected := proxyList2
		actual := redisClient.SMembers(envStackKey).Val()

		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("Expected message '%v' but was '%v'.", expected, actual)
		}
	}

	e.Shutdown()
}
