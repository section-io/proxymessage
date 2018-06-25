package proxymessage

import (
	"bytes"
	"log"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

//These tests require a local redis server at 6379
const redisHost = "127.0.0.1"
const redisPort = "6379"

func mustSetEnv(name string, value string) {
	err := os.Setenv(name, value)
	if err != nil {
		log.Panic(err)
	}
}

func TestLoadingViaEnvVars(t *testing.T) {

	expectedRegoKey := "registrationKey"
	expectedPrefix := "listKeyPrefix"
	suffix := "listKeySuffix"
	expectedSuffix := sha1FromString(suffix)
	expectedTimeout := 10

	os.Clearenv()
	mustSetEnv("REDIS_HOST", redisHost)
	mustSetEnv("REDIS_PORT", redisPort)
	mustSetEnv("PROXY_REGO_KEY", expectedRegoKey)
	mustSetEnv("LIST_KEY_PREFIX", expectedPrefix)
	mustSetEnv("LIST_KEY_SUFFIX", suffix)
	mustSetEnv("MESSAGE_CLIENT_REGISTRATION_TIMEOUT_SECONDS", strconv.Itoa(expectedTimeout))

	proxyMessageClient := NewClientFromEnvVars()

	assert.Equal(t, expectedPrefix+"-"+expectedSuffix, proxyMessageClient.listKey, "proxyMessageClient.listKey")
	assert.Equal(t, expectedRegoKey, proxyMessageClient.registrationKey, "proxyMessageClient.registrationKey")
	assert.Equal(t, expectedTimeout, proxyMessageClient.registrationTimeoutSeconds, "proxyMessageClient.registrationKey")
}

func TestLoadingViaEnvVarsInvalidTimeout(t *testing.T) {

	os.Clearenv()
	mustSetEnv("REDIS_HOST", redisHost)
	mustSetEnv("REDIS_PORT", redisPort)
	mustSetEnv("PROXY_REGO_KEY", "registrationKey")
	mustSetEnv("LIST_KEY_PREFIX", "listKeyPrefix")
	mustSetEnv("LIST_KEY_SUFFIX", "listKeySuffix")
	mustSetEnv("MESSAGE_CLIENT_REGISTRATION_TIMEOUT_SECONDS", "Invalid")

	assert.PanicsWithValue(t, "If MESSAGE_CLIENT_REGISTRATION_TIMEOUT_SECONDS is set it must be a valid integer.", func() { NewClientFromEnvVars() })
}

func TestLoadingViaEnvVarsDefaultTimeout(t *testing.T) {

	expectedTimeoutSeconds := 60

	os.Clearenv()
	mustSetEnv("REDIS_HOST", redisHost)
	mustSetEnv("REDIS_PORT", redisPort)
	mustSetEnv("PROXY_REGO_KEY", "registrationKey")
	mustSetEnv("LIST_KEY_PREFIX", "listKeyPrefix")
	mustSetEnv("LIST_KEY_SUFFIX", "listKeySuffix")

	proxyMessageClient := NewClientFromEnvVars()

	assert.Equal(t, expectedTimeoutSeconds, proxyMessageClient.registrationTimeoutSeconds, "proxyMessageClient.registrationTimeoutSeconds")
}

func TestListKeyPrefix(t *testing.T) {

	proxyMessageClient := NewClient(redisHost+":"+redisPort, "registrationKey", "listKeyPrefix", "listKeySuffix", 0)

	expected := "listKeyPrefix-"
	actual := proxyMessageClient.listKey

	if !strings.HasPrefix(actual, expected) {
		t.Errorf("Expected prefix '%s' but was '%s'.", expected, actual)
	}
}

func TestListKeySuffix(t *testing.T) {

	proxyMessageClient := NewClient(redisHost+":"+redisPort, "registrationKey", "listKeyPrefix", "listKeySuffix", 0)

	expected := "-listKeySuffix"
	actual := proxyMessageClient.listKey

	if !strings.HasSuffix(actual, expected) {
		t.Errorf("Expected prefix '%s' but was '%s'.", expected, actual)
	}
}

func TestInitialRegistration(t *testing.T) {

	expectedLower := time.Now()
	dur, _ := time.ParseDuration("2s")
	//Should be registered within 2 seconds
	proxyMessageClient := NewClient(redisHost+":"+redisPort, "registrationKey", "listKeyPrefix", "listKeySuffix", 0)
	time.Sleep(dur)
	expectedUpper := time.Now()
	actual := proxyMessageClient.lastRegistrationSuccess //time.Now().Sub()

	if actual.Before(expectedLower) || actual.After(expectedUpper) {
		t.Errorf("Expected connection between '%s'and '%s' but was '%s'.", expectedLower, expectedUpper, actual)
	}
}

func TestReRegistration(t *testing.T) {

	proxyMessageClient := NewClient(redisHost+":"+redisPort, "registrationKey", "listKeyPrefix", "listKeySuffix", 1)
	//Should be registered within 2 seconds
	waitDuration, _ := time.ParseDuration("4s")
	lastRegoDuration, _ := time.ParseDuration("2s")
	time.Sleep(waitDuration)
	expectedUpper := time.Now()
	expectedLower := expectedUpper.Add(-lastRegoDuration)
	actual := proxyMessageClient.lastRegistrationSuccess

	if actual.Before(expectedLower) || actual.After(expectedUpper) {
		t.Errorf("Expected connection between '%s'and '%s' but was '%s'.", expectedLower, expectedUpper, actual)
	}
}

func TestDebugLog(t *testing.T) {

	var buf bytes.Buffer
	log.SetOutput(&buf)
	defer func() {
		log.SetOutput(os.Stderr)
	}()

	os.Clearenv()
	mustSetEnv("DEBUG", "1")
	NewClient(redisHost+":"+redisPort, "registrationKey", "listKeyPrefix", "listKeySuffix", 0)
	waitDuration, _ := time.ParseDuration("4s")
	time.Sleep(waitDuration)

	assert.Contains(t, buf.String(), "Register call complete")
}

func TestNoDebugLog(t *testing.T) {

	var buf bytes.Buffer
	log.SetOutput(&buf)
	defer func() {
		log.SetOutput(os.Stderr)
	}()

	os.Clearenv()
	NewClient(redisHost+":"+redisPort, "registrationKey", "listKeyPrefix", "listKeySuffix", 0)
	waitDuration, _ := time.ParseDuration("4s")
	time.Sleep(waitDuration)

	assert.NotContains(t, buf.String(), "Register call complete")
}
