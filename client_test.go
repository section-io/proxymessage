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

func TestLoadingViaEnvVars(t *testing.T) {

	expectedRegoKey := "registrationKey"
	expectedPrefix := "listKeyPrefix"
	suffix := "listKeySuffix"
	expectedSuffix := sha1FromString(suffix)
	expectedTimeout := 10

	os.Clearenv()
	MustSetEnv("REDIS_HOST", redisHost)
	MustSetEnv("REDIS_PORT", redisPort)
	MustSetEnv("PROXY_REGO_KEY", expectedRegoKey)
	MustSetEnv("LIST_KEY_PREFIX", expectedPrefix)
	MustSetEnv("LIST_KEY_SUFFIX", suffix)
	MustSetEnv("MESSAGE_CLIENT_REGISTRATION_TIMEOUT_SECONDS", strconv.Itoa(expectedTimeout))

	proxyMessageClient := NewClientFromEnvVars()

	assert.Equal(t, expectedPrefix+"-"+expectedSuffix, proxyMessageClient.listKey, "proxyMessageClient.listKey")
	assert.Equal(t, expectedRegoKey, proxyMessageClient.registrationKey, "proxyMessageClient.registrationKey")
	assert.Equal(t, expectedTimeout, proxyMessageClient.registrationTimeoutSeconds, "proxyMessageClient.registrationKey")
}

func TestLoadingViaEnvVarsInvalidTimeout(t *testing.T) {

	os.Clearenv()
	MustSetEnv("REDIS_HOST", redisHost)
	MustSetEnv("REDIS_PORT", redisPort)
	MustSetEnv("PROXY_REGO_KEY", "registrationKey")
	MustSetEnv("LIST_KEY_PREFIX", "listKeyPrefix")
	MustSetEnv("LIST_KEY_SUFFIX", "listKeySuffix")
	MustSetEnv("MESSAGE_CLIENT_REGISTRATION_TIMEOUT_SECONDS", "Invalid")

	assert.PanicsWithValue(t, "If MESSAGE_CLIENT_REGISTRATION_TIMEOUT_SECONDS is set it must be a valid integer.", func() { NewClientFromEnvVars() })
}

func TestLoadingViaEnvVarsDefaultTimeout(t *testing.T) {

	expectedTimeoutSeconds := 60

	os.Clearenv()
	MustSetEnv("REDIS_HOST", redisHost)
	MustSetEnv("REDIS_PORT", redisPort)
	MustSetEnv("PROXY_REGO_KEY", "registrationKey")
	MustSetEnv("LIST_KEY_PREFIX", "listKeyPrefix")
	MustSetEnv("LIST_KEY_SUFFIX", "listKeySuffix")

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

	proxyMessageClient := NewClient(redisHost+":"+redisPort, "registrationKey", "listKeyPrefix", "listKeySuffix", 0)
	//Should be registered within 2 seconds
	dur, _ := time.ParseDuration("2s")
	time.Sleep(dur)
	expectedUpper := time.Now()
	expectedLower := expectedUpper.Add(-dur)
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
	actual := proxyMessageClient.lastRegistrationSuccess //time.Now().Sub()

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
	MustSetEnv("DEBUG", "1")
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
