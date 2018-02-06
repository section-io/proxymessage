package proxymessage

import (
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
	os.Setenv("REDIS_HOST", redisHost)
	os.Setenv("REDIS_PORT", redisPort)
	os.Setenv("PROXY_REGO_KEY", expectedRegoKey)
	os.Setenv("LIST_KEY_PREFIX", expectedPrefix)
	os.Setenv("LIST_KEY_SUFFIX", suffix)
	os.Setenv("MESSAGE_CLIENT_REGISTRATION_TIMEOUT_SECONDS", strconv.Itoa(expectedTimeout))

	proxyMessageClient := NewClientFromEnvVars()

	assert.Equal(t, expectedPrefix+"-"+expectedSuffix, proxyMessageClient.listKey, "proxyMessageClient.listKey")
	assert.Equal(t, expectedRegoKey, proxyMessageClient.registrationKey, "proxyMessageClient.registrationKey")
	assert.Equal(t, expectedTimeout, proxyMessageClient.registrationTimeoutSeconds, "proxyMessageClient.registrationKey")
}

func TestLoadingViaEnvVarsInvalidTimeout(t *testing.T) {

	os.Clearenv()
	os.Setenv("REDIS_HOST", redisHost)
	os.Setenv("REDIS_PORT", redisPort)
	os.Setenv("PROXY_REGO_KEY", "registrationKey")
	os.Setenv("LIST_KEY_PREFIX", "listKeyPrefix")
	os.Setenv("LIST_KEY_SUFFIX", "listKeySuffix")
	os.Setenv("MESSAGE_CLIENT_REGISTRATION_TIMEOUT_SECONDS", "Invalid")

	assert.PanicsWithValue(t, "If MESSAGE_CLIENT_REGISTRATION_TIMEOUT_SECONDS is set it must be a valid integer.", func() { NewClientFromEnvVars() })
}

func TestLoadingViaEnvVarsDefaultTimeout(t *testing.T) {

	expectedTimeoutSeconds := 60

	os.Clearenv()
	os.Setenv("REDIS_HOST", redisHost)
	os.Setenv("REDIS_PORT", redisPort)
	os.Setenv("PROXY_REGO_KEY", "registrationKey")
	os.Setenv("LIST_KEY_PREFIX", "listKeyPrefix")
	os.Setenv("LIST_KEY_SUFFIX", "listKeySuffix")

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
