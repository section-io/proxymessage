package proxymessage

import (
	"log"
	"os"
	"testing"
)

//import "strings"
//import "time"

//These tests require a local redis server at 6379
const redisHost = "127.0.0.1"
const redisPort = "6379"

func TestGetProxyKeyBase(t *testing.T) {
	expected := "e443bff3430372ffadbfb7d6c0d5755f8f26d030"
	actual := GetProxyKeyBase("environment", "test")

	if actual != expected {
		t.Errorf("Expected '%s' but was '%s'.", expected, actual)
	}
}

func TestGetProxyRegoKey(t *testing.T) {
	expected := "R3d384b6d84b2fb0d12fb25db6335f0aed174a304"
	actual := GetProxyRegoKey("ef66f0f10e112358a0e6208246ffb4962355113b", "test")

	if actual != expected {
		t.Errorf("Expected '%s' but was '%s'.", expected, actual)
	}
}

func MustSetEnv(name string, value string) {
	err := os.Setenv(name, value)
	if err != nil {
		log.Panic(err)
	}
}
