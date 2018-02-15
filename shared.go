package proxymessage

import "crypto/sha1"
import "fmt"

func sha1FromString(s string) string {
	h := sha1.New()
	h.Write([]byte(s))
	bs := h.Sum(nil)
	return fmt.Sprintf("%x", bs)
}

// PROXY_KEY_BASE - A per-environment value given to Environment Provisioners - it can be used to derive proxy level redis key names, but provides no insight into keys for other environments.
func GetProxyKeyBase(environmentID string, sourceKey string) string {
	s := environmentID + sourceKey + "proxy-queue-salt"
	return sha1FromString(s)
}

// PROXY_REGO_KEY - A per environment proxy key used to register a queue to receive messages
func GetProxyRegoKey(proxyKeyBase, proxyName string) string {
	s := proxyKeyBase + proxyName + "proxy-rego-salt"
	return "R" + sha1FromString(s)
}

//LIST_KEY_PREFIX - A per environment proxy key that enrollments will be restricted to
func GetProxyListKeyPrefix(proxyKeyBase, proxyName string) string {
	s := proxyKeyBase + proxyName + "proxy-queue-salt"
	return "P" + sha1FromString(s)
}

func GetEnvDestKey(environmentID string, sourceKey string) string {
	s := environmentID + sourceKey + "environment-queue-salt"
	return "E" + sha1FromString(s)
}

func GetEnvStackKey(environmentID string, sourceKey string) string {
	return "S" + GetEnvDestKey(environmentID, sourceKey)
}
