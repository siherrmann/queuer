package helper

import (
	"log"
	"os"
	"strings"
)

func GetEnvVariableWithoutDelete(name string) string {
	envVariable := os.Getenv(name)
	if len(strings.TrimSpace(envVariable)) == 0 {
		log.Fatalf("get secret %v... from the admin", name[0:5])
	}
	return envVariable
}

func GetEnvVariable(name string) string {
	envVariable := os.Getenv(name)
	if len(strings.TrimSpace(envVariable)) == 0 {
		log.Fatalf("get secret %v... from the admin", name[0:5])
	}
	err := os.Unsetenv(name)
	if err != nil {
		log.Fatalf("error removing env variable: %v", err)
	}
	return envVariable
}
