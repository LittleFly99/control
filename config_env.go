package main

import (
	"log"
	"os"
	"strings"

	"github.com/joho/godotenv"
)

func appEnv() string {
	env := strings.TrimSpace(os.Getenv("APP_ENV"))
	if env == "" {
		env = "test"
	}
	return env
}

func loadAppEnv() {
	env := appEnv()
	candidates := []string{
		"config.env." + env,
		"config.env",
	}
	for _, file := range candidates {
		if err := godotenv.Load(file); err == nil {
			log.Printf("已加载配置 %s (APP_ENV=%s)", file, env)
			return
		}
	}
	log.Printf("未找到配置文件，使用环境变量 (APP_ENV=%s)", env)
}

func jiubaAPIBase() string {
	base := strings.TrimRight(getEnv("JIUBA_API_BASE", ""), "/")
	if base != "" {
		return base
	}
	if appEnv() == "prod" {
		return "https://kscy.crushclub.love"
	}
	return "https://house.makoo.xyz"
}
