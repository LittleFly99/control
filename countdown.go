package main

import (
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"
)

func calculateRemainingTime(endTime int) string {
	now := int(time.Now().Unix())
	remaining := endTime - now

	if remaining <= 0 {
		return "0:00"
	}

	minutes := remaining / 60
	seconds := remaining % 60

	return fmt.Sprintf("%02d:%02d", minutes, seconds)
}

func parseCountdownSeconds(countdown string) int {
	countdown = strings.TrimSpace(countdown)
	if countdown == "" {
		return 0
	}

	parts := strings.Split(countdown, ":")
	if len(parts) == 2 {
		minutes, errMin := strconv.Atoi(strings.TrimSpace(parts[0]))
		seconds, errSec := strconv.Atoi(strings.TrimSpace(parts[1]))
		if errMin == nil && errSec == nil && minutes >= 0 && seconds >= 0 {
			return minutes*60 + seconds
		}
	}

	// 后台「倒计时」下拉值为 1-10 分钟；历史数据也可能是纯数字分钟
	if n, err := strconv.Atoi(countdown); err == nil && n > 0 {
		if n <= 60 {
			return n * 60
		}
		return n
	}

	return 0
}

func getFirstMessageCountdown() string {
	if !firstMessageGameState.isActive || !firstMessageGameState.isCountdown {
		return ""
	}

	now := time.Now()
	if now.After(firstMessageGameState.endTime) {
		firstMessageGameState.isActive = false
		firstMessageGameState.isCountdown = false
		log.Printf("第一个小纸条游戏倒计时结束")
		return ""
	}

	remaining := firstMessageGameState.endTime.Sub(now)
	minutes := int(remaining.Minutes())
	seconds := int(remaining.Seconds()) % 60

	return fmt.Sprintf("%02d:%02d", minutes, seconds)
}
