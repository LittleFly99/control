package main

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

// extractCountdownFromTopicContent 解析主题 content 中的倒计时（兼容 JSON 数字、分钟数字符串、MM:SS）
func extractCountdownFromTopicContent(content string) string {
	if content == "" {
		return ""
	}
	var raw map[string]interface{}
	if err := json.Unmarshal([]byte(content), &raw); err != nil {
		return ""
	}
	v, ok := raw["countdown"]
	if !ok || v == nil {
		return ""
	}
	switch n := v.(type) {
	case float64:
		minutes := int(n)
		if minutes > 0 {
			return fmt.Sprintf("%02d:00", minutes)
		}
	case int:
		if n > 0 {
			return fmt.Sprintf("%02d:00", n)
		}
	case int64:
		minutes := int(n)
		if minutes > 0 {
			return fmt.Sprintf("%02d:00", minutes)
		}
	case string:
		s := strings.TrimSpace(n)
		if s == "" {
			return ""
		}
		if strings.Contains(s, ":") {
			return s
		}
		if minutes, err := strconv.Atoi(s); err == nil && minutes > 0 {
			return fmt.Sprintf("%02d:00", minutes)
		}
	default:
		return strings.TrimSpace(fmt.Sprintf("%v", v))
	}
	return ""
}

// extractParticipantsFromTopicContent 解析主题 content 中的参与人数（兼容 JSON 数字或字符串）
func extractParticipantsFromTopicContent(content string) string {
	if content == "" {
		return ""
	}
	var raw map[string]interface{}
	if err := json.Unmarshal([]byte(content), &raw); err != nil {
		return ""
	}
	v, ok := raw["participants"]
	if !ok || v == nil {
		return ""
	}
	switch n := v.(type) {
	case float64:
		return strconv.Itoa(int(n))
	case int:
		return strconv.Itoa(n)
	case int64:
		return strconv.Itoa(int(n))
	case string:
		return strings.TrimSpace(n)
	default:
		return strings.TrimSpace(fmt.Sprintf("%v", v))
	}
}
