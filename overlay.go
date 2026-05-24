package main

import (
	"bytes"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

type overlayBroadcastRequest struct {
	GameID int                    `json:"game_id"`
	Type   string                 `json:"type"`
	Data   map[string]interface{} `json:"data"`
}

func (h *Hub) broadcastToGameControl(gameID int, wsType string, data map[string]interface{}) {
	if gameID <= 0 || wsType == "" {
		return
	}

	payload := Message{
		Type:   wsType,
		GameID: gameID,
		Data:   data,
	}
	if payload.Data == nil {
		payload.Data = map[string]interface{}{}
	}

	sent := 0
	h.mutex.RLock()
	for client := range h.clients {
		if client.Type != ControlClient || int(client.GameID) != gameID {
			continue
		}
		client.sendMessage(payload)
		sent++
	}
	h.mutex.RUnlock()

	log.Printf("overlay 广播 game_id=%d type=%s 送达=%d", gameID, wsType, sent)
}

func postJiubaOverlay(path string, form url.Values) {
	base := jiubaAPIBase()
	if base == "" {
		log.Printf("JIUBA_API_BASE 未配置，跳过回调 %s", path)
		return
	}

	endpoint := base + path
	body := form.Encode()
	req, err := http.NewRequest(http.MethodPost, endpoint, strings.NewReader(body))
	if err != nil {
		log.Printf("jiuba 回调创建请求失败: %v", err)
		return
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("jiuba 回调失败 url=%s err=%v", endpoint, err)
		return
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		log.Printf("jiuba 回调异常 url=%s status=%d body=%s", endpoint, resp.StatusCode, string(respBody))
		return
	}
	log.Printf("jiuba 回调成功 url=%s", endpoint)
}

func notifyJiubaOverlaySessionStart(gameID, gameType, overlayDurationSec int) {
	storeSid, err := getGameStoreSid(gameID)
	if err != nil || storeSid <= 0 {
		log.Printf("获取门店 sid 失败 game_id=%d: %v", gameID, err)
		return
	}

	form := url.Values{}
	form.Set("game_id", strconv.Itoa(gameID))
	form.Set("store_sid", strconv.Itoa(storeSid))
	form.Set("game_type", strconv.Itoa(gameType))
	form.Set("overlay_duration_sec", strconv.Itoa(overlayDurationSec))
	postJiubaOverlay("/outsideapi/game_overlay/sessionStart", form)
}

func notifyJiubaOverlaySessionEnd(gameID int) {
	form := url.Values{}
	form.Set("game_id", strconv.Itoa(gameID))
	postJiubaOverlay("/outsideapi/game_overlay/sessionEnd", form)
}

func extractOverlayDurationSec(data map[string]interface{}) int {
	if data == nil {
		return 8
	}
	if v, ok := data["overlay_duration_sec"]; ok {
		switch n := v.(type) {
		case float64:
			return int(n)
		case int:
			return n
		case json.Number:
			i, _ := n.Int64()
			return int(i)
		}
	}
	return 8
}

// 兼容 JSON body 的 broadcast（jiuba game_overlay_notify 发送 application/json）
func handleGameOverlayBroadcastJSON(hub *Hub, c *gin.Context) {
	raw, err := io.ReadAll(c.Request.Body)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"code": 400, "msg": "读取请求失败"})
		return
	}
	c.Request.Body = io.NopCloser(bytes.NewBuffer(raw))

	var req overlayBroadcastRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"code": 400, "msg": "JSON 无效"})
		return
	}
	if req.GameID <= 0 || req.Type == "" {
		c.JSON(http.StatusBadRequest, gin.H{"code": 400, "msg": "game_id 或 type 缺失"})
		return
	}

	hub.broadcastToGameControl(req.GameID, req.Type, req.Data)
	c.JSON(http.StatusOK, gin.H{"code": 0, "msg": "ok"})
}
