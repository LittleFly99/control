package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	_ "github.com/go-sql-driver/mysql"
	"github.com/gorilla/websocket"
	"github.com/joho/godotenv"
)

// 数据库配置
type DatabaseConfig struct {
	Host     string
	Port     int
	User     string
	Password string
	DBName   string
}

// WebSocket连接类型
type ClientType string

const (
	ControlClient ClientType = "control"
	GameClient    ClientType = "game"
)

// WebSocket客户端
type Client struct {
	ID     string          `json:"id"`
	Conn   *websocket.Conn `json:"-"`
	Type   ClientType      `json:"type"`
	GameID int             `json:"game_id,omitempty"`
	Table  string          `json:"table,omitempty"`
	Send   chan []byte     `json:"-"`
	Hub    *Hub            `json:"-"`
}

// 存储客户端桌号的映射
var clientTableMap = make(map[string]string) // key: clientID, value: table

// 全局Hub实例，供HTTP接口复用
var globalHub *Hub

// WebSocket消息
type Message struct {
	Type     string      `json:"type"`
	Data     interface{} `json:"data"`
	GameID   int         `json:"game_id,omitempty"`
	ClientID string      `json:"client_id,omitempty"`
}

// 游戏状态
type GameStatus struct {
	ID          int    `json:"id"`
	GameType    int    `json:"game_type"`
	Status      int    `json:"status"`
	Content     string `json:"content"`
	SignupCount int    `json:"signup_count"`
	StartTime   string `json:"start_time"`
}

// 报名信息
type SignupInfo struct {
	ID       int    `json:"id"`
	GameID   int    `json:"game_id"`
	Table    string `json:"table"`
	ClientID int    `json:"client_id"`
	Gender   string `json:"gender"`
}

// HTTP报名通知请求结构
type SignupNotifyRequest struct {
	GameID   int    `json:"game_id"`
	Table    string `json:"table"`
	SignupID int    `json:"signup_id"`
}

// 内存中的性别存储
var genderMap = make(map[string]string) // key: table, value: gender

// 第一个小纸条游戏状态管理
var firstMessageGameState struct {
	isActive     bool      // 游戏是否激活
	startTime    time.Time // 游戏开始时间
	endTime      time.Time // 游戏结束时间（开始时间+5分钟）
	isCountdown  bool      // 是否在倒计时中
	hasBroadcast bool      // 是否已经广播过第一个消息桌号
}

// 游戏匹配信息
type GameMatch struct {
	Table1 string `json:"table1"`
	Table2 string `json:"table2"`
	Winner string `json:"winner"`
	Result string `json:"result"`
}

// Hub管理所有WebSocket连接
type Hub struct {
	clients    map[*Client]bool
	register   chan *Client
	unregister chan *Client
	broadcast  chan []byte
	mutex      sync.RWMutex
}

func newHub() *Hub {
	return &Hub{
		clients:    make(map[*Client]bool),
		register:   make(chan *Client),
		unregister: make(chan *Client),
		broadcast:  make(chan []byte),
	}
}

// Hub 层面通知控制台更新报名列表
func (h *Hub) notifySignupUpdate(gameID int) {
	// 从数据库获取报名信息
	signups, err := getSignupList(gameID)
	if err != nil {
		log.Printf("获取报名列表失败: %v", err)
		return
	}

	// 使用map按桌号去重，保留每个桌号的最新报名记录
	tableMap := make(map[string]SignupInfo)
	for _, signup := range signups {
		gender := genderMap[signup.Table] // 从内存中获取性别信息
		signupInfo := SignupInfo{
			ID:       signup.ID,
			GameID:   signup.GameID,
			Table:    signup.Table,
			ClientID: signup.ClientID,
			Gender:   gender,
		}
		// 由于数据库按id升序排列，后面的会覆盖前面的，保留最新记录
		tableMap[signup.Table] = signupInfo
	}

	// 将map转换为数组
	signupData := make([]SignupInfo, 0, len(tableMap))
	for _, signupInfo := range tableMap {
		signupData = append(signupData, signupInfo)
	}

	log.Printf("报名列表去重（断连更新）: 原始数量=%d, 去重后数量=%d", len(signups), len(signupData))

	message := Message{
		Type: "signup_update",
		Data: signupData,
	}

	messageBytes, _ := json.Marshal(message)

	// 发送给所有控制台客户端
	h.mutex.RLock()
	for client := range h.clients {
		if client.Type == ControlClient {
			select {
			case client.Send <- messageBytes:
			default:
				// 发送失败，跳过
			}
		}
	}
	h.mutex.RUnlock()
}

func (h *Hub) run() {
	for {
		select {
		case client := <-h.register:
			h.mutex.Lock()
			h.clients[client] = true
			h.mutex.Unlock()
			log.Printf("客户端连接: %s (类型: %s)", client.ID, client.Type)

		case client := <-h.unregister:
			h.mutex.Lock()
			if _, ok := h.clients[client]; ok {
				delete(h.clients, client)
				close(client.Send)
			}
			h.mutex.Unlock()
			log.Printf("客户端断开: %s", client.ID)

			// 如果是游戏客户端且已报名，删除报名记录
			if client.Type == GameClient {
				if table, exists := clientTableMap[client.ID]; exists {
					gameID := int(client.GameID)
					if gameID > 0 {
						// 从数据库删除报名记录
						err := deleteSignupByTableAndGame(gameID, table)
						if err != nil {
							log.Printf("删除报名记录失败: 游戏ID=%d, 桌号=%s, 错误=%v", gameID, table, err)
						} else {
							log.Printf("用户断开连接，已删除报名记录: 游戏ID=%d, 桌号=%s", gameID, table)

							// 从内存中删除性别信息
							delete(genderMap, table)

							// 从客户端桌号映射中删除
							delete(clientTableMap, client.ID)

							// 通知控制台更新报名列表
							h.notifySignupUpdate(gameID)
						}
					}
				}
			}

		case message := <-h.broadcast:
			h.mutex.RLock()
			for client := range h.clients {
				select {
				case client.Send <- message:
				default:
					close(client.Send)
					delete(h.clients, client)
				}
			}
			h.mutex.RUnlock()
		}
	}
}

// 客户端读写处理
func (c *Client) readPump() {
	defer func() {
		c.Hub.unregister <- c
		c.Conn.Close()
	}()

	c.Conn.SetReadLimit(512)
	c.Conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	c.Conn.SetPongHandler(func(string) error {
		c.Conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})

	for {
		_, messageBytes, err := c.Conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("WebSocket错误: %v", err)
			}
			break
		}

		var message Message
		if err := json.Unmarshal(messageBytes, &message); err != nil {
			log.Printf("消息解析错误: %v", err)
			continue
		}

		c.handleMessage(message)
	}
}

func (c *Client) writePump() {
	ticker := time.NewTicker(54 * time.Second)
	defer func() {
		ticker.Stop()
		c.Conn.Close()
	}()

	for {
		select {
		case message, ok := <-c.Send:
			c.Conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if !ok {
				c.Conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			// 单独发送每条消息，避免批量发送导致前端解析问题
			if err := c.Conn.WriteMessage(websocket.TextMessage, message); err != nil {
				return
			}

			// 发送队列中的其他消息
			n := len(c.Send)
			for i := 0; i < n; i++ {
				nextMessage := <-c.Send
				c.Conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
				if err := c.Conn.WriteMessage(websocket.TextMessage, nextMessage); err != nil {
					return
				}
			}

		case <-ticker.C:
			c.Conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := c.Conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

// 推送当前比赛状态给控制台
func (c *Client) pushCurrentMatchState(gameID int) {
	// 获取游戏信息
	gameInfo, err := getGameInfo(gameID)
	if err != nil {
		log.Printf("获取游戏信息失败: %v", err)
		return
	}

	// 获取报名数量
	signups, err := getSignupList(gameID)
	if err != nil {
		log.Printf("获取报名列表失败: %v", err)
		signups = []SignupRecord{}
	}

	// 计算剩余时间
	remainingTime := calculateRemainingTime(gameInfo.StartTime)

	// 发送完整的游戏信息，包含报名数量
	gameInfoWithSignup := GameStatus{
		ID:          gameInfo.ID,
		GameType:    1, // 默认游戏类型
		Status:      gameInfo.Status,
		Content:     gameInfo.Content,
		SignupCount: len(signups),
		StartTime:   remainingTime,
	}

	gameInfoMessage := Message{
		Type:   "game_info",
		Data:   gameInfoWithSignup,
		GameID: gameID,
	}
	c.sendMessage(gameInfoMessage)

	// 如果游戏状态是进行中，推送当前报名列表
	if gameInfo.Status == 2 {
		signupList, _ := getSignupList(gameID)
		signupMessage := Message{
			Type:   "signup_list",
			Data:   signupList,
			GameID: gameID,
		}
		c.sendMessage(signupMessage)

		// 推送当前比赛信息（如果有的话）
		gameContent, err := getGameContent(gameID)
		if err == nil && gameContent.Table1 != "" && gameContent.Table2 != "" {
			matchInfo := map[string]interface{}{
				"table1": gameContent.Table1,
				"table2": gameContent.Table2,
				"winner": gameContent.Winner,
				"result": gameContent.Result,
			}
			matchMessage := Message{
				Type:   "match_info",
				Data:   matchInfo,
				GameID: gameID,
			}
			c.sendMessage(matchMessage)
		}
	}

	log.Printf("已推送当前比赛状态给控制台: 游戏ID=%d", gameID)
}

// 处理不同类型的消息
func (c *Client) handleMessage(message Message) {
	switch message.Type {
	case "signup":
		c.handleSignup(message)
	case "start_signup":
		c.handleStartSignup(message)
	case "end_signup":
		c.handleEndSignup(message)
	case "start_match":
		c.handleStartMatch(message)
	case "end_match":
		c.handleEndMatch(message)
	case "end_game":
		c.handleEndGame(message)
	case "get_game_info":
		c.handleGetGameInfo(message)
	case "player_choice":
		c.handlePlayerChoice(message)
	case "tie_breaker":
		c.handleTieBreaker(message)
	case "confirm_players":
		c.handleConfirmPlayers(message)
	case "control_start":
		c.handleControlStart(message)
	case "control_stop":
		c.handleControlStop(message)
	case "enter_game":
		c.handleEnterGame(message)
	case "start_game_communication":
		c.handleStartGame(message)
	case "end_game_communication":
		c.handleEndGameCommunication(message)
	case "announce_result":
		c.handleAnnounceResult(message)
	default:
		log.Printf("未知消息类型: %s", message.Type)
	}
}

// 处理报名
func (c *Client) handleSignup(message Message) {
	if c.Type != GameClient {
		return
	}

	// 从消息中获取桌号
	if message.Data == nil {
		log.Printf("报名消息中Data字段为空")
		return
	}

	data, ok := message.Data.(map[string]interface{})
	if !ok {
		log.Printf("报名消息中Data格式错误")
		return
	}

	table, ok := data["table"].(string)
	if !ok {
		log.Printf("报名消息中缺少桌号信息")
		return
	}

	gender, ok := data["gender"].(string)
	if !ok {
		log.Printf("报名消息中缺少性别信息")
		return
	}

	// 生成客户端ID
	clientID := int(time.Now().UnixNano() % 1000000)

	// 保存报名信息到数据库
	gameID := int(c.GameID)
	if gameID == 0 {
		log.Printf("无效的游戏ID: %d", gameID)
		return
	}

	err := addSignupRecord(gameID, table, clientID)
	if err != nil {
		log.Printf("保存报名信息失败: %v", err)
		return
	}

	// 将性别信息存储到内存中
	genderMap[table] = gender

	// 存储客户端桌号映射
	clientTableMap[c.ID] = table

	log.Printf("用户报名成功: 游戏ID=%d, 桌号=%s, 性别=%s, 客户端ID=%d", gameID, table, gender, clientID)

	// 发送报名成功消息给客户端
	signupSuccessMessage := Message{
		Type: "signup_success",
		Data: map[string]interface{}{
			"client_id": clientID,
			"table":     table,
		},
	}
	c.sendMessage(signupSuccessMessage)

	// 通知控制台更新报名信息
	c.notifyControlUpdate()
}

// 处理开始报名
func (c *Client) handleStartSignup(message Message) {
	if c.Type != ControlClient {
		return
	}

	gameID := int(c.GameID)
	if gameID == 0 {
		log.Printf("无效的游戏ID: %d", gameID)
		return
	}

	var (
		isCustom     bool
		participants string
		countdown    string
		prizeName    string
	)

	if gameInfo, err := getGameInfo(gameID); err != nil {
		log.Printf("获取游戏信息失败: %v", err)
	} else if gameInfo != nil {
		isCustom = gameInfo.IsCustom == 1
		if isCustom && gameInfo.TopicID > 0 {
			if topicInfo, err := getGameTopicByID(gameInfo.TopicID); err != nil {
				log.Printf("获取游戏主题失败: %v", err)
			} else if topicInfo != nil && topicInfo.Content != "" {
				var topicContent struct {
					Participants string `json:"participants"`
					Countdown    string `json:"countdown"`
				}
				if err := json.Unmarshal([]byte(topicInfo.Content), &topicContent); err != nil {
					log.Printf("解析游戏主题内容失败: %v", err)
				} else {
					participants = topicContent.Participants
					countdown = topicContent.Countdown
				}
			}
			if gameContent, err := getGameContent(gameID); err != nil {
				log.Printf("获取游戏内容失败: %v", err)
			} else if gameContent != nil {
				prizeName = gameContent.PrizeName
			}
		}
	}

	// 计算结束时间：当前时间 + 3分钟
	endTime := int(time.Now().Unix()) + 180 // 180秒 = 3分钟

	// 清空报名池子
	err := clearGameSignups(gameID)
	if err != nil {
		log.Printf("清空报名池失败: %v", err)
	}

	// 清空内存中的性别信息
	genderMap = make(map[string]string)

	// 清空客户端桌号映射
	clientTableMap = make(map[string]string)

	// 更新游戏状态和结束时间
	err = updateGameStatusAndStartTime(gameID, 1, endTime)
	if err != nil {
		log.Printf("更新游戏状态失败: %v", err)
		return
	}

	log.Printf("开始报名: 游戏ID=%d, 结束时间=%d", gameID, endTime)

	// 计算剩余时间（分钟:秒格式）
	remainingTime := calculateRemainingTime(endTime)

	// 广播给所有客户端
	c.broadcastToClients(Message{
		Type:   "game_status",
		Data:   GameStatus{ID: gameID, GameType: 1, Status: 1, StartTime: remainingTime},
		GameID: gameID,
	})

	// 广播开始报名成功消息给所有控制台客户端
	startSignupData := map[string]interface{}{
		"game_id":    gameID,
		"game_type":  1, // 默认游戏类型
		"status":     1,
		"start_time": remainingTime,
		"message":    "报名已开始",
	}

	if isCustom {
		if participants != "" {
			startSignupData["participants"] = participants
		}
		startSignupData["is_custom"] = true
		if countdown != "" {
			startSignupData["start_time"] = countdown
		}
		if prizeName != "" {
			startSignupData["prize_name"] = prizeName
		}
	}

	c.sendToControlClients(Message{
		Type:   "start_signup_success",
		Data:   startSignupData,
		GameID: gameID,
	})

	// 如果是自定义游戏，将配置广播给所有大屏端
	if isCustom {
		customConfig := map[string]interface{}{
			"game_id": gameID,
		}
		if participants != "" {
			customConfig["participants"] = participants
		}
		if countdown != "" {
			customConfig["start_time"] = countdown
		}
		if prizeName != "" {
			customConfig["prize_name"] = prizeName
		}
		customConfig["message"] = "自定义游戏报名配置"

		customMessage := Message{
			Type:   "custom_signup_config",
			Data:   customConfig,
			GameID: gameID,
		}

		c.Hub.mutex.RLock()
		for client := range c.Hub.clients {
			if client.Type == GameClient {
				client.sendMessage(customMessage)
				log.Printf("下发自定义报名配置给大屏端: %s", client.ID)
			}
		}
		c.Hub.mutex.RUnlock()
	}
}

// 处理报名结束
func (c *Client) handleEndSignup(message Message) {
	if c.Type != ControlClient {
		return
	}

	// 更新游戏状态为倒计时结束（报名结束，等待开始比赛）
	gameID := int(c.GameID)
	if gameID == 0 {
		log.Printf("无效的游戏ID: %d", gameID)
		return
	}

	err := updateGameStatus(gameID, 2)
	if err != nil {
		log.Printf("更新游戏状态失败: %v", err)
		return
	}

	log.Printf("报名结束，游戏开始: 游戏ID=%d", gameID)

	// 广播给所有客户端
	c.broadcastToClients(Message{
		Type:   "game_status",
		Data:   GameStatus{ID: gameID, GameType: 1, Status: 2},
		GameID: gameID,
	})
}

// 处理开始对战
func (c *Client) handleStartMatch(message Message) {
	if c.Type != ControlClient {
		return
	}

	// 从消息中获取匹配信息
	if message.Data == nil {
		log.Printf("匹配消息中Data字段为空")
		return
	}

	matchData, ok := message.Data.(map[string]interface{})
	if !ok {
		log.Printf("匹配消息格式错误")
		return
	}

	table1, _ := matchData["table1"].(string)
	table2, _ := matchData["table2"].(string)
	customData, _ := matchData["custom_data"]

	gameID := int(c.GameID)
	if gameID == 0 {
		log.Printf("无效的游戏ID: %d", gameID)
		return
	}

	// 获取现有游戏内容
	existingContent, err := getGameContent(gameID)
	if err != nil {
		log.Printf("获取游戏内容失败: %v", err)
		return
	}

	// 获取游戏记录，判断是否自定义
	gameInfo, err := getGameInfo(gameID)
	if err != nil {
		log.Printf("获取游戏记录失败: %v", err)
		return
	}
	isCustomGame := gameInfo.IsCustom == 1

	// 更新游戏状态为进行中
	err = updateGameStatus(gameID, 3)
	if err != nil {
		log.Printf("更新游戏状态失败: %v", err)
		return
	}

	// 更新游戏内容，重置游戏状态
	content := *existingContent
	content.Winner = ""
	content.Result = ""

	err = updateGameContentJSON(gameID, content)
	if err != nil {
		log.Printf("更新游戏内容失败: %v", err)
		return
	}

	log.Printf("开始对战: 游戏ID=%d, %s vs %s", gameID, table1, table2)

	// 给所有游戏客户端发送游戏状态更新
	gameStatusMessage := Message{
		Type:   "game_status",
		Data:   GameStatus{ID: gameID, GameType: 1, Status: 3},
		GameID: gameID,
	}

	c.Hub.mutex.RLock()
	for client := range c.Hub.clients {
		if client.Type == GameClient {
			client.sendMessage(gameStatusMessage)
			log.Printf("发送游戏状态更新给客户端: %s", client.ID)

			if isCustomGame {
				customPayload := map[string]interface{}{
					"type":   "custom_match_start",
					"table1": table1,
					"table2": table2,
				}
				if customData != nil {
					customPayload["custom_data"] = customData
				}
				client.sendMessage(Message{
					Type:   "custom_match_info",
					Data:   customPayload,
					GameID: gameID,
				})
				log.Printf("自定义游戏下发桌号信息给客户端: %s (桌号1: %s, 桌号2: %s)", client.ID, table1, table2)
			}
		}
	}
	c.Hub.mutex.RUnlock()

	// 发送游戏状态更新给控制台客户端
	c.sendToControlClients(Message{
		Type:   "game_status",
		Data:   GameStatus{ID: gameID, GameType: 1, Status: 3},
		GameID: gameID,
	})
}

// 处理结束对战
func (c *Client) handleEndMatch(message Message) {
	if c.Type != ControlClient {
		return
	}

	// 从消息中获取对战结果
	if message.Data == nil {
		log.Printf("对战结果消息中Data字段为空")
		return
	}

	matchData, ok := message.Data.(map[string]interface{})
	if !ok {
		log.Printf("对战结果消息格式错误")
		return
	}

	table1, _ := matchData["table1"].(string)
	table2, _ := matchData["table2"].(string)
	winner, _ := matchData["winner"].(string)
	result, _ := matchData["result"].(string)

	gameID := int(c.GameID)
	if gameID == 0 {
		log.Printf("无效的游戏ID: %d", gameID)
		return
	}

	// 获取现有游戏内容
	existingContent, err := getGameContent(gameID)
	if err != nil {
		log.Printf("获取游戏内容失败: %v", err)
		return
	}

	// 更新游戏内容，添加对战结果
	content := *existingContent
	content.Winner = "桌" + winner
	content.GameType = "猜拳"
	content.Rounds = 3
	content.Result = result

	err = updateGameContentJSON(gameID, content)
	if err != nil {
		log.Printf("更新游戏内容失败: %v", err)
		return
	}

	// 更新游戏状态为已完成
	err = updateGameStatus(gameID, 4)
	if err != nil {
		log.Printf("更新游戏状态失败: %v", err)
		return
	}

	log.Printf("结束对战: 游戏ID=%d, %s vs %s, 获胜者: %s", gameID, table1, table2, winner)

	// 确定获胜者和失败者
	var winnerTable, loserTable string
	if winner == table1 {
		winnerTable = table1
		loserTable = table2
	} else {
		winnerTable = table2
		loserTable = table1
	}

	// 给获胜者发送胜利消息（包含status字段和奖品信息）
	winnerMessage := Message{
		Type: "game_result",
		Data: map[string]interface{}{
			"result":     "胜利",
			"message":    fmt.Sprintf("恭喜您获得%s！", existingContent.PrizeName),
			"opponent":   "桌" + loserTable,
			"winner":     "桌" + winner,              // 添加胜利方信息
			"prize_name": existingContent.PrizeName, // 添加奖品名称信息
			"status":     4,                         // 添加状态字段，表示游戏结束
		},
		GameID: gameID,
	}

	// 给失败者发送失败消息（包含status字段和奖品信息）
	loserMessage := Message{
		Type: "game_result",
		Data: map[string]interface{}{
			"result":     "失败",
			"message":    "很遗憾，您输了",
			"opponent":   "桌" + winnerTable,
			"winner":     "桌" + winner,              // 添加胜利方信息
			"prize_name": existingContent.PrizeName, // 添加奖品名称信息
			"status":     4,                         // 添加状态字段，表示游戏结束
		},
		GameID: gameID,
	}

	// 给其他用户发送观战结果（包含status字段和奖品信息）
	spectatorMessage := Message{
		Type: "game_result",
		Data: map[string]interface{}{
			"result":     "观战",
			"message":    fmt.Sprintf("对战结束：%s 获胜", "桌"+winner),
			"winner":     "桌" + winner,              // 添加胜利方信息
			"loser":      "桌" + loserTable,          // 添加失败方信息
			"prize_name": existingContent.PrizeName, // 添加奖品名称信息
			"status":     4,                         // 添加状态字段，表示游戏结束
		},
		GameID: gameID,
	}

	// 分别发送消息给不同的客户端
	c.Hub.mutex.RLock()
	for client := range c.Hub.clients {
		if client.Type == GameClient {
			// 通过客户端桌号映射来匹配
			if clientTable, exists := clientTableMap[client.ID]; exists {
				if clientTable == winnerTable {
					client.sendMessage(winnerMessage)
					log.Printf("发送胜利消息给获胜者: %s (桌号: %s)", client.ID, clientTable)
				} else if clientTable == loserTable {
					client.sendMessage(loserMessage)
					log.Printf("发送失败消息给失败者: %s (桌号: %s)", client.ID, clientTable)
				} else {
					client.sendMessage(spectatorMessage)
					log.Printf("发送观战消息给其他用户: %s (桌号: %s)", client.ID, clientTable)
				}
			} else {
				// 没有桌号映射的客户端也发送观战消息
				client.sendMessage(spectatorMessage)
				log.Printf("发送观战消息给未参与用户: %s", client.ID)
			}
		}
	}
	c.Hub.mutex.RUnlock()

	// 发送游戏状态更新给控制台客户端（包含胜利方和奖品名称信息）
	gameStatusData := map[string]interface{}{
		"id":         gameID,
		"game_type":  1,
		"status":     4,
		"winner":     "桌" + winner,              // 添加胜利方信息
		"prize_name": existingContent.PrizeName, // 添加奖品名称信息
		"table1":     "桌" + table1,              // 添加对战桌号信息
		"table2":     "桌" + table2,              // 添加对战桌号信息
		"result":     result,                    // 添加对战结果
	}

	gameStatusMessage := Message{
		Type:   "game_status",
		Data:   gameStatusData,
		GameID: gameID,
	}

	c.Hub.mutex.RLock()
	for client := range c.Hub.clients {
		if client.Type == ControlClient {
			client.sendMessage(gameStatusMessage)
			log.Printf("发送游戏状态更新给控制台客户端: %s, 胜利方: 桌%s, 奖品: %s", client.ID, winner, existingContent.PrizeName)
		}
	}
	c.Hub.mutex.RUnlock()

	// 发送结束对战成功回应给所有控制台客户端
	endMatchResponse := Message{
		Type: "end_match_success",
		Data: map[string]interface{}{
			"game_id":    gameID,
			"table1":     "桌" + table1,
			"table2":     "桌" + table2,
			"winner":     "桌" + winner,
			"result":     result,
			"prize_name": existingContent.PrizeName,
			"message":    fmt.Sprintf("对战结束：%s 获胜", "桌"+winner),
			"timestamp":  time.Now().Unix(),
		},
		GameID: gameID,
	}

	c.sendToControlClients(endMatchResponse)
	log.Printf("已发送结束对战回应给控制台客户端，游戏ID: %d", gameID)
}

// 处理游戏结束
func (c *Client) handleEndGame(message Message) {
	if c.Type != ControlClient {
		return
	}

	// 更新游戏状态为已完成
	gameID := int(c.GameID)
	if gameID == 0 {
		log.Printf("无效的游戏ID: %d", gameID)
		return
	}

	err := updateGameStatus(gameID, 4)
	if err != nil {
		log.Printf("更新游戏状态失败: %v", err)
		return
	}

	log.Printf("游戏结束: 游戏ID=%d", gameID)

	// 广播给所有客户端
	c.broadcastToClients(Message{
		Type:   "game_status",
		Data:   GameStatus{ID: gameID, GameType: 1, Status: 3},
		GameID: gameID,
	})
}

// 获取游戏信息
func (c *Client) handleGetGameInfo(message Message) {
	// 从数据库获取游戏信息
	gameID := int(c.GameID)
	if gameID == 0 {
		log.Printf("无效的游戏ID: %d", gameID)
		return
	}

	game, err := getGameInfo(gameID)
	if err != nil {
		log.Printf("获取游戏信息失败: %v", err)
		return
	}

	// 获取报名数量
	signups, err := getSignupList(gameID)
	if err != nil {
		log.Printf("获取报名列表失败: %v", err)
		signups = []SignupRecord{}
	}

	// 计算剩余时间
	remainingTime := calculateRemainingTime(game.StartTime)

	// 处理机器编号，获取桌号
	var tableSN string
	if message.Data != nil {
		if data, ok := message.Data.(map[string]interface{}); ok {
			if machineSN, exists := data["machine_sn"].(string); exists && machineSN != "" {
				tableSN, err = getTableByMachineSN(machineSN)
				if err != nil {
					log.Printf("根据机器编号获取桌号失败: %v, machine_sn: %s", err, machineSN)
					// 继续执行，不返回错误
				} else {
					log.Printf("机器编号 %s 对应桌号: %s", machineSN, tableSN)
				}
			}
		}
	}

	// 发送完整的游戏信息，包含报名数量和桌号
	gameInfoData := map[string]interface{}{
		"id":           game.ID,
		"status":       game.Status,
		"content":      game.Content,
		"signup_count": len(signups),
		"start_time":   remainingTime,
	}

	// 如果获取到了桌号，添加到响应中
	if tableSN != "" {
		gameInfoData["table"] = tableSN
	}

	response := Message{
		Type:   "game_info",
		Data:   gameInfoData,
		GameID: gameID,
	}

	c.sendMessage(response)
}

// 处理玩家选择
func (c *Client) handlePlayerChoice(message Message) {
	if c.Type != GameClient {
		return
	}

	// 从消息中获取选择信息
	if message.Data == nil {
		log.Printf("玩家选择消息中Data字段为空")
		return
	}

	data, ok := message.Data.(map[string]interface{})
	if !ok {
		log.Printf("玩家选择消息中Data格式错误")
		return
	}

	choice, ok := data["choice"].(string)
	if !ok {
		log.Printf("玩家选择消息中缺少选择信息")
		return
	}

	table, ok := data["table"].(string)
	if !ok {
		log.Printf("玩家选择消息中缺少桌号信息")
		return
	}

	gameID := int(c.GameID)
	if gameID == 0 {
		log.Printf("无效的游戏ID: %d", gameID)
		return
	}

	log.Printf("玩家选择: 游戏ID=%d, 桌号=%s, 选择=%s", gameID, table, choice)

	// 发送选择信息给控制台
	choiceData := map[string]interface{}{
		"table":  table,
		"choice": choice,
	}

	choiceMessage := Message{
		Type: "player_choice",
		Data: choiceData,
	}

	c.sendToControlClients(choiceMessage)
}

// 处理加塞
func (c *Client) handleTieBreaker(message Message) {
	if c.Type != ControlClient {
		return
	}

	// 从消息中获取匹配信息
	if message.Data == nil {
		log.Printf("加塞消息中Data字段为空")
		return
	}

	tieData, ok := message.Data.(map[string]interface{})
	if !ok {
		log.Printf("加塞消息格式错误")
		return
	}

	table1, _ := tieData["table1"].(string)
	table2, _ := tieData["table2"].(string)

	gameID := int(c.GameID)
	if gameID == 0 {
		log.Printf("无效的游戏ID: %d", gameID)
		return
	}

	log.Printf("加塞: 游戏ID=%d, %s vs %s", gameID, table1, table2)

	// 通知对应客户端重新选择
	tieInfo := map[string]interface{}{
		"table1": "桌" + table1,
		"table2": "桌" + table2,
	}

	messageToSend := Message{
		Type: "game_tie",
		Data: tieInfo,
	}

	// 只发送给被选中的游戏客户端
	c.Hub.mutex.RLock()
	for client := range c.Hub.clients {
		if client.Type == GameClient {
			// 通过客户端桌号映射来匹配
			if clientTable, exists := clientTableMap[client.ID]; exists {
				if clientTable == table1 || clientTable == table2 {
					client.sendMessage(messageToSend)
					log.Printf("发送加塞消息给客户端: %s (桌号: %s)", client.ID, clientTable)
				}
			}
		}
	}
	c.Hub.mutex.RUnlock()

	log.Printf("已通知玩家重新选择: %s vs %s", table1, table2)

	// 发送加塞成功回应给所有控制台客户端
	tieBreakerResponse := Message{
		Type: "tie_breaker_success",
		Data: map[string]interface{}{
			"game_id":   gameID,
			"table1":    "桌" + table1,
			"table2":    "桌" + table2,
			"message":   fmt.Sprintf("加塞成功：%s vs %s 需要重新选择", "桌"+table1, "桌"+table2),
			"timestamp": time.Now().Unix(),
		},
		GameID: gameID,
	}

	c.sendToControlClients(tieBreakerResponse)
	log.Printf("已发送加塞回应给控制台客户端，游戏ID: %d", gameID)
}

// 处理确定玩家
func (c *Client) handleConfirmPlayers(message Message) {
	if c.Type != ControlClient {
		return
	}

	// 从消息中获取匹配信息
	if message.Data == nil {
		log.Printf("确定玩家消息中Data字段为空")
		return
	}

	confirmData, ok := message.Data.(map[string]interface{})
	if !ok {
		log.Printf("确定玩家消息格式错误")
		return
	}

	table1, _ := confirmData["table1"].(string)
	table2, _ := confirmData["table2"].(string)

	gameID := int(c.GameID)
	if gameID == 0 {
		log.Printf("无效的游戏ID: %d", gameID)
		return
	}

	// 检查是否需要随机选取桌号
	needRandomSelect := false
	if table1 == "" || table2 == "" {
		needRandomSelect = true
		log.Printf("检测到桌号不完整，需要随机选取: table1=%s, table2=%s", table1, table2)
	}

	// 如果需要随机选取，从报名列表中获取可用桌号
	if needRandomSelect {
		// 获取报名列表
		signups, err := getSignupList(gameID)
		if err != nil {
			log.Printf("获取报名列表失败: %v", err)
			return
		}

		// 去重，获取唯一桌号列表
		tableSet := make(map[string]bool)
		for _, signup := range signups {
			tableSet[signup.Table] = true
		}

		// 转换为数组
		availableTables := make([]string, 0, len(tableSet))
		for table := range tableSet {
			// 排除已经选中的桌号
			if table != table1 && table != table2 {
				availableTables = append(availableTables, table)
			}
		}

		// 如果没有可用桌号，返回错误
		if len(availableTables) == 0 && (table1 == "" || table2 == "") {
			log.Printf("没有可用的报名桌号进行随机选取")
			return
		}

		// 随机选取缺失的桌号
		if table1 == "" && len(availableTables) > 0 {
			randomIndex := rand.Intn(len(availableTables))
			table1 = availableTables[randomIndex]
			log.Printf("随机选取 table1: %s", table1)
			// 从可用列表中移除已选中的桌号
			availableTables = append(availableTables[:randomIndex], availableTables[randomIndex+1:]...)
		}

		if table2 == "" && len(availableTables) > 0 {
			randomIndex := rand.Intn(len(availableTables))
			table2 = availableTables[randomIndex]
			log.Printf("随机选取 table2: %s", table2)
		}

		// 最终检查
		if table1 == "" || table2 == "" {
			log.Printf("随机选取桌号失败，桌号不足: table1=%s, table2=%s", table1, table2)
			return
		}
	}

	log.Printf("确定玩家: 游戏ID=%d, %s vs %s", gameID, table1, table2)

	// 获取现有游戏内容
	existingContent, err := getGameContent(gameID)
	if err != nil {
		log.Printf("获取游戏内容失败: %v", err)
		return
	}

	// 更新游戏内容，记录对战的桌号
	content := *existingContent
	content.Table1 = "桌" + table1
	content.Table2 = "桌" + table2
	content.Winner = ""
	content.GameType = "猜拳"
	content.Rounds = 3
	content.Result = ""

	err = updateGameContentJSON(gameID, content)
	if err != nil {
		log.Printf("更新游戏内容失败: %v", err)
		return
	}

	// 广播确认玩家成功消息给所有控制台客户端
	c.sendToControlClients(Message{
		Type: "confirm_players_success",
		Data: map[string]interface{}{
			"game_id":   gameID,
			"game_type": 1, // 默认游戏类型
			"table1":    "桌" + table1,
			"table2":    "桌" + table2,
			"message":   "玩家确认成功",
		},
		GameID: gameID,
	})

	// 通知对应客户端准备游戏
	gameStartInfo := map[string]interface{}{
		"table1": "桌" + table1,
		"table2": "桌" + table2,
	}

	gameStartMessage := Message{
		Type: "game_start",
		Data: gameStartInfo,
	}

	// 给其他桌发送游戏结束消息
	gameEndMessage := Message{
		Type: "game_end",
		Data: map[string]interface{}{
			"message": "本轮游戏已结束",
			"table1":  "桌" + table1,
			"table2":  "桌" + table2,
		},
	}

	// 发送给所有游戏客户端
	c.Hub.mutex.RLock()
	for client := range c.Hub.clients {
		if client.Type == GameClient {
			// 通过客户端桌号映射来匹配
			if clientTable, exists := clientTableMap[client.ID]; exists {
				if clientTable == table1 || clientTable == table2 {
					// 给被选中的两桌发送游戏开始消息
					client.sendMessage(gameStartMessage)
					log.Printf("发送游戏开始消息给客户端: %s (桌号: %s)", client.ID, clientTable)
				} else {
					// 给其他桌发送游戏结束消息
					client.sendMessage(gameEndMessage)
					log.Printf("发送游戏结束消息给客户端: %s (桌号: %s)", client.ID, clientTable)
				}
			}
		}
	}
	c.Hub.mutex.RUnlock()

	log.Printf("已通知玩家准备游戏: %s vs %s，并通知其他桌游戏结束", table1, table2)
}

// 处理控制台开始
func (c *Client) handleControlStart(message Message) {
	if c.Type != ControlClient {
		return
	}

	gameID := int(c.GameID)
	log.Printf("控制台开始: 游戏ID=%d", gameID)

	// 广播给所有控制台客户端
	startMessage := Message{
		Type:   "control_started",
		Data:   map[string]interface{}{"game_id": gameID, "timestamp": time.Now().Unix()},
		GameID: gameID,
	}

	c.Hub.mutex.RLock()
	for client := range c.Hub.clients {
		if client.Type == ControlClient {
			client.sendMessage(startMessage)
			log.Printf("发送控制台开始消息给控制台客户端: %s", client.ID)
		}
	}
	c.Hub.mutex.RUnlock()
}

// 处理控制台停止
func (c *Client) handleControlStop(message Message) {
	if c.Type != ControlClient {
		return
	}

	gameID := int(c.GameID)
	log.Printf("控制台停止: 游戏ID=%d", gameID)

	// 广播给所有控制台客户端
	stopMessage := Message{
		Type:   "control_stopped",
		Data:   map[string]interface{}{"game_id": gameID, "timestamp": time.Now().Unix()},
		GameID: gameID,
	}

	c.Hub.mutex.RLock()
	for client := range c.Hub.clients {
		if client.Type == ControlClient {
			client.sendMessage(stopMessage)
			log.Printf("发送控制台停止消息给控制台客户端: %s", client.ID)
		}
	}
	c.Hub.mutex.RUnlock()
}

// 处理进入游戏
func (c *Client) handleEnterGame(message Message) {
	if c.Type != ControlClient {
		return
	}

	gameID := int(c.GameID)
	var (
		screenBgURL string
		isCustom    bool
		topicName   string
	)

	gameType := 1
	topicID := 0

	// 从消息中获取游戏类型与主题ID
	if message.Data != nil {
		if data, ok := message.Data.(map[string]interface{}); ok {
			if gt, exists := data["game_type"]; exists {
				if gtInt, ok := gt.(float64); ok {
					gameType = int(gtInt)
				} else if gtStr, ok := gt.(string); ok {
					if parsed, err := strconv.Atoi(gtStr); err == nil {
						gameType = parsed
					}
				}
			}
			if tid, exists := data["topic_id"]; exists {
				switch v := tid.(type) {
				case float64:
					topicID = int(v)
				case int:
					topicID = v
				case string:
					if parsed, err := strconv.Atoi(v); err == nil {
						topicID = parsed
					}
				}
			}
		}
	}

	// 如未上报主题ID，回退到游戏记录中的主题ID
	if gameID > 0 {
		gameInfo, err := getGameInfo(gameID)
		if err != nil {
			log.Printf("获取游戏信息失败: %v", err)
		} else if gameInfo.TopicID > 0 {
			topicID = gameInfo.TopicID
		}
	}

	// 根据主题ID加载背景与自定义标识
	if topicID > 0 {
		gameTopic, err := getGameTopicByID(topicID)
		if err != nil {
			log.Printf("获取游戏主题信息失败: %v", err)
		} else {
			topicContent := struct {
				ScreenBgURL string `json:"screen_bg_url"`
			}{}
			if gameTopic.Content != "" {
				if err := json.Unmarshal([]byte(gameTopic.Content), &topicContent); err != nil {
					log.Printf("解析游戏主题内容失败: %v", err)
				}
			}
			screenBgURL = topicContent.ScreenBgURL
			isCustom = gameTopic.Status == 1
			topicName = gameTopic.Name
		}
	}

	log.Printf("收到进入游戏请求: 游戏ID=%d, 游戏类型=%d, 客户端ID=%s", gameID, gameType, c.ID)

	// 构建响应消息
	responseMessage := fmt.Sprintf("欢迎进入游戏%d！", gameType)

	// 如果是猜拳游戏（game_type=2），查询奖品信息
	if gameType == 2 {
		gameContent, err := getGameContent(gameID)
		if err != nil {
			log.Printf("获取游戏内容失败: %v", err)
		} else if gameContent.PrizeName != "" && gameContent.GoodsPrice != "" {
			responseMessage = fmt.Sprintf("「 价值 %s 元的 %s 」", gameContent.GoodsPrice, gameContent.PrizeName)
		} else if gameContent.PrizeName != "" {
			responseMessage = gameContent.PrizeName
		}
	}
	if gameType == 3 {
		// 获取奖品信息
		prizeName, err := getFirstMessagePrizeInfo()
		if err != nil {
			log.Printf("获取奖品信息失败: %v", err)
			prizeName = "" // 设置为空字符串，不影响响应
		}
		responseMessage = prizeName
	}
	if gameType == 5 {
		if topicName != "" {
			responseMessage = fmt.Sprintf("%s！", topicName)
		} else {
			responseMessage = "欢迎进入：神秘主题！"
		}
	}

	// 广播进入游戏响应给所有控制台客户端
	enterGameMessage := Message{
		Type: "enter_game_response",
		Data: map[string]interface{}{
			"game_id":     gameID,
			"game_type":   gameType,
			"client_id":   c.ID,
			"message":     responseMessage,
			"timestamp":   time.Now().Unix(),
			"server_time": time.Now().Format("2006-01-02 15:04:05"),
		},
		GameID: gameID,
	}

	if screenBgURL != "" {
		enterGameMessage.Data.(map[string]interface{})["screen_bg_url"] = screenBgURL
	}
	enterGameMessage.Data.(map[string]interface{})["is_custom"] = isCustom

	c.Hub.mutex.RLock()
	for client := range c.Hub.clients {
		if client.Type == ControlClient {
			client.sendMessage(enterGameMessage)
			log.Printf("发送进入游戏%d响应给控制台客户端: %s", gameType, client.ID)
		}
	}
	c.Hub.mutex.RUnlock()
}

// 处理开始游戏
func (c *Client) handleStartGame(message Message) {
	if c.Type != ControlClient {
		return
	}

	// 解析游戏类型、视频链接和桌号
	gameType := 1 // 默认类型
	videoUrl := ""
	tableSN := ""
	var customData interface{}
	var tableList []string

	gameID := int(c.GameID)
	isCustomGame := false
	customPrizeName := ""

	if gameID > 0 {
		if gameInfo, err := getGameInfo(gameID); err != nil {
			log.Printf("获取游戏信息失败: %v", err)
		} else if gameInfo != nil {
			isCustomGame = gameInfo.IsCustom == 1
			if isCustomGame {
				if content, err := getGameContent(gameID); err != nil {
					log.Printf("获取游戏内容失败: %v", err)
				} else if content != nil {
					customPrizeName = content.PrizeName
				}
			}
		}
	}

	if data, ok := message.Data.(map[string]interface{}); ok {
		if gt, exists := data["game_type"]; exists {
			if gtFloat, ok := gt.(float64); ok {
				gameType = int(gtFloat)
			}
		}
		// 如果游戏类型为1，获取视频链接URL
		if gameType == 1 {
			if url, exists := data["video_url"]; exists {
				if urlStr, ok := url.(string); ok {
					videoUrl = urlStr
				}
			}
		}
		// 如果游戏类型为4（大冒险），获取桌号
		if gameType == 4 {
			if table, exists := data["table_sn"]; exists {
				if tableStr, ok := table.(string); ok {
					tableSN = tableStr
				}
			}
		}
		// 记录 DJ 上报的数据
		if gameType == 5 {
            if rawData, exists := data["tablelist"]; exists {
                customData = rawData
                tableList = extractTableList(rawData)
            }
		}
	}

	log.Printf("收到开始游戏请求: 游戏类型=%d, 客户端ID=%s, 视频链接=%s, 桌号=%s", gameType, c.ID, videoUrl, tableSN)

	// 如果游戏类型是3（第一个小纸条游戏），启动游戏状态
	if gameType == 3 {
		now := time.Now()
		firstMessageGameState.isActive = true
		firstMessageGameState.startTime = now
		firstMessageGameState.endTime = now.Add(5 * time.Minute) // 5分钟后结束
		firstMessageGameState.isCountdown = true
		firstMessageGameState.hasBroadcast = false // 重置广播标志

		log.Printf("启动第一个小纸条游戏，开始时间: %s, 结束时间: %s",
			now.Format("2006-01-02 15:04:05"),
			firstMessageGameState.endTime.Format("2006-01-02 15:04:05"))
	}

	// 构建响应数据
	responseData := map[string]interface{}{
		"game_type":   gameType,
		"client_id":   c.ID,
		"message":     fmt.Sprintf("游戏%d已开始！", gameType),
		"timestamp":   time.Now().Unix(),
		"server_time": time.Now().Format("2006-01-02 15:04:05"),
	}

	// 如果是游戏类型1，添加视频链接
	if gameType == 1 && videoUrl != "" {
		responseData["video_url"] = videoUrl
	}

	// 如果是游戏类型3（第一个小纸条游戏），添加倒计时时间和奖品信息
	if gameType == 3 {
		responseData["countdown"] = "05:00" // 5分钟倒计时

		// 获取奖品信息
		prizeName, err := getFirstMessagePrizeInfo()
		if err != nil {
			log.Printf("获取奖品信息失败: %v", err)
			prizeName = "" // 设置为空字符串，不影响响应
		}

		// 如果有奖品信息，添加到响应中
		if prizeName != "" {
			responseData["prize_name"] = prizeName
		}
	}

	// 如果是游戏类型4（大冒险），添加桌号信息
	if gameType == 4 && tableSN != "" {
		responseData["table_sn"] = tableSN
	}

	if isCustomGame {
		responseData["is_custom"] = true
		if customPrizeName != "" {
			responseData["prize_name"] = customPrizeName
		}
		if len(tableList) > 0 {
			responseData["tablelist"] = tableList
		} else if customData != nil {
			responseData["tablelist"] = customData
		}
	}

	// 广播开始游戏响应给所有控制台客户端
	startGameMessage := Message{
		Type: "start_game_communication_response",
		Data: responseData,
	}

	c.Hub.mutex.RLock()
	for client := range c.Hub.clients {
		if client.Type == ControlClient {
			client.sendMessage(startGameMessage)
			log.Printf("发送开始游戏%d响应给控制台客户端: %s", gameType, client.ID)
		}
	}
	c.Hub.mutex.RUnlock()
}

// 处理结束游戏通信
func (c *Client) handleEndGameCommunication(message Message) {
	if c.Type != ControlClient {
		return
	}

	// 解析游戏类型
	gameType := 1 // 默认类型
	if data, ok := message.Data.(map[string]interface{}); ok {
		if gt, exists := data["game_type"]; exists {
			if gtFloat, ok := gt.(float64); ok {
				gameType = int(gtFloat)
			}
		}
	}

	log.Printf("收到结束游戏请求: 游戏类型=%d, 客户端ID=%s", gameType, c.ID)

	// 如果游戏类型是3（第一个小纸条游戏），停止游戏状态
	if gameType == 3 {
		firstMessageGameState.isActive = false
		firstMessageGameState.isCountdown = false

		log.Printf("停止第一个小纸条游戏，停止时间: %s",
			time.Now().Format("2006-01-02 15:04:05"))
	}

	// 广播结束游戏响应给所有控制台客户端
	endGameMessage := Message{
		Type: "end_game_communication_response",
		Data: map[string]interface{}{
			"game_type":   gameType,
			"client_id":   c.ID,
			"message":     fmt.Sprintf("游戏%d已结束！", gameType),
			"timestamp":   time.Now().Unix(),
			"server_time": time.Now().Format("2006-01-02 15:04:05"),
		},
	}

	c.Hub.mutex.RLock()
	for client := range c.Hub.clients {
		if client.Type == ControlClient {
			client.sendMessage(endGameMessage)
			log.Printf("发送结束游戏%d响应给控制台客户端: %s", gameType, client.ID)
		}
	}
	c.Hub.mutex.RUnlock()
}

// 处理公示结果
func (c *Client) handleAnnounceResult(message Message) {
	if c.Type != ControlClient {
		return
	}

	gameID := int(c.GameID)
	log.Printf("收到公示结果请求: 游戏ID=%d, 客户端ID=%s", gameID, c.ID)

	// 查询游戏信息
	gameInfo, err := getGameInfo(gameID)
	if err != nil {
		log.Printf("查询游戏信息失败: %v", err)
		// 发送错误响应
		errorMessage := Message{
			Type: "announce_result_response",
			Data: map[string]interface{}{
				"game_id":     gameID,
				"client_id":   c.ID,
				"message":     "查询游戏信息失败",
				"prize_name":  "",
				"timestamp":   time.Now().Unix(),
				"server_time": time.Now().Format("2006-01-02 15:04:05"),
			},
		}
		c.sendMessage(errorMessage)
		return
	}

	// 解析content字段
	var contentData map[string]interface{}
	if err := json.Unmarshal([]byte(gameInfo.Content), &contentData); err != nil {
		log.Printf("解析游戏内容失败: %v", err)
		// 发送错误响应
		errorMessage := Message{
			Type: "announce_result_response",
			Data: map[string]interface{}{
				"game_id":     gameID,
				"client_id":   c.ID,
				"message":     "解析游戏内容失败",
				"prize_name":  "",
				"timestamp":   time.Now().Unix(),
				"server_time": time.Now().Format("2006-01-02 15:04:05"),
			},
		}
		c.sendMessage(errorMessage)
		return
	}

	// 获取prize_name
	prizeName, ok := contentData["prize_name"].(string)
	if !ok {
		log.Printf("未找到prize_name字段")
		prizeName = "未知奖品"
	}

	log.Printf("游戏%d的奖品名称: %s", gameID, prizeName)

	// 广播公示结果给所有控制台客户端
	announceMessage := Message{
		Type: "announce_result_response",
		Data: map[string]interface{}{
			"game_id":     gameID,
			"client_id":   c.ID,
			"message":     fmt.Sprintf("游戏%d结果公示", gameID),
			"prize_name":  prizeName,
			"timestamp":   time.Now().Unix(),
			"server_time": time.Now().Format("2006-01-02 15:04:05"),
		},
	}

	c.Hub.mutex.RLock()
	for client := range c.Hub.clients {
		if client.Type == ControlClient {
			client.sendMessage(announceMessage)
			log.Printf("发送公示结果给控制台客户端: %s, 奖品: %s", client.ID, prizeName)
		}
	}
	c.Hub.mutex.RUnlock()
}

// 通知控制台更新
func (c *Client) notifyControlUpdate() {
	// 从数据库获取报名信息
	gameID := int(c.GameID)
	if gameID == 0 {
		log.Printf("无效的游戏ID: %d", gameID)
		return
	}

	signups, err := getSignupList(gameID)
	if err != nil {
		log.Printf("获取报名列表失败: %v", err)
		return
	}

	// 使用map按桌号去重，保留每个桌号的最新报名记录
	tableMap := make(map[string]SignupInfo)
	for _, signup := range signups {
		gender := genderMap[signup.Table] // 从内存中获取性别信息
		signupInfo := SignupInfo{
			ID:       signup.ID,
			GameID:   signup.GameID,
			Table:    signup.Table,
			ClientID: signup.ClientID,
			Gender:   gender,
		}
		// 由于数据库按id升序排列，后面的会覆盖前面的，保留最新记录
		tableMap[signup.Table] = signupInfo
	}

	// 将map转换为数组
	signupData := make([]SignupInfo, 0, len(tableMap))
	for _, signupInfo := range tableMap {
		signupData = append(signupData, signupInfo)
	}

	log.Printf("报名列表去重: 原始数量=%d, 去重后数量=%d", len(signups), len(signupData))

	message := Message{
		Type: "signup_update",
		Data: signupData,
	}

	c.sendToControlClients(message)
}

// 通知匹配的玩家
func (c *Client) notifyMatchedPlayers(message Message) {
	// 从消息中获取匹配信息
	if message.Data == nil {
		log.Printf("匹配消息中Data字段为空")
		return
	}

	matchData, ok := message.Data.(map[string]interface{})
	if !ok {
		log.Printf("匹配消息格式错误")
		return
	}

	table1, _ := matchData["table1"].(string)
	table2, _ := matchData["table2"].(string)

	// 通知对应的客户端
	matchInfo := map[string]interface{}{
		"table1": "桌" + table1,
		"table2": "桌" + table2,
	}

	messageToSend := Message{
		Type: "match_info",
		Data: matchInfo,
	}

	// 发送给所有游戏客户端
	c.Hub.mutex.RLock()
	for client := range c.Hub.clients {
		if client.Type == GameClient {
			client.sendMessage(messageToSend)
		}
	}
	c.Hub.mutex.RUnlock()

	log.Printf("已通知匹配玩家: %s vs %s", table1, table2)
}

// 广播给所有客户端
func (c *Client) broadcastToClients(message Message) {
	messageBytes, _ := json.Marshal(message)
	c.Hub.broadcast <- messageBytes
}

func extractTableList(data interface{}) []string {
	var result []string
	switch v := data.(type) {
	case []interface{}:
		for _, item := range v {
			result = append(result, fmt.Sprintf("%v", item))
		}
	case []string:
		result = append(result, v...)
	case []int:
		for _, item := range v {
			result = append(result, fmt.Sprintf("%d", item))
		}
	case []float64:
		for _, item := range v {
			result = append(result, fmt.Sprintf("%v", item))
		}
	case map[string]interface{}:
		keys := make([]string, 0, len(v))
		for key := range v {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			result = append(result, fmt.Sprintf("%v", v[key]))
		}
	case map[string]string:
		keys := make([]string, 0, len(v))
		for key := range v {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			result = append(result, v[key])
		}
	case string:
		result = append(result, v)
	case float64, float32, int, int64, int32, uint, uint32, uint64, bool:
		result = append(result, fmt.Sprintf("%v", v))
	default:
		if data != nil {
			result = append(result, fmt.Sprintf("%v", data))
		}
	}
	return result
}

// 发送给控制台客户端
func (c *Client) sendToControlClients(message Message) {
	messageBytes, _ := json.Marshal(message)
	c.Hub.mutex.RLock()
	for client := range c.Hub.clients {
		if client.Type == ControlClient {
			select {
			case client.Send <- messageBytes:
			default:
				close(client.Send)
				delete(c.Hub.clients, client)
			}
		}
	}
	c.Hub.mutex.RUnlock()
}

// 发送消息给当前客户端
func (c *Client) sendMessage(message Message) {
	messageBytes, _ := json.Marshal(message)
	select {
	case c.Send <- messageBytes:
	default:
		close(c.Send)
		c.Hub.unregister <- c
	}
}

// WebSocket升级器
var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true // 允许跨域
	},
}

// WebSocket处理函数
func handleWebSocket(hub *Hub, c *gin.Context) {
	conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		log.Printf("WebSocket升级失败: %v", err)
		return
	}

	clientType := c.Query("type")
	gameIDStr := c.Query("game_id")
	table := c.Query("table")

	var gameID int
	if clientType == "game" || clientType == "control" {
		// 游戏客户端和控制台客户端都自动获取最新的未完成游戏
		latestGame, err := getLatestUnfinishedGame()
		if err != nil {
			log.Printf("获取最新未完成游戏失败: %v，客户端将继续连接等待游戏开始", err)
			// 不关闭连接，设置gameID为0，表示暂时没有进行中的游戏
			gameID = 0
		} else {
			gameID = latestGame.ID
			log.Printf("%s客户端连接到最新游戏: ID=%d", clientType, gameID)
		}
	} else {
		// 其他类型客户端使用提供的game_id
		gameID, _ = strconv.Atoi(gameIDStr)
	}

	client := &Client{
		ID:     fmt.Sprintf("%d", time.Now().UnixNano()),
		Conn:   conn,
		Type:   ClientType(clientType),
		GameID: gameID,
		Table:  table,
		Send:   make(chan []byte, 256),
		Hub:    hub,
	}

	client.Hub.register <- client

	// 控制台客户端连接后，等待客户端主动请求游戏信息
	// 不再自动推送状态，避免重复发送game_info消息

	go client.writePump()
	go client.readPump()
}

// HTTP API处理函数
func handleGetGameInfoAPI(c *gin.Context) {
	gameIDStr := c.Param("id")
	gameID, err := strconv.Atoi(gameIDStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"code": 0,
			"msg":  "无效的游戏ID",
		})
		return
	}

	// 从数据库获取游戏信息
	game, err := getGameInfo(gameID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"code": 0,
			"msg":  "获取游戏信息失败",
		})
		return
	}

	// 获取报名数量
	signups, err := getSignupList(gameID)
	if err != nil {
		log.Printf("获取报名列表失败: %v", err)
		signups = []SignupRecord{}
	}

	// 处理机器编号查询参数
	machineSN := c.Query("machine_sn")
	var tableSN string
	if machineSN != "" {
		tableSN, err = getTableByMachineSN(machineSN)
		if err != nil {
			log.Printf("根据机器编号获取桌号失败: %v, machine_sn: %s", err, machineSN)
			// 继续执行，不返回错误
		} else {
			log.Printf("HTTP API - 机器编号 %s 对应桌号: %s", machineSN, tableSN)
		}
	}

	// 构建响应数据
	gameInfoData := map[string]interface{}{
		"id":           game.ID,
		"status":       game.Status,
		"content":      game.Content,
		"signup_count": len(signups),
	}

	// 如果获取到了桌号，添加到响应中
	if tableSN != "" {
		gameInfoData["table"] = tableSN
	}

	c.JSON(http.StatusOK, gin.H{
		"code": 1,
		"data": gameInfoData,
		"msg":  "success",
	})
}

// HTTP接口：通知控制端刷新报名列表
func handleSignupNotifyAPI(c *gin.Context) {
	var req SignupNotifyRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"code": 0,
			"msg":  "参数解析失败",
		})
		return
	}

	req.Table = strings.TrimSpace(req.Table)
	if req.GameID <= 0 || req.Table == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"code": 0,
			"msg":  "缺少必要的报名信息",
		})
		return
	}

	if globalHub == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"code": 0,
			"msg":  "服务尚未就绪",
		})
		return
	}

	log.Printf("收到HTTP报名通知: 游戏ID=%d, 桌号=%s, 报名ID=%d", req.GameID, req.Table, req.SignupID)

	go globalHub.notifySignupUpdate(req.GameID)

	c.JSON(http.StatusOK, gin.H{
		"code": 1,
		"msg":  "success",
		"data": map[string]interface{}{
			"game_id":   req.GameID,
			"table":     req.Table,
			"signup_id": req.SignupID,
		},
	})
}

func main() {
	// 初始化随机数种子
	rand.Seed(time.Now().UnixNano())

	// 加载环境变量
	if err := godotenv.Load("config.env"); err != nil {
		log.Println("未找到config.env文件")
	}

	// 初始化数据库
	if err := initDatabase(); err != nil {
		log.Fatal("数据库初始化失败:", err)
	}
	defer closeDatabase()

	// 创建Hub
	hub := newHub()
	globalHub = hub
	go hub.run()

	// 启动定时任务
	// 1. 定时检查第一个消息桌号（每3秒检查一次）
	go func() {
		ticker := time.NewTicker(3 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			broadcastFirstMessageTable(hub)
		}
	}()

	// 2. 定时广播消息排行榜（每10秒广播一次）
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			broadcastMessageRanking(hub)
		}
	}()

	// 创建Gin路由
	r := gin.Default()

	// 静态文件服务
	r.Static("/static", "./static")
	r.LoadHTMLGlob("templates/*")

	// WebSocket路由
	r.GET("/ws", func(c *gin.Context) {
		handleWebSocket(hub, c)
	})

	// API路由
	r.GET("/api/game/:id", handleGetGameInfoAPI)
	r.POST("/api/signup/notify", handleSignupNotifyAPI)

	// 页面路由
	r.GET("/control", func(c *gin.Context) {
		// 无游戏ID时，自动获取最新游戏
		latestGame, err := getLatestUnfinishedGame()
		if err != nil {
			log.Printf("获取最新未完成游戏失败: %v", err)
			c.HTML(http.StatusOK, "control.html", gin.H{
				"has_game_id": false,
				"game_id":     nil,
			})
		} else {
			c.HTML(http.StatusOK, "control.html", gin.H{
				"has_game_id": true,
				"game_id":     latestGame.ID,
			})
		}
	})

	r.GET("/control/:game_id", func(c *gin.Context) {
		gameID := c.Param("game_id")
		c.HTML(http.StatusOK, "control.html", gin.H{
			"has_game_id": true,
			"game_id":     gameID,
		})
	})

	r.GET("/game", func(c *gin.Context) {
		c.HTML(http.StatusOK, "game.html", gin.H{})
	})

	// 启动服务器
	port := os.Getenv("PORT")
	if port == "" {
		port = "8061"
	}

	log.Printf("WebSocket服务启动在端口 %s", port)
	log.Fatal(r.Run(":" + port))
}

// 数据库连接
var db *sql.DB

// 初始化数据库连接
func initDatabase() error {
	host := getEnv("DB_HOST", "127.0.0.1")
	port := getEnv("DB_PORT", "3306")
	user := getEnv("DB_USER", "bar_new")
	password := getEnv("DB_PASSWORD", "Ba7LxFLjPmX622LF")
	dbName := getEnv("DB_NAME", "bar_new")

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8mb4&parseTime=True&loc=Local",
		user, password, host, port, dbName)

	var err error
	db, err = sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("连接数据库失败: %v", err)
	}

	// 测试连接
	if err := db.Ping(); err != nil {
		return fmt.Errorf("数据库连接测试失败: %v", err)
	}

	// 设置连接池参数
	db.SetMaxOpenConns(100)
	db.SetMaxIdleConns(10)
	db.SetConnMaxLifetime(time.Hour)

	log.Println("数据库连接成功")
	return nil
}

// 获取环境变量
func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

// 游戏记录结构
type GameRecord struct {
	ID         int       `json:"id"`
	TopicID    int       `json:"topic_id"`
	Link       string    `json:"link"`
	Status     int       `json:"status"`
	Content    string    `json:"content"`
	CreateTime time.Time `json:"create_time"`
	StartTime  int       `json:"start_time"`
	IsCustom   int       `json:"is_custom"`
}

// 游戏主题结构
type GameTopic struct {
	ID      int    `json:"id"`
	Name    string `json:"name"`
	Content string `json:"content"`
	Status  int    `json:"status"`
}

// 报名记录结构
type SignupRecord struct {
	ID       int    `json:"id"`
	GameID   int    `json:"game_id"`
	Table    string `json:"table"`
	ClientID int    `json:"client_id"`
}

// 机器桌号映射记录结构
type MachineTableRecord struct {
	ID        int    `json:"id"`
	MachineSN string `json:"machine_sn"`
	TableSN   string `json:"table_sn"`
	ImageURL  string `json:"image_url"`
	Sort      int    `json:"sort"`
	Remark    string `json:"remark"`
	Qrcode    string `json:"qrcode"`
	EditUID   int    `json:"edit_uid"`
	Type      int    `json:"type"`
	MaxUser   int    `json:"max_user"`
}

// 消息记录结构
type MessageRecord struct {
	ID         int       `json:"id"`
	Table      string    `json:"table"`
	Content    string    `json:"content"`
	CreateTime time.Time `json:"create_time"`
}

// 消息统计结构
type MessageStats struct {
	Table        string `json:"table"`
	MessageCount int    `json:"message_count"`
}

// 注意：已移除每日只广播一次的限制，现在每3秒都会广播第一个消息桌号

// 获取游戏信息
func getGameInfo(gameID int) (*GameRecord, error) {
	query := "SELECT id, topic_id, link, status, content, create_time, start_time, is_custom FROM ls_game WHERE id = ?"

	var game GameRecord
	err := db.QueryRow(query, gameID).Scan(
		&game.ID, &game.TopicID, &game.Link, &game.Status, &game.Content, &game.CreateTime, &game.StartTime, &game.IsCustom,
	)

	if err != nil {
		return nil, err
	}

	return &game, nil
}

// 获取最新的未完成游戏
func getLatestUnfinishedGame() (*GameRecord, error) {
	query := "SELECT id, topic_id, link, status, content, create_time, start_time, is_custom FROM ls_game WHERE status IN (0, 1, 2, 3) ORDER BY id DESC LIMIT 1"

	var game GameRecord
	err := db.QueryRow(query).Scan(
		&game.ID, &game.TopicID, &game.Link, &game.Status, &game.Content, &game.CreateTime, &game.StartTime, &game.IsCustom,
	)

	if err != nil {
		return nil, err
	}

	return &game, nil
}

// 更新游戏状态
func updateGameStatus(gameID int, status int) error {
	query := "UPDATE ls_game SET status = ? WHERE id = ?"
	_, err := db.Exec(query, status, gameID)
	return err
}

// 更新游戏状态和开始时间
func updateGameStatusAndStartTime(gameID int, status int, startTime int) error {
	query := "UPDATE ls_game SET status = ?, start_time = ? WHERE id = ?"
	_, err := db.Exec(query, status, startTime, gameID)
	return err
}

// 计算剩余时间（返回分钟:秒格式的字符串）
func calculateRemainingTime(endTime int) string {
	now := int(time.Now().Unix())
	remaining := endTime - now

	if remaining <= 0 {
		return ""
	}

	minutes := remaining / 60
	seconds := remaining % 60

	return fmt.Sprintf("%02d:%02d", minutes, seconds)
}

// 更新游戏内容
func updateGameContent(gameID int, content string) error {
	query := "UPDATE ls_game SET content = ? WHERE id = ?"
	_, err := db.Exec(query, content, gameID)
	return err
}

// 添加报名记录
func addSignupRecord(gameID int, table string, clientID int) error {
	query := "INSERT INTO ls_game_sign (game_id, `table`, client_id) VALUES (?, ?, ?)"
	_, err := db.Exec(query, gameID, table, clientID)
	return err
}

// 获取报名列表
func getSignupList(gameID int) ([]SignupRecord, error) {
	query := "SELECT id, game_id, `table`, client_id FROM ls_game_sign WHERE game_id = ? ORDER BY id ASC"

	rows, err := db.Query(query, gameID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var signups []SignupRecord
	for rows.Next() {
		var signup SignupRecord
		err := rows.Scan(&signup.ID, &signup.GameID, &signup.Table, &signup.ClientID)
		if err != nil {
			return nil, err
		}
		signups = append(signups, signup)
	}

	return signups, nil
}

// 删除报名记录
func deleteSignupRecord(id int) error {
	query := "DELETE FROM ls_game_sign WHERE id = ?"
	_, err := db.Exec(query, id)
	return err
}

// 根据游戏ID和桌号删除报名记录
func deleteSignupByTableAndGame(gameID int, table string) error {
	query := "DELETE FROM ls_game_sign WHERE game_id = ? AND `table` = ?"
	_, err := db.Exec(query, gameID, table)
	return err
}

// 清空游戏报名记录
func clearGameSignups(gameID int) error {
	query := "DELETE FROM ls_game_sign WHERE game_id = ?"
	_, err := db.Exec(query, gameID)
	return err
}

// 游戏内容结构
type GameContent struct {
	Table1            string `json:"table1"`
	Table2            string `json:"table2"`
	Winner            string `json:"winner"`
	GoodsID           string `json:"goods_id"`
	PrizeName         string `json:"prize_name"`
	GameType          string `json:"game_type"`
	Rounds            int    `json:"rounds"`
	Result            string `json:"result"`
	GoodsName         string `json:"goods_name"`
	GoodsPrice        string `json:"goods_price"`
	PrizeSelectedTime string `json:"prize_selected_time"`
}

// 更新游戏内容（JSON格式）
func updateGameContentJSON(gameID int, content GameContent) error {
	contentJSON, err := json.Marshal(content)
	if err != nil {
		return err
	}

	return updateGameContent(gameID, string(contentJSON))
}

// 获取游戏内容
func getGameContent(gameID int) (*GameContent, error) {
	game, err := getGameInfo(gameID)
	if err != nil {
		return nil, err
	}

	var content GameContent
	if game.Content != "" {
		err = json.Unmarshal([]byte(game.Content), &content)
		if err != nil {
			return nil, err
		}
	}

	return &content, nil
}

// 根据主题ID获取游戏主题信息
func getGameTopicByID(topicID int) (*GameTopic, error) {
	query := "SELECT id, name, content, status FROM ls_game_topic WHERE id = ? LIMIT 1"

	var topic GameTopic
	err := db.QueryRow(query, topicID).Scan(&topic.ID, &topic.Name, &topic.Content, &topic.Status)
	if err != nil {
		return nil, err
	}

	return &topic, nil
}

// 根据机器编号获取桌号
func getTableByMachineSN(machineSN string) (string, error) {
	query := "SELECT table_sn FROM ls_machine_table WHERE machine_sn = ? LIMIT 1"

	var tableSN string
	err := db.QueryRow(query, machineSN).Scan(&tableSN)

	if err != nil {
		return "", err
	}

	return tableSN, nil
}

// 关闭数据库连接
func closeDatabase() {
	if db != nil {
		db.Close()
	}
}

// 获取游戏开始后第一个发送并收到小纸条的桌号
func getFirstMessageTableInGame() (string, error) {
	// 检查游戏是否激活
	if !firstMessageGameState.isActive {
		return "", fmt.Errorf("游戏未激活")
	}

	// 使用游戏开始时间和结束时间作为查询范围
	startTime := firstMessageGameState.startTime
	endTime := firstMessageGameState.endTime

	// 将时间转换为字符串格式，确保与数据库格式一致
	startTimeStr := startTime.Format("2006-01-02 15:04:05")
	endTimeStr := endTime.Format("2006-01-02 15:04:05")

	// 第一步：查询在5分钟内发送过小纸条的桌号
	senderQuery := `SELECT DISTINCT from_table_sn FROM ls_chat_msg 
					WHERE created_at >= ? AND created_at < ?`

	senderRows, err := db.Query(senderQuery, startTimeStr, endTimeStr)
	if err != nil {
		return "", err
	}
	defer senderRows.Close()

	// 收集所有发送过小纸条的桌号
	var senderTables []string
	for senderRows.Next() {
		var table string
		if err := senderRows.Scan(&table); err != nil {
			return "", err
		}
		senderTables = append(senderTables, table)
	}

	// 如果没有发送者，直接返回空
	if len(senderTables) == 0 {
		return "", nil
	}

	// 第二步：查询接收小纸条的记录，从这些发送者中找接收时间最早的
	receiverQuery := `SELECT to_table_sn FROM ls_chat_msg 
					  WHERE to_table_sn IN (` + fmt.Sprintf("'%s'", strings.Join(senderTables, "','")) + `)
					  AND is_recv = 1 AND created_at >= ? AND created_at < ?
					  ORDER BY created_at ASC LIMIT 1`

	var receiverTable string
	err = db.QueryRow(receiverQuery, startTimeStr, endTimeStr).Scan(&receiverTable)
	if err != nil {
		return "", err
	}

	return receiverTable, nil
}

// 获取当天所有桌的消息数量统计
func getMessageStatsToday() ([]MessageStats, error) {
	// 获取当天中午12点到次日中午12点的时间范围
	now := time.Now()
	today := now.Format("2006-01-02")

	// 当天中午12点
	startTime, err := time.Parse("2006-01-02 15:04:05", today+" 12:00:00")
	if err != nil {
		return nil, err
	}

	// 次日中午12点
	endTime := startTime.Add(24 * time.Hour)

	query := `SELECT to_table_sn, COUNT(*) as message_count 
			  FROM ls_chat_msg 
			  WHERE created_at >= ? AND created_at < ? 
			  GROUP BY to_table_sn 
			  ORDER BY message_count DESC`

	// 将时间转换为字符串格式，确保与数据库格式一致
	startTimeStr := startTime.Format("2006-01-02 15:04:05")
	endTimeStr := endTime.Format("2006-01-02 15:04:05")

	rows, err := db.Query(query, startTimeStr, endTimeStr)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var stats []MessageStats
	for rows.Next() {
		var stat MessageStats
		err := rows.Scan(&stat.Table, &stat.MessageCount)
		if err != nil {
			return nil, err
		}
		stats = append(stats, stat)
	}

	return stats, nil
}

// 注意：已移除日期检查功能，现在每3秒都会广播第一个消息桌号
 
// 获取第一个小纸条游戏的奖品信息
func getFirstMessagePrizeInfo() (string, error) {
	query := "SELECT content FROM ls_game_topic WHERE id = 4"

	var content string
	err := db.QueryRow(query).Scan(&content)
	if err != nil {
		return "", err
	}

	// 解析JSON内容
	var contentData map[string]interface{}
	if err := json.Unmarshal([]byte(content), &contentData); err != nil {
		return "", err
	}

	// 获取奖品信息
	if prize, ok := contentData["prize"].(map[string]interface{}); ok {
		goodsName, hasName := prize["goods_name"].(string)
		goodsPrice, hasPrice := prize["goods_price"].(string)

		if hasName && hasPrice && goodsName != "" && goodsPrice != "" {
			return fmt.Sprintf("「 价值 %s 元的 %s 」随机两桌石头剪刀布！", goodsPrice, goodsName), nil
		} else if hasName && goodsName != "" {
			return goodsName, nil
		}
	}

	return "", nil
}

// 计算第一个小纸条游戏的倒计时
func getFirstMessageCountdown() string {
	if !firstMessageGameState.isActive || !firstMessageGameState.isCountdown {
		return ""
	}

	now := time.Now()
	if now.After(firstMessageGameState.endTime) {
		// 倒计时结束，停止游戏状态
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

// 广播第一个消息桌号给控制台
func broadcastFirstMessageTable(hub *Hub) {
	// 只有在第一个小纸条游戏激活时才广播
	if !firstMessageGameState.isActive {
		return
	}

	// 如果已经广播过，不再广播
	if firstMessageGameState.hasBroadcast {
		return
	}

	// 检查是否有控制台客户端连接
	hasControlClient := false
	hub.mutex.RLock()
	for client := range hub.clients {
		if client.Type == ControlClient {
			hasControlClient = true
			break
		}
	}
	hub.mutex.RUnlock()

	// 如果没有控制台客户端，不进行广播
	if !hasControlClient {
		return
	}

	// 获取游戏开始后第一个发送并收到小纸条的桌号
	table, err := getFirstMessageTableInGame()
	if err != nil {
		log.Printf("获取第一个消息桌号失败: %v", err)
		return
	}

	// 如果没有找到桌号，不广播
	if table == "" {
		return
	}

	// 获取奖品信息
	prizeName, err := getFirstMessagePrizeInfo()
	if err != nil {
		log.Printf("获取奖品信息失败: %v", err)
		prizeName = "" // 设置为空字符串，不影响广播
	}

	// 构建广播数据（移除倒计时字段）
	broadcastData := map[string]interface{}{
		"table":     table,
		"message":   fmt.Sprintf("游戏开始后第一个发送消息的是桌号: %s", table),
		"timestamp": time.Now().Unix(),
	}

	// 如果有奖品信息，添加到广播数据中
	if prizeName != "" {
		broadcastData["prize_name"] = prizeName
	}

	// 广播给所有控制台客户端
	message := Message{
		Type: "first_message_table",
		Data: broadcastData,
	}

	hub.mutex.RLock()
	for client := range hub.clients {
		if client.Type == ControlClient {
			client.sendMessage(message)
		}
	}
	hub.mutex.RUnlock()

	// 标记已经广播过
	firstMessageGameState.hasBroadcast = true

	log.Printf("已广播游戏开始后第一个消息桌号: %s, 奖品: %s", table, prizeName)
}

// 广播消息排行榜给控制台
func broadcastMessageRanking(hub *Hub) {
	// 检查是否有控制台客户端连接
	hasControlClient := false
	hub.mutex.RLock()
	for client := range hub.clients {
		if client.Type == ControlClient {
			hasControlClient = true
			break
		}
	}
	hub.mutex.RUnlock()

	// 如果没有控制台客户端，不进行广播
	if !hasControlClient {
		return
	}

	// 获取当天消息统计
	stats, err := getMessageStatsToday()
	if err != nil {
		log.Printf("获取消息统计失败: %v", err)
		return
	}

	// 如果没有消息，不广播
	if len(stats) == 0 {
		return
	}

	// 广播给所有控制台客户端
	message := Message{
		Type: "message_ranking",
		Data: map[string]interface{}{
			"ranking":   stats,
			"message":   "今日消息排行榜",
			"timestamp": time.Now().Unix(),
		},
	}

	hub.mutex.RLock()
	for client := range hub.clients {
		if client.Type == ControlClient {
			client.sendMessage(message)
		}
	}
	hub.mutex.RUnlock()

	log.Printf("已广播消息排行榜，条目数: %d", len(stats))
}
