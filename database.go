package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

var db *sql.DB

func initDatabase() error {
	host := getEnv("DB_HOST", "127.0.0.1")
	port := getEnv("DB_PORT", "3306")
	user := getEnv("DB_USER", "root")
	password := getEnv("DB_PASSWORD", "")
	dbName := getEnv("DB_NAME", "bar")

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8mb4&parseTime=True&loc=Local",
		user, password, host, port, dbName)

	var err error
	db, err = sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("连接数据库失败: %v", err)
	}

	if err := db.Ping(); err != nil {
		return fmt.Errorf("数据库连接测试失败: %v", err)
	}

	db.SetMaxOpenConns(100)
	db.SetMaxIdleConns(10)
	db.SetConnMaxLifetime(time.Hour)

	log.Println("数据库连接成功")
	return nil
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

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

type GameTopic struct {
	ID      int    `json:"id"`
	Name    string `json:"name"`
	Content string `json:"content"`
	Status  int    `json:"status"`
}

type SignupRecord struct {
	ID       int    `json:"id"`
	GameID   int    `json:"game_id"`
	Table    string `json:"table"`
	ClientID int    `json:"client_id"`
}

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

type MessageRecord struct {
	ID         int       `json:"id"`
	Table      string    `json:"table"`
	Content    string    `json:"content"`
	CreateTime time.Time `json:"create_time"`
}

type MessageStats struct {
	Table        string `json:"table"`
	MessageCount int    `json:"message_count"`
}

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

func getGameStoreSid(gameID int) (int, error) {
	query := "SELECT selffetch_shop_id FROM ls_game WHERE id = ? LIMIT 1"
	var storeSid int
	err := db.QueryRow(query, gameID).Scan(&storeSid)
	if err != nil {
		return 0, err
	}
	return storeSid, nil
}

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

func updateGameStatus(gameID int, status int) error {
	query := "UPDATE ls_game SET status = ? WHERE id = ?"
	_, err := db.Exec(query, status, gameID)
	return err
}

func updateGameStatusAndStartTime(gameID int, status int, startTime int) error {
	query := "UPDATE ls_game SET status = ?, start_time = ? WHERE id = ?"
	_, err := db.Exec(query, status, startTime, gameID)
	return err
}

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

func updateGameContent(gameID int, content string) error {
	query := "UPDATE ls_game SET content = ? WHERE id = ?"
	_, err := db.Exec(query, content, gameID)
	return err
}

func addSignupRecord(gameID int, table string, clientID int) error {
	query := "INSERT INTO ls_game_sign (game_id, `table`, client_id) VALUES (?, ?, ?)"
	_, err := db.Exec(query, gameID, table, clientID)
	return err
}

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

func deleteSignupRecord(id int) error {
	query := "DELETE FROM ls_game_sign WHERE id = ?"
	_, err := db.Exec(query, id)
	return err
}

func deleteSignupByTableAndGame(gameID int, table string) error {
	query := "DELETE FROM ls_game_sign WHERE game_id = ? AND `table` = ?"
	_, err := db.Exec(query, gameID, table)
	return err
}

func clearGameSignups(gameID int) error {
	query := "DELETE FROM ls_game_sign WHERE game_id = ?"
	_, err := db.Exec(query, gameID)
	return err
}

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

func updateGameContentJSON(gameID int, content GameContent) error {
	contentJSON, err := json.Marshal(content)
	if err != nil {
		return err
	}

	return updateGameContent(gameID, string(contentJSON))
}

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

func getGameTopicByID(topicID int) (*GameTopic, error) {
	query := "SELECT id, name, content, status FROM ls_game_topic WHERE id = ? LIMIT 1"

	var topic GameTopic
	err := db.QueryRow(query, topicID).Scan(&topic.ID, &topic.Name, &topic.Content, &topic.Status)
	if err != nil {
		return nil, err
	}

	return &topic, nil
}

func getTableByMachineSN(machineSN string) (string, error) {
	query := "SELECT table_sn FROM ls_machine_table WHERE machine_sn = ? LIMIT 1"

	var tableSN string
	err := db.QueryRow(query, machineSN).Scan(&tableSN)
	if err != nil {
		return "", err
	}

	return tableSN, nil
}

func closeDatabase() {
	if db != nil {
		db.Close()
	}
}

func getFirstMessageTableInGame() (string, error) {
	if !firstMessageGameState.isActive {
		return "", fmt.Errorf("游戏未激活")
	}

	startTime := firstMessageGameState.startTime
	endTime := firstMessageGameState.endTime

	startTimeStr := startTime.Format("2006-01-02 15:04:05")
	endTimeStr := endTime.Format("2006-01-02 15:04:05")

	senderQuery := `SELECT DISTINCT from_table_sn FROM ls_chat_msg 
					WHERE created_at >= ? AND created_at < ?`

	senderRows, err := db.Query(senderQuery, startTimeStr, endTimeStr)
	if err != nil {
		return "", err
	}
	defer senderRows.Close()

	var senderTables []string
	for senderRows.Next() {
		var table string
		if err := senderRows.Scan(&table); err != nil {
			return "", err
		}
		senderTables = append(senderTables, table)
	}

	if len(senderTables) == 0 {
		return "", nil
	}

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

func getMessageStatsToday() ([]MessageStats, error) {
	now := time.Now()
	today := now.Format("2006-01-02")

	startTime, err := time.Parse("2006-01-02 15:04:05", today+" 12:00:00")
	if err != nil {
		return nil, err
	}

	endTime := startTime.Add(24 * time.Hour)

	query := `SELECT to_table_sn, COUNT(*) as message_count 
			  FROM ls_chat_msg 
			  WHERE created_at >= ? AND created_at < ? 
			  GROUP BY to_table_sn 
			  ORDER BY message_count DESC`

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

func getFirstMessagePrizeInfo() (string, error) {
	query := "SELECT content FROM ls_game_topic WHERE id = 4"

	var content string
	err := db.QueryRow(query).Scan(&content)
	if err != nil {
		return "", err
	}

	var contentData map[string]interface{}
	if err := json.Unmarshal([]byte(content), &contentData); err != nil {
		return "", err
	}

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
