package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

// 数据库连接
var db *sql.DB

// 初始化数据库连接
func initDatabase() error {
	host := getEnv("DB_HOST", "localhost")
	port := getEnv("DB_PORT", "3306")
	user := getEnv("DB_USER", "root")
	password := getEnv("DB_PASSWORD", "")
	dbName := getEnv("DB_NAME", "game_db")

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
}

// 报名记录结构
type SignupRecord struct {
	ID       int    `json:"id"`
	GameID   int    `json:"game_id"`
	Table    string `json:"table"`
	ClientID int    `json:"client_id"`
}

// 获取游戏信息
func getGameInfo(gameID int) (*GameRecord, error) {
	query := "SELECT id, topic_id, link, status, content, create_time FROM ls_game WHERE id = ?"

	var game GameRecord
	err := db.QueryRow(query, gameID).Scan(
		&game.ID, &game.TopicID, &game.Link, &game.Status, &game.Content, &game.CreateTime,
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

// 关闭数据库连接
func closeDatabase() {
	if db != nil {
		db.Close()
	}
}
