# play_game 服务（8061）

游戏控台 WebSocket + H5 页面，与 jiuba 主站通过 HTTP 互通。

## 环境

| APP_ENV | 配置文件 | 说明 |
|---------|----------|------|
| `test`（默认） | `config.env.test` | 测试机 `39.97.39.66`，API 回调 `house.makoo.xyz` |
| `prod` | `config.env.prod` | 正式机 `182.92.153.92`，API 回调 `kscy.crushclub.love` |

复制 `config.env.example`，填写数据库密码后保存为 `config.env.test` / `config.env.prod`（已在 `.gitignore`）。

## 启动

```bash
# 测试环境（默认）
make run-test
# 或
APP_ENV=test go run .

# 正式环境
make run-prod
```

## 与 jiuba 对齐

- jiuba `.env` 中 `domain.env = test` 时，`play_game` 指向 `http://39.97.39.66:8061`
- overlay 广播：`POST /api/game_overlay/broadcast`（JSON）
- DJ 开始/暂停/结束视频：WS `start_game_communication` / `pause_game_communication` / `end_game_communication` → 回调 jiuba `sessionStart`、`sessionPause`、`sessionEnd`；展示完成：`showDone`
