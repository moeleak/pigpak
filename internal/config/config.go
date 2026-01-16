package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// Config holds runtime configuration loaded from env vars.
type Config struct {
	BotToken        string
	BotUsername     string
	TelegramAPIURL  string
	DataDir         string
	DBPath          string
	PollTimeout     time.Duration
	PageSize        int
	WebDAVEnable    bool
	WebDAVAddr      string
	WebDAVUser      string
	WebDAVPassword  string
	WebDAVOwnerID   int64
	StorageChatID   int64
	ShareBaseURL    string
}

// Load reads environment variables and applies defaults.
func Load() (Config, error) {
	cfg := Config{}
	cfg.BotToken = strings.TrimSpace(os.Getenv("BOT_TOKEN"))
	if cfg.BotToken == "" {
		return cfg, errors.New("BOT_TOKEN is required")
	}
	cfg.BotUsername = strings.TrimSpace(os.Getenv("BOT_USERNAME"))
	cfg.TelegramAPIURL = strings.TrimSpace(os.Getenv("TELEGRAM_API_URL"))
	if cfg.TelegramAPIURL == "" {
		cfg.TelegramAPIURL = "https://api.telegram.org"
	}
	cfg.DataDir = strings.TrimSpace(os.Getenv("DATA_DIR"))
	if cfg.DataDir == "" {
		cfg.DataDir = "./data"
	}
	cfg.DBPath = strings.TrimSpace(os.Getenv("DB_PATH"))
	if cfg.DBPath == "" {
		cfg.DBPath = filepath.Join(cfg.DataDir, "bot.db")
	}
	cfg.PollTimeout = parseDuration("POLL_TIMEOUT", 30*time.Second)
	cfg.PageSize = parseInt("PAGE_SIZE", 8)

	cfg.WebDAVEnable = parseBool("WEB_DAV_ENABLE", false)
	cfg.WebDAVAddr = strings.TrimSpace(os.Getenv("WEB_DAV_ADDR"))
	if cfg.WebDAVAddr == "" {
		cfg.WebDAVAddr = ":8081"
	}
	cfg.WebDAVUser = strings.TrimSpace(os.Getenv("WEB_DAV_USER"))
	cfg.WebDAVPassword = strings.TrimSpace(os.Getenv("WEB_DAV_PASSWORD"))
	cfg.WebDAVOwnerID = parseInt64("WEB_DAV_OWNER_ID", 0)
	cfg.StorageChatID = parseInt64("STORAGE_CHAT_ID", 0)
	if cfg.WebDAVOwnerID == 0 {
		cfg.WebDAVOwnerID = cfg.StorageChatID
	}
	if cfg.StorageChatID == 0 {
		cfg.StorageChatID = cfg.WebDAVOwnerID
	}

	cfg.ShareBaseURL = strings.TrimSpace(os.Getenv("SHARE_BASE_URL"))
	if cfg.ShareBaseURL == "" && cfg.BotUsername != "" {
		cfg.ShareBaseURL = fmt.Sprintf("https://t.me/%s", cfg.BotUsername)
	}

	return cfg, nil
}

func parseBool(key string, def bool) bool {
	val := strings.TrimSpace(os.Getenv(key))
	if val == "" {
		return def
	}
	val = strings.ToLower(val)
	return val == "1" || val == "true" || val == "yes" || val == "y"
}

func parseInt(key string, def int) int {
	val := strings.TrimSpace(os.Getenv(key))
	if val == "" {
		return def
	}
	parsed, err := strconv.Atoi(val)
	if err != nil {
		return def
	}
	return parsed
}

func parseInt64(key string, def int64) int64 {
	val := strings.TrimSpace(os.Getenv(key))
	if val == "" {
		return def
	}
	parsed, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return def
	}
	return parsed
}

func parseDuration(key string, def time.Duration) time.Duration {
	val := strings.TrimSpace(os.Getenv(key))
	if val == "" {
		return def
	}
	parsed, err := time.ParseDuration(val)
	if err != nil {
		return def
	}
	return parsed
}
