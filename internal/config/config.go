package config

import (
	"errors"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

type Config struct {
	DatabaseURL, RedisURL, Token, APIURL, CAFile, Mode, Listen, WebhookSecret, WebhookURL string
	BannerURL, BannerToken, HowTo, Rules                                                  string
	AdminIDs                                                                              map[int64]bool
	AdminChatID                                                                           int64
	Location                                                                              *time.Location
	GrandReserveCount                                                                     *int
	GrandResponseSeconds                                                                  int
}

func Load() (Config, error) {
	c := Config{
		DatabaseURL: env("DATABASE_URL", "postgres://festival:festival@localhost:15432/festival?sslmode=disable"),
		RedisURL:    env("REDIS_URL", "redis://localhost:16379/0"),
		Token:       os.Getenv("MAX_TOKEN"), APIURL: env("MAX_API_URL", "https://platform-api2.max.ru"),
		CAFile: env("MAX_CA_FILE", "certs/russian_trusted_root_ca.crt"), Mode: env("BOT_MODE", "webhook"),
		Listen: env("HTTP_ADDR", ":8080"), WebhookSecret: os.Getenv("WEBHOOK_SECRET"), WebhookURL: os.Getenv("WEBHOOK_URL"),
		BannerURL: os.Getenv("BANNER_URL"), BannerToken: os.Getenv("BANNER_TOKEN"),
		HowTo:    env("HOW_TO_TEXT", "Получите промокод или зарегистрируйтесь в розыгрыше партнера дня. Один активный день дает один шанс, максимум — 10. Главный приз: пять победителей по два билета."),
		Rules:    env("RULES_TEXT", "Правила акции пока готовятся к публикации. Участие станет доступно после их утверждения."),
		AdminIDs: make(map[int64]bool),
	}
	var err error
	if raw := os.Getenv("GRAND_RESPONSE_SECONDS"); raw != "" {
		c.GrandResponseSeconds, err = strconv.Atoi(raw)
		if err != nil || c.GrandResponseSeconds <= 0 || c.GrandResponseSeconds > 2147483647 {
			return c, errors.New("GRAND_RESPONSE_SECONDS must be 1..2147483647")
		}
	}
	if raw := os.Getenv("GRAND_RESERVE_COUNT"); raw != "" {
		n, e := strconv.Atoi(raw)
		if e != nil || n < 0 || n > 10000 {
			return c, errors.New("GRAND_RESERVE_COUNT must be 0..10000")
		}
		c.GrandReserveCount = &n
	}
	c.Location, err = time.LoadLocation(env("FESTIVAL_TIMEZONE", "Europe/Moscow"))
	if err != nil {
		return c, errors.New("invalid FESTIVAL_TIMEZONE")
	}
	if raw := os.Getenv("ADMIN_CHAT_ID"); raw != "" {
		c.AdminChatID, err = strconv.ParseInt(raw, 10, 64)
		if err != nil || c.AdminChatID == 0 {
			return c, errors.New("invalid ADMIN_CHAT_ID")
		}
	}
	if raw := os.Getenv("ADMIN_IDS"); raw != "" {
		for _, s := range strings.Split(raw, ",") {
			id, e := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
			if e != nil || id <= 0 {
				return c, errors.New("invalid ADMIN_IDS")
			}
			c.AdminIDs[id] = true
		}
	}
	if c.Mode != "webhook" && c.Mode != "polling" {
		return c, errors.New("BOT_MODE must be webhook or polling")
	}
	if c.Mode == "webhook" && (len(c.WebhookSecret) < 16 || len(c.WebhookSecret) > 256 || strings.ContainsAny(c.WebhookSecret, "\r\n")) {
		return c, errors.New("WEBHOOK_SECRET must have 16..256 characters without newlines")
	}
	if c.WebhookURL != "" && !HTTPS(c.WebhookURL) {
		return c, errors.New("WEBHOOK_URL must be HTTPS")
	}
	if c.BannerURL != "" && !HTTPS(c.BannerURL) {
		return c, errors.New("BANNER_URL must be HTTPS")
	}
	if c.BannerURL != "" && c.BannerToken != "" {
		return c, errors.New("set only one of BANNER_URL and BANNER_TOKEN")
	}
	if utf8.RuneCountInString(c.HowTo) > 4000 || utf8.RuneCountInString(c.Rules) > 4000 {
		return c, errors.New("HOW_TO_TEXT and RULES_TEXT must fit MAX's 4000 character limit")
	}
	if c.Token != "" && strings.ContainsAny(c.Token, "\r\n") {
		return c, errors.New("invalid MAX_TOKEN")
	}
	u, e := url.Parse(c.APIURL)
	if e != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Scheme != "https" && !(u.Scheme == "http" && (u.Hostname() == "localhost" || u.Hostname() == "127.0.0.1" || u.Hostname() == "max-mock"))) {
		return c, errors.New("MAX_API_URL must be HTTPS (HTTP only for local simulator)")
	}
	return c, nil
}
func HTTPS(s string) bool {
	u, e := url.Parse(s)
	return e == nil && u.Scheme == "https" && u.Host != "" && u.User == nil
}
func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
func (c Config) IsAdmin(user, chat int64) bool {
	return c.AdminChatID != 0 && chat == c.AdminChatID && c.AdminIDs[user]
}
