package bot

import (
	"context"
	"example.com/max-festival-bot/internal/config"
	"example.com/max-festival-bot/internal/maxapi"
	"example.com/max-festival-bot/internal/storage"
	"example.com/max-festival-bot/internal/storage/db"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"strings"
	"time"
)

type Handler struct {
	Config config.Config
	Now    func() time.Time
}

func (h Handler) Handle(ctx context.Context, tx pgx.Tx, key string, u maxapi.Update) error {
	if u.Type != "bot_started" && u.Type != "message_created" && u.Type != "message_callback" {
		return nil
	}
	user, chat := u.Actor()
	callback := ""
	action := ""
	text := strings.TrimSpace(u.Message.Body.Text)
	if u.Type == "bot_started" || (u.Type == "message_created" && text == "/start") {
		first := user.FirstName
		if first == "" {
			first = user.Name
		}
		var started pgtype.Timestamptz
		if u.Timestamp > 0 {
			started = pgtype.Timestamptz{Time: time.UnixMilli(u.Timestamp), Valid: true}
		}
		if err := db.New(tx).UpsertUser(ctx, db.UpsertUserParams{ID: user.ID, FirstName: first, LastName: user.LastName, Username: user.Username, FirstStartedAt: started}); err != nil {
			return err
		}
		action = "menu"
	} else if u.Type == "message_callback" {
		callback = u.Callback.ID
		action = u.Callback.Payload
	} else if strings.HasPrefix(text, "/") {
		command, _, _ := strings.Cut(text, " ")
		if command == "/import" || command == "/admin" {
			if !h.Config.IsAdmin(user.ID, chat) {
				return storage.Reply(ctx, tx, key, chat, "", maxapi.Body{Text: "Доступ запрещен."})
			}
			if command == "/admin" {
				return storage.Reply(ctx, tx, key, chat, "", maxapi.Body{Text: "Импорт: /import {\"partners\":[...]}. Формат — в README."})
			}
			raw := strings.TrimSpace(strings.TrimPrefix(text, "/import"))
			data, err := storage.ParseImport([]byte(raw), h.Config.Location)
			if err != nil {
				return storage.Reply(ctx, tx, key, chat, "", maxapi.Body{Text: "Импорт отклонен: " + err.Error()})
			}
			// Savepoint allows an import error reply without committing partial data.
			nested, err := tx.Begin(ctx)
			if err != nil {
				return err
			}
			if err = storage.ImportPartners(ctx, nested, data); err != nil {
				if e := nested.Rollback(ctx); e != nil {
					return e
				}
				return storage.Reply(ctx, tx, key, chat, "", maxapi.Body{Text: "Импорт отклонен: проверьте даты и дубли."})
			}
			if err = nested.Commit(ctx); err != nil {
				return err
			}
			return storage.Reply(ctx, tx, key, chat, "", maxapi.Body{Text: "Партнеры и промокоды импортированы."})
		}
	}
	var body maxapi.Body
	switch action {
	case "menu":
		body = h.Menu()
	case "info:how_to":
		body = screen(h.Config.HowTo)
	case "info:rules":
		body = screen(h.Config.Rules)
	case "promo:get", "draw:enter", "draw:verify":
		day, err := (storage.Festival{Location: h.Config.Location, Now: h.Now}).Current(ctx, db.New(tx))
		if err != nil {
			return err
		}
		switch {
		case day.Partner == nil:
			body = screen("Сегодня нет активного партнера фестиваля. Получение промокода и регистрация в розыгрыше недоступны.")
		case action != "promo:get" && !day.RegistrationOpen:
			body = screen("Прием заявок на сегодняшний розыгрыш закрыт.")
		default:
			body = screen("Этот раздел пока недоступен. Он откроется после запуска акции.")
		}
	case "profile:chances":
		body = screen("Этот раздел пока недоступен. Он откроется после запуска акции.")
	default:
		body = screen("Нажмите /start, чтобы открыть меню.")
	}
	return storage.Reply(ctx, tx, key, chat, callback, body)
}
func screen(text string) maxapi.Body {
	return maxapi.Body{Text: text, Attachments: []maxapi.Attachment{maxapi.Keyboard(maxapi.Callback("В меню", "menu"))}}
}
func (h Handler) Menu() maxapi.Body {
	b := maxapi.Body{Text: "Добро пожаловать в Бот розыгрышей"}
	if h.Config.BannerToken != "" {
		b.Attachments = append(b.Attachments, maxapi.Attachment{Type: "image", Payload: map[string]string{"token": h.Config.BannerToken}})
	} else if h.Config.BannerURL != "" {
		b.Attachments = append(b.Attachments, maxapi.Attachment{Type: "image", Payload: map[string]string{"url": h.Config.BannerURL}})
	}
	b.Attachments = append(b.Attachments, maxapi.Keyboard(
		maxapi.Callback("Получить промокод", "promo:get"), maxapi.Callback("Участвовать в розыгрыше", "draw:enter"), maxapi.Callback("Как участвовать", "info:how_to"), maxapi.Callback("Мои шансы", "profile:chances"), maxapi.Callback("Правила акции", "info:rules")))
	return b
}
