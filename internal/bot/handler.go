package bot

import (
	"context"
	"errors"
	"example.com/max-festival-bot/internal/config"
	"example.com/max-festival-bot/internal/maxapi"
	"example.com/max-festival-bot/internal/storage"
	"example.com/max-festival-bot/internal/storage/db"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"strings"
	"time"
)

type Handler struct {
	Config     config.Config
	Now        func() time.Time
	Membership interface {
		CheckMembership(context.Context, int64, int64) (bool, error)
	}
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
	if action == "promo:get" || strings.HasPrefix(action, "draw:") || action == "profile:chances" {
		if err := storage.LockParticipant(ctx, tx, user.ID); errors.Is(err, pgx.ErrNoRows) {
			return storage.Reply(ctx, tx, key, chat, callback, screen("Нажмите /start, чтобы начать участие."))
		} else if err != nil {
			return err
		}
	}
	if action == "promo:get" || strings.HasPrefix(action, "draw:") {
		return h.participate(ctx, tx, key, chat, callback, user.ID, action)
	}
	var body maxapi.Body
	switch action {
	case "menu":
		body = h.Menu()
	case "info:how_to":
		body = screen(h.Config.HowTo)
	case "info:rules":
		body = screen(h.Config.Rules)
	case "profile:chances":
		text, err := storage.ChanceProfile(ctx, tx, user.ID, h.instant().In(h.Config.Location).Format(time.DateOnly))
		if err != nil {
			return err
		}
		body = screen(text)
	default:
		body = screen("Нажмите /start, чтобы открыть меню.")
	}
	return storage.Reply(ctx, tx, key, chat, callback, body)
}

func (h Handler) instant() time.Time {
	if h.Now != nil {
		return h.Now()
	}
	return time.Now()
}

func participationScreen(text string, p db.Partner) maxapi.Body {
	return maxapi.Body{Text: text, Attachments: []maxapi.Attachment{maxapi.Keyboard(
		maxapi.Button{Type: "link", Text: "Подписаться на " + p.Name, URL: p.Link},
		maxapi.Callback("Проверить подписку", fmt.Sprintf("draw:verify:%d", p.ID)), maxapi.Callback("В меню", "menu"))}}
}

func (h Handler) participate(ctx context.Context, tx pgx.Tx, key string, chat int64, callback string, user int64, action string) error {
	f := storage.Festival{Location: h.Config.Location, Now: h.Now}
	day, err := f.Current(ctx, db.New(tx))
	if err != nil {
		return err
	}
	reply := func(body maxapi.Body) error { return storage.Reply(ctx, tx, key, chat, callback, body) }
	if action == "promo:get" {
		var partner any
		if day.Partner != nil {
			partner = day.Partner.ID
		}
		if _, err = tx.Exec(ctx, `INSERT INTO promo_requests(event_key,user_id,partner_id) VALUES($1,$2,$3) ON CONFLICT DO NOTHING`, key, user, partner); err != nil {
			return err
		}
	}
	if day.Partner == nil {
		return reply(screen("Сегодня нет активного партнера фестиваля. Получение промокода и регистрация в розыгрыше недоступны."))
	}
	p := *day.Partner
	if action == "promo:get" {
		code, err := storage.IssuePromo(ctx, tx, user, p, day.Instant, h.Config.Location)
		if err != nil {
			return err
		}
		text := fmt.Sprintf("Промокоды от %s уже закончились — их разобрали быстрее всех 🔥\nНо ты все еще можешь подписаться на партнера и принять участие в розыгрыше призов дня.", p.Name)
		if code != "" {
			text = fmt.Sprintf("Твой фестивальный бонус от %s готов 🎁\nПромокод: %s\nИспользуй его до %s.\n%s\nТеперь можешь принять участие в розыгрыше призов дня.", p.Name, code, p.PromoExpiresAt.Time.In(h.Config.Location).Format("02.01.2006 15:04"), p.PromoInstructions)
		}
		return reply(maxapi.Body{Text: text, Attachments: []maxapi.Attachment{maxapi.Keyboard(maxapi.Callback("Участвовать в розыгрыше", "draw:enter"), maxapi.Callback("В меню", "menu"))}})
	}
	if action != "draw:enter" && action != "draw:verify" && action != fmt.Sprintf("draw:verify:%d", p.ID) {
		return reply(screen("Эта кнопка устарела. Открой участие в сегодняшнем розыгрыше из меню."))
	}
	entry, err := storage.Entry(ctx, tx, user, p.ID)
	if err == nil {
		n, err := storage.ChanceCount(ctx, tx, user)
		if err != nil {
			return err
		}
		return storage.Confirmation(ctx, tx, key, chat, callback, screen(fmt.Sprintf("Ты уже участвуешь в сегодняшнем розыгрыше ✅\nРезультаты сообщим после подведения итогов.\nНакоплено шансов: %d из 10", n)), entry)
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return err
	}
	if !day.RegistrationOpen {
		return reply(screen("Прием заявок на сегодняшний розыгрыш закрыт."))
	}
	if action == "draw:enter" {
		return reply(participationScreen("Чтобы попасть в розыгрыш призов от "+p.Name+", подпишись на аккаунт партнера.", p))
	}
	if h.Membership == nil {
		return reply(participationScreen("Не удалось проверить подписку из-за технической ошибки. Попробуй еще раз позже.", p))
	}
	member, err := h.Membership.CheckMembership(ctx, p.ChatID, user)
	if err != nil {
		return reply(participationScreen("Не удалось проверить подписку из-за технической ошибки. Попробуй еще раз позже.", p))
	}
	// The API call (including rate limiting) may cross midnight or the closing time.
	current, err := f.Current(ctx, db.New(tx))
	if err != nil {
		return err
	}
	if current.Partner == nil || current.Partner.ID != p.ID || !current.RegistrationOpen {
		return reply(screen("Прием заявок на этот розыгрыш закрыт или фестивальный день сменился. Вернись в меню."))
	}
	if !member {
		return reply(participationScreen("Пока не вижу подписку 👀\nПодпишись на аккаунт "+p.Name+", вернись в бот и попробуй еще раз.", p))
	}
	entry, n, err := storage.RegisterDaily(ctx, tx, user, p, current.Instant, h.Config.Location)
	if err != nil {
		return err
	}
	return storage.Confirmation(ctx, tx, key, chat, callback, screen(fmt.Sprintf("Ты в лайнапе ✅\nЗаявка на розыгрыш призов от %s зарегистрирована.\nВозвращайся завтра — на сцену выйдет новый партнер.\nНакоплено шансов: %d из 10", p.Name, n)), entry)
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
