package bot

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"example.com/max-festival-bot/internal/maxapi"
	"example.com/max-festival-bot/internal/storage"
	"github.com/jackc/pgx/v5"
	"github.com/redis/go-redis/v9"
)

type claimDraft struct {
	Winner                              int64
	Chat                                int64
	Step, First, Last, Recipient, Token string
}

func validName(s string) bool {
	if utf8.RuneCountInString(s) < 1 || utf8.RuneCountInString(s) > 100 {
		return false
	}
	letter := false
	for _, r := range s {
		if unicode.IsLetter(r) {
			letter = true
			continue
		}
		if r != ' ' && r != '-' && r != '\'' && r != '’' {
			return false
		}
	}
	return letter
}

func draftBody(d claimDraft) maxapi.Body {
	switch d.Step {
	case "first":
		return screen("Введи свое имя.")
	case "last":
		return screen("Введи свою фамилию.")
	default:
		return maxapi.Body{Text: fmt.Sprintf("Имя: %s\nФамилия: %s\nПолучатель данных: %s\nНажимая «Согласен», ты даешь согласие на передачу указанных имени и фамилии этому получателю для передачи приза.", d.First, d.Last, d.Recipient), Attachments: []maxapi.Attachment{maxapi.Keyboard(maxapi.Callback("Согласен", fmt.Sprintf("claim:consent:%d:%s", d.Winner, d.Token)), maxapi.Callback("Ввести заново", fmt.Sprintf("claim:start:%d", d.Winner)), maxapi.Callback("В меню", "menu"))}}
	}
}

func (h Handler) claim(ctx context.Context, tx pgx.Tx, key string, u maxapi.Update, action, text string) (bool, error) {
	user, chat := u.Actor()
	isClaim := strings.HasPrefix(action, "claim:")
	if !isClaim && (u.Type != "message_created" || strings.HasPrefix(text, "/")) {
		return false, nil
	}
	reply := func(message string) (bool, error) {
		return true, storage.Reply(ctx, tx, key, chat, u.Callback.ID, screen(message))
	}
	if h.Claims == nil {
		if isClaim {
			return reply("Получение приза временно недоступно. Попробуй позже.")
		}
		return false, nil
	}
	// Serializes drafts for one user across bot processes and inbox retries.
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, user.ID); err != nil {
		return true, err
	}
	var private bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM users WHERE id=$1 AND private_chat_id=$2)`, user.ID, chat).Scan(&private); err != nil {
		return true, err
	}
	if !private && u.Message.Recipient.ChatType != "dialog" {
		if isClaim {
			return reply("Открой личный чат с ботом и нажми /start.")
		}
		return false, nil
	}
	session := fmt.Sprintf("claim:user:%d", user.ID)
	event := "claim:event:" + key
	var d claimDraft
	raw, err := h.Claims.Get(ctx, event).Bytes()
	cached := err == nil
	if errors.Is(err, redis.Nil) {
		raw, err = h.Claims.Get(ctx, session).Bytes()
	}
	if err != nil && !errors.Is(err, redis.Nil) {
		return true, err
	}
	if len(raw) > 0 {
		if err = json.Unmarshal(raw, &d); err != nil {
			return true, err
		}
	}
	parts := strings.Split(action, ":")
	start := len(parts) == 3 && parts[0] == "claim" && parts[1] == "start"
	consent := len(parts) == 4 && parts[0] == "claim" && parts[1] == "consent"
	var id int64
	if start || consent {
		id, _ = strconv.ParseInt(parts[2], 10, 64)
	} else {
		id = d.Winner
	}
	if !isClaim && (id == 0 || d.Chat != chat) {
		return false, nil
	}
	if isClaim && ((!start && !consent) || id <= 0) {
		return reply("Эта кнопка получения приза недействительна.")
	}
	c, ok, err := storage.OpenClaim(ctx, tx, id, user.ID, h.Config.GrandDataRecipient)
	if err != nil {
		return true, err
	}
	if !ok {
		return reply("Этот выигрыш недоступен: проверь получателя уведомления и срок ответа.")
	}
	if consent {
		if d.Winner != id || d.Chat != chat || d.Step != "consent" || d.Token != parts[3] || d.Recipient != c.Recipient {
			return reply("Подтверждение устарело. Нажми «Получить приз» и введи данные заново.")
		}
		ok, err = storage.ConfirmClaim(ctx, tx, c, d.First, d.Last)
		if err != nil {
			return true, err
		}
		if !ok {
			return reply("Срок подтверждения приза истек.")
		}
		// Preserve this event for a PG rollback, and finish the active dialogue.
		raw, err = json.Marshal(d)
		if err != nil {
			return true, err
		}
		ttl := time.Until(c.Deadline)
		if ttl <= 0 {
			ttl = time.Second
		}
		_, err = h.Claims.TxPipelined(ctx, func(p redis.Pipeliner) error { p.Set(ctx, event, raw, ttl); p.Del(ctx, session); return nil })
		if err != nil {
			return true, err
		}
		return reply("Получение приза подтверждено. Данные и согласие сохранены.")
	}
	if !cached {
		if start {
			sum := sha256.Sum256([]byte(key))
			d = claimDraft{Winner: id, Chat: chat, Step: "first", Token: hex.EncodeToString(sum[:8]), Recipient: c.Recipient}
		} else {
			if d.Recipient != c.Recipient {
				return reply("Получатель данных изменился. Нажми «Получить приз» и начни заново.")
			}
			if d.Step == "consent" {
				return true, storage.Reply(ctx, tx, key, chat, u.Callback.ID, draftBody(d))
			}
			if !validName(text) {
				return reply("Введи имя или фамилию буквами, от 1 до 100 символов. Допустимы пробел, дефис и апостроф.")
			}
			if d.Step == "first" {
				d.First = text
				d.Step = "last"
			} else {
				d.Last = text
				d.Step = "consent"
			}
		}
		raw, err = json.Marshal(d)
		if err != nil {
			return true, err
		}
		ttl := time.Until(c.Deadline)
		if ttl <= 0 {
			return reply("Срок подтверждения приза истек.")
		}
		// Cache this event's result atomically with the draft, so PG rollback cannot consume a name twice.
		_, err = h.Claims.TxPipelined(ctx, func(p redis.Pipeliner) error { p.Set(ctx, session, raw, ttl); p.Set(ctx, event, raw, ttl); return nil })
		if err != nil {
			return true, err
		}
	}
	return true, storage.Reply(ctx, tx, key, chat, u.Callback.ID, draftBody(d))
}
