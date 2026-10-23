package maxapi

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
)

type User struct {
	ID        int64  `json:"user_id"`
	FirstName string `json:"first_name"`
	LastName  string `json:"last_name"`
	Name      string `json:"name"`
	Username  string `json:"username"`
}
type Message struct {
	Sender    User `json:"sender"`
	Recipient struct {
		ChatID int64 `json:"chat_id"`
	} `json:"recipient"`
	Body struct {
		MID  string `json:"mid"`
		Text string `json:"text"`
	} `json:"body"`
}
type Update struct {
	Type      string  `json:"update_type"`
	Timestamp int64   `json:"timestamp"`
	ChatID    int64   `json:"chat_id"`
	User      User    `json:"user"`
	Message   Message `json:"message"`
	Callback  struct {
		ID      string `json:"callback_id"`
		Payload string `json:"payload"`
		User    User   `json:"user"`
	} `json:"callback"`
}

func Decode(raw []byte) (Update, string, error) {
	var u Update
	if err := json.Unmarshal(raw, &u); err != nil || u.Type == "" {
		return u, "", errors.New("invalid update")
	}
	var canonical any
	if err := json.Unmarshal(raw, &canonical); err != nil {
		return u, "", err
	}
	b, _ := json.Marshal(canonical)
	h := sha256.Sum256(b)
	key := u.Type + ":" + hex.EncodeToString(h[:])
	switch u.Type {
	case "message_created":
		if u.Message.Body.MID == "" || u.Message.Sender.ID <= 0 || u.Message.Recipient.ChatID == 0 {
			return u, "", errors.New("invalid message")
		}
		key = u.Type + ":" + u.Message.Body.MID
	case "message_callback":
		if u.Callback.ID == "" || u.Callback.User.ID <= 0 || u.Message.Recipient.ChatID == 0 {
			return u, "", errors.New("invalid callback")
		}
		key = u.Type + ":" + u.Callback.ID
	case "bot_started":
		if u.User.ID <= 0 || u.ChatID == 0 || u.Timestamp <= 0 {
			return u, "", errors.New("invalid bot_started")
		}
	}
	return u, key, nil
}
func (u Update) Actor() (User, int64) {
	switch u.Type {
	case "bot_started":
		return u.User, u.ChatID
	case "message_callback":
		return u.Callback.User, u.Message.Recipient.ChatID
	default:
		return u.Message.Sender, u.Message.Recipient.ChatID
	}
}

type Attachment struct {
	Type    string `json:"type"`
	Payload any    `json:"payload"`
}
type Body struct {
	Text        string       `json:"text"`
	Attachments []Attachment `json:"attachments,omitempty"`
}
type Button struct {
	Type    string `json:"type"`
	Text    string `json:"text"`
	Payload string `json:"payload,omitempty"`
	URL     string `json:"url,omitempty"`
}

func Keyboard(buttons ...Button) Attachment {
	rows := make([][]Button, 0, len(buttons))
	for _, b := range buttons {
		rows = append(rows, []Button{b})
	}
	return Attachment{Type: "inline_keyboard", Payload: map[string]any{"buttons": rows}}
}
func Callback(text, payload string) Button {
	return Button{Type: "callback", Text: text, Payload: payload}
}
