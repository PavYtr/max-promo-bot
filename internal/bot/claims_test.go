package bot

import (
	"strings"
	"testing"
)

func TestValidClaimName(t *testing.T) {
	for _, s := range []string{"Анна-Мария", "О’Коннор", "Jean Luc", "李", "O'Connor"} {
		if !validName(s) {
			t.Errorf("valid name rejected: %q", s)
		}
	}
	for _, s := range []string{"", "123", "---", "<script>", "Иван\nИван", "Иван\u200b", strings.Repeat("я", 101)} {
		if validName(s) {
			t.Errorf("invalid name accepted: %q", s)
		}
	}
}
