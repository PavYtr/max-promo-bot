package storage

import (
	"bytes"
	"errors"
	"io"
	"reflect"
	"testing"
)

func TestDailySelection(t *testing.T) {
	users := []int64{10, 20, 30}
	// Every possible first position is selected exactly once with controlled bytes.
	for i := 0; i < 3; i++ {
		got, err := selectDaily(users, 1, bytes.NewReader([]byte{byte(i)}))
		if err != nil || len(got) != 1 || got[0] != users[i] {
			t.Fatalf("position %d: %v %v", i, got, err)
		}
	}
	got, err := selectDaily(users, 3, bytes.NewReader([]byte{2, 1}))
	if err != nil || !reflect.DeepEqual(got, []int64{30, 10, 20}) {
		t.Fatalf("%v %v", got, err)
	}
	if !reflect.DeepEqual(users, []int64{10, 20, 30}) {
		t.Fatal("input mutated")
	}
	if _, err = selectDaily(users, 4, bytes.NewReader(nil)); err == nil {
		t.Fatal("accepted shortage")
	}
	if _, err = selectDaily(users, 1, bytes.NewReader(nil)); !errors.Is(err, io.EOF) {
		t.Fatalf("random failure lost: %v", err)
	}
	// Values outside the interval are rejected, rather than reduced modulo N.
	got, err = selectDaily(users, 1, bytes.NewReader([]byte{3, 1}))
	if err != nil || got[0] != 20 {
		t.Fatalf("rejection sampling: %v %v", got, err)
	}
}
