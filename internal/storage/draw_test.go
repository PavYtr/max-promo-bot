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

func TestWeightedSelection(t *testing.T) {
	users := []weightedUser{{10, 1}, {20, 3}, {30, 2}}
	counts := map[int64]int{}
	for ticket := byte(0); ticket < 6; ticket++ {
		got, err := selectWeighted(users, 1, bytes.NewReader([]byte{ticket}))
		if err != nil {
			t.Fatal(err)
		}
		counts[got[0]]++
	}
	if !reflect.DeepEqual(counts, map[int64]int{10: 1, 20: 3, 30: 2}) {
		t.Fatal("incorrect weighted intervals", counts)
	}
	// First pick removes all three chances of user 20; ticket 1 then picks 30.
	got, err := selectWeighted(users, 3, bytes.NewReader([]byte{1, 1}))
	if err != nil || !reflect.DeepEqual(got, []int64{20, 30, 10}) {
		t.Fatal(got, err)
	}
	if !reflect.DeepEqual(users, []weightedUser{{10, 1}, {20, 3}, {30, 2}}) {
		t.Fatal("input mutated")
	}
	for _, n := range []int{-1, 4} {
		if _, err = selectWeighted(users, n, bytes.NewReader(nil)); err == nil {
			t.Fatal("invalid size accepted")
		}
	}
	for _, weight := range []int{0, -1, 11} {
		if _, err = selectWeighted([]weightedUser{{10, weight}}, 1, bytes.NewReader(nil)); err == nil {
			t.Fatal("invalid weight accepted")
		}
	}
	if _, err = selectWeighted(users, 1, bytes.NewReader(nil)); !errors.Is(err, io.EOF) {
		t.Fatal("random failure lost", err)
	}
	got, err = selectWeighted(users, 1, bytes.NewReader([]byte{7, 4}))
	if err != nil || got[0] != 30 {
		t.Fatal("biased modulo selection", got, err)
	}
}
