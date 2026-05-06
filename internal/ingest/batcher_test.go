package ingest

import (
	"strings"
	"testing"
)

func TestBatcherFlushesBeforeExceeding(t *testing.T) {
	var sent [][]byte
	b := NewBatcher(20, func(batch []byte) error {
		// Copy because Batcher reuses its buffer
		cp := make([]byte, len(batch))
		copy(cp, batch)
		sent = append(sent, cp)
		return nil
	})
	// Each row is 8 bytes. With newline separator, 2 rows = 17 bytes (fits), 3 rows would be 26 (exceeds 20).
	for i := 0; i < 3; i++ {
		if err := b.Add([]byte("12345678")); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.Flush(); err != nil {
		t.Fatal(err)
	}
	if len(sent) != 2 {
		t.Fatalf("expected 2 flushes, got %d: %v", len(sent), sent)
	}
	// First flush should hold the first 2 rows; never exceed 20 bytes
	for _, batch := range sent {
		if len(batch) > 20 {
			t.Errorf("batch exceeded max: %d bytes", len(batch))
		}
		// rows separated by single \n
		parts := strings.Split(string(batch), "\n")
		for _, p := range parts {
			if p != "12345678" {
				t.Errorf("bad row %q", p)
			}
		}
	}
}

func TestBatcherOversizedSingleRow(t *testing.T) {
	var sent [][]byte
	b := NewBatcher(10, func(batch []byte) error {
		cp := make([]byte, len(batch))
		copy(cp, batch)
		sent = append(sent, cp)
		return nil
	})
	// Row larger than max — must still be sent (on its own)
	if err := b.Add([]byte("VERY_LONG_ROW_OVER_LIMIT")); err != nil {
		t.Fatal(err)
	}
	if err := b.Flush(); err != nil {
		t.Fatal(err)
	}
	if len(sent) != 1 || string(sent[0]) != "VERY_LONG_ROW_OVER_LIMIT" {
		t.Errorf("unexpected: %v", sent)
	}
}

func TestBatcherEmpty(t *testing.T) {
	called := 0
	b := NewBatcher(100, func(batch []byte) error { called++; return nil })
	if err := b.Flush(); err != nil {
		t.Fatal(err)
	}
	if called != 0 {
		t.Errorf("Flush on empty should not call send; got %d", called)
	}
}
