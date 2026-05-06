package ingest

// Batcher accumulates JSON-serialized rows separated by '\n', flushing whenever
// adding the next row would exceed maxBytes. A single oversize row is sent on its own.
type Batcher struct {
	maxBytes int
	send     func([]byte) error
	buf      []byte
}

func NewBatcher(maxBytes int, send func([]byte) error) *Batcher {
	return &Batcher{maxBytes: maxBytes, send: send}
}

func (b *Batcher) Add(row []byte) error {
	nextSize := len(row)
	if len(b.buf) > 0 {
		nextSize++ // newline separator
	}
	if len(b.buf)+nextSize > b.maxBytes && len(b.buf) > 0 {
		if err := b.Flush(); err != nil {
			return err
		}
	}
	if len(b.buf) > 0 {
		b.buf = append(b.buf, '\n')
	}
	b.buf = append(b.buf, row...)
	return nil
}

func (b *Batcher) Flush() error {
	if len(b.buf) == 0 {
		return nil
	}
	if err := b.send(b.buf); err != nil {
		return err
	}
	b.buf = b.buf[:0]
	return nil
}
