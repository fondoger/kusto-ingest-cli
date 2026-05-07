package ingest

// Batcher accumulates rows separated by '\n', flushing when adding the next row
// would exceed either maxBytes or maxRows (whichever comes first). A single
// oversize row is sent on its own. maxRows == 0 disables the row-count limit.
type Batcher struct {
	maxBytes int
	maxRows  int
	send     func([]byte) error
	buf      []byte
	rows     int
}

func NewBatcher(maxBytes, maxRows int, send func([]byte) error) *Batcher {
	return &Batcher{maxBytes: maxBytes, maxRows: maxRows, send: send}
}

func (b *Batcher) Add(row []byte) error {
	nextSize := len(row)
	if len(b.buf) > 0 {
		nextSize++ // newline separator
	}
	sizeWouldExceed := len(b.buf)+nextSize > b.maxBytes && len(b.buf) > 0
	rowsWouldExceed := b.maxRows > 0 && b.rows >= b.maxRows
	if sizeWouldExceed || rowsWouldExceed {
		if err := b.Flush(); err != nil {
			return err
		}
	}
	if len(b.buf) > 0 {
		b.buf = append(b.buf, '\n')
	}
	b.buf = append(b.buf, row...)
	b.rows++
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
	b.rows = 0
	return nil
}
