package lm2log

import (
	"errors"
	"strconv"

	"github.com/Preetam/lm2"
)

const (
	PendingKey   = "pending"
	CommittedKey = "committed"
)

var (
	errNotFound = errors.New("lm2log: not found")
)

type Log struct {
	col *lm2.Collection
}

func New(col *lm2.Collection) (*Log, error) {
	l := &Log{
		col: col,
	}

	wb := lm2.NewWriteBatch()
	wb.Set(CommittedKey, "")
	wb.Delete(PendingKey)
	_, err := col.Update(wb)

	return l, err
}

func Open(col *lm2.Collection) (*Log, error) {
	l := &Log{
		col: col,
	}

	cur, err := col.NewCursor()
	if err != nil {
		return nil, err
	}

	// Check CommittedKey
	_, err = cursorGet(cur, CommittedKey)
	if err != nil {
		return nil, err
	}

	return l, nil
}

func (l *Log) Prepare(data string) error {
	cur, err := l.col.NewCursor()
	if err != nil {
		return err
	}

	// Check if something is already prepared.
	if _, err := cursorGet(cur, PendingKey); err == nil {
		return errors.New("lm2col: already prepared data")
	}

	// Prepare
	committedStr, err := cursorGet(cur, CommittedKey)
	committed, err := strconv.ParseUint(committedStr, 10, 64)
	if err != nil {
		return err
	}

	wb := lm2.NewWriteBatch()
	nextRecordNum := strconv.FormatUint(committed, 10)
	wb.Set(nextRecordNum, data)
	wb.Set(PendingKey, nextRecordNum)
	_, err = l.col.Update(wb)
	return err
}

func (l *Log) Rollback() error {
	cur, err := l.col.NewCursor()
	if err != nil {
		return err
	}

	if _, err = cursorGet(cur, PendingKey); err != nil {
		if err == errNotFound {
			return nil
		}
		return err
	}

	wb := lm2.NewWriteBatch()
	wb.Delete(PendingKey)
	_, err = l.col.Update(wb)
	return err
}

func (l *Log) Commit() error {
	cur, err := l.col.NewCursor()
	if err != nil {
		return err
	}

	// Check if something is already prepared.
	prepared, err := cursorGet(cur, PendingKey)
	if err != nil {
		return errors.New("lm2col: couldn't get prepared data")
	}

	// Commit
	wb := lm2.NewWriteBatch()
	wb.Set(CommittedKey, prepared)
	wb.Delete(PendingKey)
	_, err = l.col.Update(wb)

	return err
}

func (l *Log) Pending() (uint64, error) {
	cur, err := l.col.NewCursor()
	if err != nil {
		return 0, err
	}

	// Check if something is already prepared.
	prepared, err := cursorGet(cur, PendingKey)
	if err != nil {
		return 0, errors.New("lm2col: couldn't get prepared data")
	}

	return strconv.ParseUint(prepared, 10, 64)
}

func (l *Log) Committed() (uint64, error) {
	cur, err := l.col.NewCursor()
	if err != nil {
		return 0, err
	}

	// Check if something is already prepared.
	committed, err := cursorGet(cur, CommittedKey)
	if err != nil {
		return 0, errors.New("lm2col: couldn't get committed data")
	}

	return strconv.ParseUint(committed, 10, 64)
}

func (l *Log) Get(record uint64) (string, error) {
	cur, err := l.col.NewCursor()
	if err != nil {
		return "", err
	}

	// Check if something is already prepared.
	data, err := cursorGet(cur, strconv.FormatUint(record, 10))
	if err != nil {
		return "", errors.New("lm2col: couldn't get record")
	}
	return data, nil
}

func (l *Log) SetCommitted(record uint64, data string) error {
	cur, err := l.col.NewCursor()
	if err != nil {
		return err
	}

	wb := lm2.NewWriteBatch()
	recordStr := strconv.FormatUint(record, 10)
	committedStr, err := cursorGet(cur, CommittedKey)
	committed, err := strconv.ParseUint(committedStr, 10, 64)
	if err != nil {
		return err
	}
	if committed < record {
		wb.Set(committedStr, recordStr)
	}

	wb.Set(recordStr, data)
	_, err = l.col.Update(wb)
	return err
}

func cursorGet(cur *lm2.Cursor, key string) (string, error) {
	cur.Seek(key)
	for cur.Next() {
		if cur.Key() > key {
			break
		}
		if cur.Key() == key {
			return cur.Value(), nil
		}
	}
	return "", errNotFound
}
