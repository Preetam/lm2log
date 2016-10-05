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
	col           *lm2.Collection
	currentRecord uint64
}

func New(col *lm2.Collection) *Log {
	return &Log{
		col: col,
	}
}

func (l *Log) Prepare(data string) error {
	wb := lm2.NewWriteBatch()
	recordNum := strconv.FormatUint(l.currentRecord, 10)
	wb.Set(recordNum, data)
	wb.Set(PendingKey, recordNum)
	_, err := l.col.Update(wb)
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
	// TODO
	return errors.New("not implemented")
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
