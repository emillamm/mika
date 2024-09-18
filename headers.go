package mika

import (
	"fmt"
	"github.com/twmb/franz-go/pkg/kgo"
	"strconv"
	"errors"
)

var ErrHeaderDoesNotExist = errors.New("Record header does not exist")

type HeaderTypes interface{int|string}

type HeaderValue struct {
	Index int
	Bytes []byte
}

// A structure that helps managing record headers
type Headers struct {
	underlying *kgo.Record
	headerMap map[string]*HeaderValue
}

func NewHeaders(record *kgo.Record) *Headers {
	headers := &Headers{
		headerMap: make(map[string]*HeaderValue),
		underlying: record,
	}
	headers.underlying = record
	for index, header := range record.Headers {
		headers.headerMap[header.Key] = &HeaderValue{
			Index: index,
			Bytes: header.Value,
		}
	}
	return headers
}

func GetHeader[T HeaderTypes](
	headers *Headers,
	key string,
	deserializer func([]byte)(T,error),
) (res T, err error) {
	headerValue, ok := headers.headerMap[key]
	if !ok {
		err = ErrHeaderDoesNotExist
		return
	}
	res, err = deserializer(headerValue.Bytes)
	if err != nil {
		err = fmt.Errorf("Could not convert key %v to desired type: %w", key, err)
	}
	return
}

func SetHeader[T HeaderTypes](
	headers *Headers,
	key string,
	value T,
	serializer func(T)[]byte,
) {
	bytes := serializer(value)
	headers.setOrUpdateHeaderValue(key, bytes)
}

func GetOrSetHeader[T HeaderTypes](
	headers *Headers,
	key string,
	value T,
	deserializer func([]byte)(T,error),
	serializer func(T)[]byte,
) (T, error) {
	res, err := GetHeader(headers, key, deserializer)
	if err == ErrHeaderDoesNotExist {
		SetHeader(headers, key, value, serializer)
		return value, nil
	}
	return res, err
}

func StringSerializer(v string) []byte {
	return []byte(v)
}
func StringDeserializer(bytes []byte) (string, error) {
	return string(bytes), nil
}
func IntSerializer(v int) []byte {
	return []byte(strconv.Itoa(v))
}
func IntDeserializer(bytes []byte) (int, error) {
	return strconv.Atoi(string(bytes))
}

// Update value in array record.Headers and use the stored index to identify the correct element.
// If it is a new header, append this as a new element to the array.
func (headers *Headers) setOrUpdateHeaderValue(key string, bytes []byte) {
	header, ok := headers.headerMap[key]
	if !ok {
		// new header => append
		header = &HeaderValue{}
		header.Bytes = bytes
		header.Index = len(headers.underlying.Headers)
		headers.underlying.Headers = append(headers.underlying.Headers, kgo.RecordHeader{
			Key: key,
			Value: bytes,
		})
	} else {
		// existing header => update
		header.Bytes = bytes
		underlyingRecordHeader := &headers.underlying.Headers[header.Index]
		if underlyingRecordHeader.Key != key {
			panicStr := "RecordHeader.Key \"%s\" did not match local Headers.headerMap key \"%s\". This should not happen!"
			panic(fmt.Sprintf(panicStr, underlyingRecordHeader.Key, key))
		}
		underlyingRecordHeader.Value = bytes
	}
	// update headerMap
	headers.headerMap[key] = header
}

