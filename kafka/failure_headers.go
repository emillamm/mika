package kafka

import "fmt"

const FailureHeaderKeyFmt = "mika_err_%s_%s"
const KeyOriginalTopic = "org_topic"
const KeyErrMessage = "msg"
const KeyRetries = "retries"

type FailureHeaders struct {
	OriginalTopic string
	ErrMessage string
	Retries int
}

func InitFailureHeaders(headers *Headers, group string, reason error) (state *FailureHeaders, err error) {
	wrapErr := func() { err = fmt.Errorf("failed to create record failure state: %v", err) }
	originalTopic, err := GetOrSetFailureHeader(headers, group, KeyOriginalTopic, headers.underlying.Topic, StringDeserializer, StringSerializer)
	if err != nil { wrapErr(); return }
	errMessage, err := GetOrSetFailureHeader(headers, group, KeyErrMessage, reason.Error(), StringDeserializer, StringSerializer)
	if err != nil { wrapErr(); return }
	retries, err := GetOrSetFailureHeader(headers, group, KeyRetries, 0, IntDeserializer, IntSerializer)
	if err != nil { wrapErr(); return }
	state = &FailureHeaders{
		OriginalTopic: originalTopic,
		ErrMessage: errMessage,
		Retries: retries,
	}
	return
}

func GetOrSetFailureHeader[T HeaderTypes](
	headers *Headers,
	group string,
	key string,
	value T,
	deserializer func([]byte)(T,error),
	serializer func(T)[]byte,
) (res T, err error) {
	res, err = GetOrSetHeader(headers, FailureHeaderKey(key, group), value, deserializer, serializer)
	return
}

func GetFailureHeader[T HeaderTypes](
	headers *Headers,
	group string,
	key string,
	deserializer func([]byte)(T,error),
) (res T, err error) {
	res, err = GetHeader(headers, FailureHeaderKey(key, group), deserializer)
	return
}

func SetFailureHeader[T HeaderTypes](
	headers *Headers,
	group string,
	key string,
	value T,
	serializer func(T)[]byte,
) {
	SetHeader(headers, FailureHeaderKey(key, group), value, serializer)
}

func(r *FailureHeaders) IncrementRetries(headers *Headers, group string) {
	r.Retries = r.Retries + 1
	SetFailureHeader(headers, group, KeyRetries, r.Retries, IntSerializer)
}

func FailureHeaderKey(key string, group string) string {
	return fmt.Sprintf(FailureHeaderKeyFmt, group, key)
}

