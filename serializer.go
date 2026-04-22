package bus

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
)

type Serializer interface {
	Encode(v any) ([]byte, error)
	Decode(data []byte, v any) error
}

type GobSerializer struct{}

func (g GobSerializer) Encode(v any) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(v); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (g GobSerializer) Decode(data []byte, v any) error {
	return gob.NewDecoder(bytes.NewBuffer(data)).Decode(v)
}

func DefaultSerializer() Serializer {
	return GobSerializer{}
}

type JsonSerializer struct{}

func (j JsonSerializer) Encode(v any) ([]byte, error) {
	return json.Marshal(v)
}

func (j JsonSerializer) Decode(data []byte, v any) error {
	return json.Unmarshal(data, v)
}
