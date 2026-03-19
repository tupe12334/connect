// Copyright 2026 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package confluent

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"time"

	"github.com/redpanda-data/benthos/v4/public/schema"
)

// normalizeForAvro walks the schema.Common tree and coerces values from
// AsStructuredMut() into the native Go types that goavro's BinaryFromNative
// expects. rawJSON controls union wrapping behavior for optional/union fields.
func normalizeForAvro(data any, s schema.Common, rawJSON bool) (any, error) {
	if s.Optional {
		if data == nil {
			return nil, nil
		}
		// BinaryFromNative always requires union wrapping for nullable
		// fields: map[string]any{"typeName": value}.
		//
		// When rawJSON == false (lame union mode), the input JSON already
		// contains the wrapping (e.g. {"string": "val"}), which
		// AsStructuredMut() deserializes to map[string]any. Detect this
		// and normalize the inner value.
		if !rawJSON {
			if wrapped, ok := data.(map[string]any); ok && len(wrapped) == 1 {
				for k, v := range wrapped {
					norm, err := normalizeValue(v, s, rawJSON)
					if err != nil {
						return nil, err
					}
					return map[string]any{k: norm}, nil
				}
			}
		}
		inner, err := normalizeValue(data, s, rawJSON)
		if err != nil {
			return nil, err
		}
		key := avroUnionKey(s)
		return map[string]any{key: inner}, nil
	}
	return normalizeValue(data, s, rawJSON)
}

// normalizeValue converts a single value according to its schema type.
func normalizeValue(data any, s schema.Common, rawJSON bool) (any, error) {
	if data == nil {
		if s.Type == schema.Null || s.Type == schema.Union {
			return nil, nil
		}
		return nil, fmt.Errorf("field %q: nil value for required %v field", s.Name, s.Type)
	}

	switch s.Type {
	case schema.Null:
		return nil, fmt.Errorf("field %q: schema type is null but received non-nil %T", s.Name, data)
	case schema.Boolean:
		return normalizeBool(data, s.Name)
	case schema.Int32:
		return normalizeInt32(data, s.Name)
	case schema.Int64:
		return normalizeInt64(data, s.Name)
	case schema.Float32:
		return normalizeFloat32(data, s.Name)
	case schema.Float64:
		return normalizeFloat64(data, s.Name)
	case schema.String:
		return normalizeString(data, s.Name)
	case schema.ByteArray:
		return normalizeBytes(data, s.Name)
	case schema.Timestamp:
		return normalizeTimestamp(data, s.Name)
	case schema.Object:
		return normalizeObject(data, s, rawJSON)
	case schema.Array:
		return normalizeArray(data, s, rawJSON)
	case schema.Map:
		return normalizeMap(data, s, rawJSON)
	case schema.Union:
		return normalizeUnion(data, s, rawJSON)
	case schema.Any:
		return normalizeAny(data)
	default:
		return nil, fmt.Errorf("field %q: unsupported schema type %v", s.Name, s.Type)
	}
}

func normalizeBool(data any, name string) (bool, error) {
	switch v := data.(type) {
	case bool:
		return v, nil
	default:
		return false, fmt.Errorf("field %q: expected bool, got %T", name, data)
	}
}

func normalizeInt32(data any, name string) (int32, error) {
	n, err := toInt64(data)
	if err != nil {
		return 0, fmt.Errorf("field %q: %w", name, err)
	}
	if n < math.MinInt32 || n > math.MaxInt32 {
		return 0, fmt.Errorf("field %q: value %d overflows int32", name, n)
	}
	return int32(n), nil
}

func normalizeInt64(data any, name string) (int64, error) {
	n, err := toInt64(data)
	if err != nil {
		return 0, fmt.Errorf("field %q: %w", name, err)
	}
	return n, nil
}

func normalizeFloat32(data any, name string) (float32, error) {
	f, err := toFloat64(data)
	if err != nil {
		return 0, fmt.Errorf("field %q: %w", name, err)
	}
	return float32(f), nil
}

func normalizeFloat64(data any, name string) (float64, error) {
	f, err := toFloat64(data)
	if err != nil {
		return 0, fmt.Errorf("field %q: %w", name, err)
	}
	return f, nil
}

func normalizeString(data any, name string) (string, error) {
	switch v := data.(type) {
	case string:
		return v, nil
	default:
		return "", fmt.Errorf("field %q: expected string, got %T", name, data)
	}
}

func normalizeBytes(data any, name string) ([]byte, error) {
	switch v := data.(type) {
	case []byte:
		return v, nil
	case string:
		b, err := base64.StdEncoding.DecodeString(v)
		if err != nil {
			// Not base64 — treat as raw bytes.
			return []byte(v), nil
		}
		return b, nil
	default:
		return nil, fmt.Errorf("field %q: expected []byte or string, got %T", name, data)
	}
}

func normalizeTimestamp(data any, name string) (time.Time, error) {
	switch v := data.(type) {
	case time.Time:
		return v, nil
	case string:
		t, err := time.Parse(time.RFC3339Nano, v)
		if err != nil {
			return time.Time{}, fmt.Errorf("field %q: parsing timestamp: %w", name, err)
		}
		return t, nil
	case float64:
		return time.UnixMilli(int64(v)), nil
	case int64:
		return time.UnixMilli(v), nil
	case int:
		return time.UnixMilli(int64(v)), nil
	case int32:
		return time.UnixMilli(int64(v)), nil
	case json.Number:
		n, err := v.Int64()
		if err != nil {
			return time.Time{}, fmt.Errorf("field %q: parsing timestamp from json.Number: %w", name, err)
		}
		return time.UnixMilli(n), nil
	default:
		return time.Time{}, fmt.Errorf("field %q: expected time.Time, string, or numeric for timestamp, got %T", name, data)
	}
}

func normalizeObject(data any, s schema.Common, rawJSON bool) (map[string]any, error) {
	m, ok := data.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("field %q: expected map[string]any for object, got %T", s.Name, data)
	}
	out := make(map[string]any, len(s.Children))
	for _, child := range s.Children {
		val, exists := m[child.Name]
		if !exists {
			if child.Optional {
				out[child.Name] = nil
				continue
			}
			return nil, fmt.Errorf("field %q: required field %q is missing", s.Name, child.Name)
		}
		norm, err := normalizeForAvro(val, child, rawJSON)
		if err != nil {
			return nil, err
		}
		out[child.Name] = norm
	}
	return out, nil
}

func normalizeArray(data any, s schema.Common, rawJSON bool) ([]any, error) {
	arr, ok := data.([]any)
	if !ok {
		return nil, fmt.Errorf("field %q: expected []any for array, got %T", s.Name, data)
	}
	if len(s.Children) == 0 {
		return arr, nil
	}
	elemSchema := s.Children[0]
	out := make([]any, len(arr))
	for i, elem := range arr {
		norm, err := normalizeForAvro(elem, elemSchema, rawJSON)
		if err != nil {
			return nil, fmt.Errorf("field %q[%d]: %w", s.Name, i, err)
		}
		out[i] = norm
	}
	return out, nil
}

func normalizeMap(data any, s schema.Common, rawJSON bool) (map[string]any, error) {
	m, ok := data.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("field %q: expected map[string]any for map, got %T", s.Name, data)
	}
	if len(s.Children) == 0 {
		return m, nil
	}
	valSchema := s.Children[0]
	out := make(map[string]any, len(m))
	for k, v := range m {
		norm, err := normalizeForAvro(v, valSchema, rawJSON)
		if err != nil {
			return nil, fmt.Errorf("field %q[%q]: %w", s.Name, k, err)
		}
		out[k] = norm
	}
	return out, nil
}

func normalizeUnion(data any, s schema.Common, rawJSON bool) (any, error) {
	if data == nil {
		return nil, nil
	}

	// When rawJSON == false, the input may already be union-wrapped.
	if !rawJSON {
		if wrapped, ok := data.(map[string]any); ok && len(wrapped) == 1 {
			for k, v := range wrapped {
				// Find the matching branch and normalize the inner value.
				for _, child := range s.Children {
					if avroUnionKey(child) == k {
						norm, err := normalizeValue(v, child, rawJSON)
						if err != nil {
							return nil, err
						}
						return map[string]any{k: norm}, nil
					}
				}
				// Key didn't match any branch, pass through as-is.
				return map[string]any{k: v}, nil
			}
		}
	}

	// BinaryFromNative always requires union wrapping.
	for _, child := range s.Children {
		if child.Type == schema.Null {
			continue
		}
		norm, err := normalizeValue(data, child, rawJSON)
		if err == nil {
			key := avroUnionKey(child)
			return map[string]any{key: norm}, nil
		}
	}
	return nil, fmt.Errorf("field %q: no union branch matched value of type %T", s.Name, data)
}

func normalizeAny(data any) ([]byte, error) {
	switch v := data.(type) {
	case []byte:
		return v, nil
	case string:
		return []byte(v), nil
	default:
		b, err := json.Marshal(data)
		if err != nil {
			return nil, fmt.Errorf("encoding 'any' value: %w", err)
		}
		return b, nil
	}
}

// avroUnionKey returns the Avro type name used as the key in goavro's
// non-rawJSON union representation: map[string]any{"typeName": value}.
func avroUnionKey(s schema.Common) string {
	switch s.Type {
	case schema.Boolean:
		return "boolean"
	case schema.Int32:
		return "int"
	case schema.Int64:
		return "long"
	case schema.Float32:
		return "float"
	case schema.Float64:
		return "double"
	case schema.String:
		return "string"
	case schema.ByteArray:
		return "bytes"
	case schema.Null:
		return "null"
	case schema.Timestamp:
		return "long.timestamp-millis"
	case schema.Object:
		if s.Name != "" {
			return s.Name
		}
		return "record"
	case schema.Array:
		return "array"
	case schema.Map:
		return "map"
	default:
		return "bytes"
	}
}

// toInt64 coerces various numeric types to int64.
func toInt64(data any) (int64, error) {
	switch v := data.(type) {
	case int:
		return int64(v), nil
	case int32:
		return int64(v), nil
	case int64:
		return v, nil
	case float64:
		if v != math.Trunc(v) {
			return 0, fmt.Errorf("expected integer, got float %v", v)
		}
		return int64(v), nil
	case float32:
		f := float64(v)
		if f != math.Trunc(f) {
			return 0, fmt.Errorf("expected integer, got float %v", v)
		}
		return int64(v), nil
	case json.Number:
		return v.Int64()
	default:
		return 0, fmt.Errorf("expected numeric, got %T", data)
	}
}

// toFloat64 coerces various numeric types to float64.
func toFloat64(data any) (float64, error) {
	switch v := data.(type) {
	case float64:
		return v, nil
	case float32:
		return float64(v), nil
	case int:
		return float64(v), nil
	case int32:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case json.Number:
		return v.Float64()
	default:
		return 0, fmt.Errorf("expected numeric, got %T", data)
	}
}
