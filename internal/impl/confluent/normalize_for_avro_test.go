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
	"encoding/json"
	"testing"
	"time"

	goavro "github.com/linkedin/goavro/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/schema"
)

func TestNormalizeForAvro(t *testing.T) {
	// --- Primitive passthrough ---

	t.Run("bool passthrough", func(t *testing.T) {
		result, err := normalizeForAvro(true, schema.Common{Name: "x", Type: schema.Boolean}, true)
		require.NoError(t, err)
		assert.Equal(t, true, result)
	})

	t.Run("string passthrough", func(t *testing.T) {
		result, err := normalizeForAvro("hello", schema.Common{Name: "x", Type: schema.String}, true)
		require.NoError(t, err)
		assert.Equal(t, "hello", result)
	})

	t.Run("float64 passthrough", func(t *testing.T) {
		result, err := normalizeForAvro(float64(3.14), schema.Common{Name: "x", Type: schema.Float64}, true)
		require.NoError(t, err)
		assert.Equal(t, float64(3.14), result)
	})

	// --- Numeric coercion ---

	t.Run("float64 to int32", func(t *testing.T) {
		result, err := normalizeForAvro(float64(42), schema.Common{Name: "x", Type: schema.Int32}, true)
		require.NoError(t, err)
		assert.Equal(t, int32(42), result)
	})

	t.Run("float64 to int64", func(t *testing.T) {
		result, err := normalizeForAvro(float64(1e12), schema.Common{Name: "x", Type: schema.Int64}, true)
		require.NoError(t, err)
		assert.Equal(t, int64(1e12), result)
	})

	t.Run("float64 to float32", func(t *testing.T) {
		result, err := normalizeForAvro(float64(1.5), schema.Common{Name: "x", Type: schema.Float32}, true)
		require.NoError(t, err)
		assert.Equal(t, float32(1.5), result)
	})

	t.Run("int to int32", func(t *testing.T) {
		result, err := normalizeForAvro(int(99), schema.Common{Name: "x", Type: schema.Int32}, true)
		require.NoError(t, err)
		assert.Equal(t, int32(99), result)
	})

	t.Run("int64 to int32", func(t *testing.T) {
		result, err := normalizeForAvro(int64(7), schema.Common{Name: "x", Type: schema.Int32}, true)
		require.NoError(t, err)
		assert.Equal(t, int32(7), result)
	})

	t.Run("json.Number to int32", func(t *testing.T) {
		result, err := normalizeForAvro(json.Number("42"), schema.Common{Name: "x", Type: schema.Int32}, true)
		require.NoError(t, err)
		assert.Equal(t, int32(42), result)
	})

	t.Run("json.Number to int64", func(t *testing.T) {
		result, err := normalizeForAvro(json.Number("9999999999"), schema.Common{Name: "x", Type: schema.Int64}, true)
		require.NoError(t, err)
		assert.Equal(t, int64(9999999999), result)
	})

	t.Run("json.Number to float32", func(t *testing.T) {
		result, err := normalizeForAvro(json.Number("1.5"), schema.Common{Name: "x", Type: schema.Float32}, true)
		require.NoError(t, err)
		assert.Equal(t, float32(1.5), result)
	})

	t.Run("json.Number to float64", func(t *testing.T) {
		result, err := normalizeForAvro(json.Number("3.14"), schema.Common{Name: "x", Type: schema.Float64}, true)
		require.NoError(t, err)
		assert.InDelta(t, float64(3.14), result, 0.001)
	})

	// --- Numeric error cases ---

	t.Run("int32 overflow", func(t *testing.T) {
		_, err := normalizeForAvro(float64(3e10), schema.Common{Name: "big", Type: schema.Int32}, true)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "overflows int32")
	})

	t.Run("non-integer float64 for int32", func(t *testing.T) {
		_, err := normalizeForAvro(float64(1.5), schema.Common{Name: "x", Type: schema.Int32}, true)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "expected integer")
	})

	t.Run("wrong type for int32", func(t *testing.T) {
		_, err := normalizeForAvro("not a number", schema.Common{Name: "x", Type: schema.Int32}, true)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "expected numeric")
	})

	// --- Timestamp ---

	t.Run("timestamp from RFC3339 string", func(t *testing.T) {
		ts := "2026-03-19T10:05:09.934345Z"
		expected, err := time.Parse(time.RFC3339Nano, ts)
		require.NoError(t, err)

		result, err := normalizeForAvro(ts, schema.Common{Name: "ts", Type: schema.Timestamp}, true)
		require.NoError(t, err)
		assert.Equal(t, expected, result)
	})

	t.Run("timestamp from time.Time", func(t *testing.T) {
		ts := time.Date(2026, 3, 19, 10, 0, 0, 0, time.UTC)
		result, err := normalizeForAvro(ts, schema.Common{Name: "ts", Type: schema.Timestamp}, true)
		require.NoError(t, err)
		assert.Equal(t, ts, result)
	})

	t.Run("timestamp from int64 millis", func(t *testing.T) {
		millis := int64(1742378709934)
		result, err := normalizeForAvro(millis, schema.Common{Name: "ts", Type: schema.Timestamp}, true)
		require.NoError(t, err)
		assert.Equal(t, time.UnixMilli(millis), result)
	})

	t.Run("timestamp from float64 millis", func(t *testing.T) {
		millis := float64(1742378709934)
		result, err := normalizeForAvro(millis, schema.Common{Name: "ts", Type: schema.Timestamp}, true)
		require.NoError(t, err)
		assert.Equal(t, time.UnixMilli(int64(millis)), result)
	})

	t.Run("timestamp from int", func(t *testing.T) {
		result, err := normalizeForAvro(int(1000), schema.Common{Name: "ts", Type: schema.Timestamp}, true)
		require.NoError(t, err)
		assert.Equal(t, time.UnixMilli(1000), result)
	})

	t.Run("timestamp from int32", func(t *testing.T) {
		result, err := normalizeForAvro(int32(500), schema.Common{Name: "ts", Type: schema.Timestamp}, true)
		require.NoError(t, err)
		assert.Equal(t, time.UnixMilli(500), result)
	})

	t.Run("timestamp from json.Number", func(t *testing.T) {
		millis := int64(1742378709934)
		result, err := normalizeForAvro(json.Number("1742378709934"), schema.Common{Name: "ts", Type: schema.Timestamp}, true)
		require.NoError(t, err)
		assert.Equal(t, time.UnixMilli(millis), result)
	})

	t.Run("timestamp invalid string", func(t *testing.T) {
		_, err := normalizeForAvro("not-a-timestamp", schema.Common{Name: "ts", Type: schema.Timestamp}, true)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "parsing timestamp")
	})

	t.Run("timestamp wrong type", func(t *testing.T) {
		_, err := normalizeForAvro(true, schema.Common{Name: "ts", Type: schema.Timestamp}, true)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "expected time.Time, string, or numeric")
	})

	// --- Null ---

	t.Run("null type errors on non-nil data", func(t *testing.T) {
		_, err := normalizeForAvro("anything", schema.Common{Name: "x", Type: schema.Null}, true)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "schema type is null but received non-nil")
	})

	t.Run("null type with nil input", func(t *testing.T) {
		result, err := normalizeForAvro(nil, schema.Common{Name: "x", Type: schema.Null}, true)
		require.NoError(t, err)
		assert.Nil(t, result)
	})

	// --- Any ---

	t.Run("any from []byte", func(t *testing.T) {
		result, err := normalizeForAvro([]byte("raw"), schema.Common{Name: "x", Type: schema.Any}, true)
		require.NoError(t, err)
		assert.Equal(t, []byte("raw"), result)
	})

	t.Run("any from string", func(t *testing.T) {
		result, err := normalizeForAvro("text", schema.Common{Name: "x", Type: schema.Any}, true)
		require.NoError(t, err)
		assert.Equal(t, []byte("text"), result)
	})

	t.Run("any from map marshals to JSON bytes", func(t *testing.T) {
		result, err := normalizeForAvro(map[string]any{"k": "v"}, schema.Common{Name: "x", Type: schema.Any}, true)
		require.NoError(t, err)
		assert.Equal(t, []byte(`{"k":"v"}`), result)
	})

	t.Run("any from number marshals to JSON bytes", func(t *testing.T) {
		result, err := normalizeForAvro(float64(42), schema.Common{Name: "x", Type: schema.Any}, true)
		require.NoError(t, err)
		assert.Equal(t, []byte("42"), result)
	})

	// --- Nil data for non-optional ---

	t.Run("nil data non-optional errors", func(t *testing.T) {
		_, err := normalizeForAvro(nil, schema.Common{Name: "x", Type: schema.String}, true)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "nil value for required")
	})

	t.Run("null optional timestamp", func(t *testing.T) {
		result, err := normalizeForAvro(nil, schema.Common{Name: "ts", Type: schema.Timestamp, Optional: true}, true)
		require.NoError(t, err)
		assert.Nil(t, result)
	})

	// --- ByteArray ---

	t.Run("bytes from []byte", func(t *testing.T) {
		input := []byte("hello")
		result, err := normalizeForAvro(input, schema.Common{Name: "x", Type: schema.ByteArray}, true)
		require.NoError(t, err)
		assert.Equal(t, []byte("hello"), result)
	})

	t.Run("bytes from string", func(t *testing.T) {
		result, err := normalizeForAvro("raw", schema.Common{Name: "x", Type: schema.ByteArray}, true)
		require.NoError(t, err)
		assert.Equal(t, []byte("raw"), result)
	})

	t.Run("bytes wrong type", func(t *testing.T) {
		_, err := normalizeForAvro(42, schema.Common{Name: "x", Type: schema.ByteArray}, true)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "expected []byte or string")
	})

	// --- Error cases for type mismatches ---

	t.Run("bool wrong type", func(t *testing.T) {
		_, err := normalizeForAvro("true", schema.Common{Name: "x", Type: schema.Boolean}, true)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "expected bool")
	})

	t.Run("string wrong type", func(t *testing.T) {
		_, err := normalizeForAvro(42, schema.Common{Name: "x", Type: schema.String}, true)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "expected string")
	})

	t.Run("object wrong type", func(t *testing.T) {
		s := schema.Common{Name: "rec", Type: schema.Object}
		_, err := normalizeForAvro("not a map", s, true)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "expected map[string]any for object")
	})

	t.Run("array wrong type", func(t *testing.T) {
		s := schema.Common{Name: "arr", Type: schema.Array, Children: []schema.Common{{Type: schema.Int32}}}
		_, err := normalizeForAvro("not a slice", s, true)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "expected []any for array")
	})

	t.Run("map wrong type", func(t *testing.T) {
		s := schema.Common{Name: "m", Type: schema.Map, Children: []schema.Common{{Type: schema.String}}}
		_, err := normalizeForAvro(42, s, true)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "expected map[string]any for map")
	})

	// --- Object ---

	t.Run("object recursion", func(t *testing.T) {
		s := schema.Common{
			Name: "rec",
			Type: schema.Object,
			Children: []schema.Common{
				{Name: "name", Type: schema.String},
				{Name: "age", Type: schema.Int32},
			},
		}
		data := map[string]any{"name": "alice", "age": float64(30)}
		result, err := normalizeForAvro(data, s, true)
		require.NoError(t, err)
		m := result.(map[string]any)
		assert.Equal(t, "alice", m["name"])
		assert.Equal(t, int32(30), m["age"])
	})

	t.Run("object missing optional child fills nil", func(t *testing.T) {
		s := schema.Common{
			Name: "rec",
			Type: schema.Object,
			Children: []schema.Common{
				{Name: "name", Type: schema.String},
				{Name: "nickname", Type: schema.String, Optional: true},
			},
		}
		data := map[string]any{"name": "alice"}
		result, err := normalizeForAvro(data, s, true)
		require.NoError(t, err)
		m := result.(map[string]any)
		assert.Equal(t, "alice", m["name"])
		assert.Nil(t, m["nickname"])
	})

	t.Run("object missing required child errors", func(t *testing.T) {
		s := schema.Common{
			Name: "rec",
			Type: schema.Object,
			Children: []schema.Common{
				{Name: "name", Type: schema.String},
				{Name: "age", Type: schema.Int32},
			},
		}
		data := map[string]any{"name": "alice"}
		_, err := normalizeForAvro(data, s, true)
		require.Error(t, err)
		assert.Contains(t, err.Error(), `required field "age" is missing`)
	})

	// --- Array ---

	t.Run("array recursion", func(t *testing.T) {
		s := schema.Common{
			Name:     "nums",
			Type:     schema.Array,
			Children: []schema.Common{{Type: schema.Int32}},
		}
		data := []any{float64(1), float64(2), float64(3)}
		result, err := normalizeForAvro(data, s, true)
		require.NoError(t, err)
		arr := result.([]any)
		assert.Equal(t, int32(1), arr[0])
		assert.Equal(t, int32(2), arr[1])
		assert.Equal(t, int32(3), arr[2])
	})

	// --- Map ---

	t.Run("map recursion", func(t *testing.T) {
		s := schema.Common{
			Name:     "kvs",
			Type:     schema.Map,
			Children: []schema.Common{{Type: schema.Int64}},
		}
		data := map[string]any{"a": float64(100), "b": float64(200)}
		result, err := normalizeForAvro(data, s, true)
		require.NoError(t, err)
		m := result.(map[string]any)
		assert.Equal(t, int64(100), m["a"])
		assert.Equal(t, int64(200), m["b"])
	})

	// --- Union ---

	t.Run("union rawJSON wraps first matching branch", func(t *testing.T) {
		s := schema.Common{
			Name: "val",
			Type: schema.Union,
			Children: []schema.Common{
				{Type: schema.Null},
				{Type: schema.String},
				{Type: schema.Int32},
			},
		}
		result, err := normalizeForAvro("hello", s, true)
		require.NoError(t, err)
		assert.Equal(t, map[string]any{"string": "hello"}, result)
	})

	t.Run("union rawJSON numeric matches int branch", func(t *testing.T) {
		s := schema.Common{
			Name: "val",
			Type: schema.Union,
			Children: []schema.Common{
				{Type: schema.Null},
				{Type: schema.String},
				{Type: schema.Int32},
			},
		}
		result, err := normalizeForAvro(float64(42), s, true)
		require.NoError(t, err)
		assert.Equal(t, map[string]any{"int": int32(42)}, result)
	})

	t.Run("union nil returns nil", func(t *testing.T) {
		s := schema.Common{
			Name: "val",
			Type: schema.Union,
			Children: []schema.Common{
				{Type: schema.Null},
				{Type: schema.String},
			},
		}
		result, err := normalizeForAvro(nil, s, true)
		require.NoError(t, err)
		assert.Nil(t, result)
	})

	t.Run("union no matching branch errors", func(t *testing.T) {
		s := schema.Common{
			Name: "val",
			Type: schema.Union,
			Children: []schema.Common{
				{Type: schema.Null},
				{Type: schema.Int32},
			},
		}
		_, err := normalizeForAvro("not a number", s, true)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no union branch matched")
	})

	t.Run("union non-rawJSON pre-wrapped", func(t *testing.T) {
		s := schema.Common{
			Name: "val",
			Type: schema.Union,
			Children: []schema.Common{
				{Type: schema.Null},
				{Type: schema.String},
				{Type: schema.Int32},
			},
		}
		result, err := normalizeForAvro(map[string]any{"string": "hello"}, s, false)
		require.NoError(t, err)
		assert.Equal(t, map[string]any{"string": "hello"}, result)
	})

	t.Run("union non-rawJSON pre-wrapped with coercion", func(t *testing.T) {
		s := schema.Common{
			Name: "val",
			Type: schema.Union,
			Children: []schema.Common{
				{Type: schema.Null},
				{Type: schema.Int32},
			},
		}
		result, err := normalizeForAvro(map[string]any{"int": float64(7)}, s, false)
		require.NoError(t, err)
		assert.Equal(t, map[string]any{"int": int32(7)}, result)
	})

	// --- Optional ---

	t.Run("optional non-null rawJSON wraps for BinaryFromNative", func(t *testing.T) {
		s := schema.Common{Name: "hobby", Type: schema.String, Optional: true}
		result, err := normalizeForAvro("reading", s, true)
		require.NoError(t, err)
		assert.Equal(t, map[string]any{"string": "reading"}, result)
	})

	t.Run("optional non-null non-rawJSON pre-wrapped", func(t *testing.T) {
		s := schema.Common{Name: "hobby", Type: schema.String, Optional: true}
		result, err := normalizeForAvro(map[string]any{"string": "dancing"}, s, false)
		require.NoError(t, err)
		assert.Equal(t, map[string]any{"string": "dancing"}, result)
	})

	t.Run("optional null", func(t *testing.T) {
		s := schema.Common{Name: "hobby", Type: schema.String, Optional: true}
		result, err := normalizeForAvro(nil, s, true)
		require.NoError(t, err)
		assert.Nil(t, result)

		result, err = normalizeForAvro(nil, s, false)
		require.NoError(t, err)
		assert.Nil(t, result)
	})

	t.Run("optional timestamp wraps with logical type key", func(t *testing.T) {
		ts := "2026-03-19T10:05:09Z"
		expected, err := time.Parse(time.RFC3339Nano, ts)
		require.NoError(t, err)

		s := schema.Common{Name: "created_at", Type: schema.Timestamp, Optional: true}
		result, err := normalizeForAvro(ts, s, true)
		require.NoError(t, err)
		assert.Equal(t, map[string]any{"long.timestamp-millis": expected}, result)
	})

	t.Run("optional int32 non-rawJSON pre-wrapped coerces inner", func(t *testing.T) {
		s := schema.Common{Name: "count", Type: schema.Int32, Optional: true}
		result, err := normalizeForAvro(map[string]any{"int": float64(99)}, s, false)
		require.NoError(t, err)
		assert.Equal(t, map[string]any{"int": int32(99)}, result)
	})

	t.Run("optional object non-rawJSON pre-wrapped", func(t *testing.T) {
		s := schema.Common{
			Name: "nested",
			Type: schema.Object,
			Children: []schema.Common{
				{Name: "x", Type: schema.Int32},
			},
			Optional: true,
		}
		result, err := normalizeForAvro(
			map[string]any{"nested": map[string]any{"x": float64(5)}},
			s, false,
		)
		require.NoError(t, err)
		wrapped := result.(map[string]any)
		inner := wrapped["nested"].(map[string]any)
		assert.Equal(t, int32(5), inner["x"])
	})

	// --- avroUnionKey ---

	t.Run("avroUnionKey covers all types", func(t *testing.T) {
		cases := []struct {
			s        schema.Common
			expected string
		}{
			{schema.Common{Type: schema.Boolean}, "boolean"},
			{schema.Common{Type: schema.Int32}, "int"},
			{schema.Common{Type: schema.Int64}, "long"},
			{schema.Common{Type: schema.Float32}, "float"},
			{schema.Common{Type: schema.Float64}, "double"},
			{schema.Common{Type: schema.String}, "string"},
			{schema.Common{Type: schema.ByteArray}, "bytes"},
			{schema.Common{Type: schema.Null}, "null"},
			{schema.Common{Type: schema.Timestamp}, "long.timestamp-millis"},
			{schema.Common{Name: "MyRecord", Type: schema.Object}, "MyRecord"},
			{schema.Common{Type: schema.Object}, "record"},
			{schema.Common{Type: schema.Array}, "array"},
			{schema.Common{Type: schema.Map}, "map"},
		}
		for _, c := range cases {
			assert.Equal(t, c.expected, avroUnionKey(c.s), "type %v", c.s.Type)
		}
	})

	// --- CDC-realistic record ---

	t.Run("CDC-realistic record", func(t *testing.T) {
		ts := time.Date(2026, 3, 19, 10, 5, 9, 934345000, time.UTC)
		s := schema.Common{
			Name: "products",
			Type: schema.Object,
			Children: []schema.Common{
				{Name: "id", Type: schema.Int32},
				{Name: "name", Type: schema.String},
				{Name: "in_stock", Type: schema.Boolean},
				{Name: "created_at", Type: schema.Timestamp, Optional: true},
			},
		}
		data := map[string]any{
			"id":         int64(42),
			"name":       "widget",
			"in_stock":   true,
			"created_at": ts,
		}
		result, err := normalizeForAvro(data, s, true)
		require.NoError(t, err)
		m := result.(map[string]any)
		assert.Equal(t, int32(42), m["id"])
		assert.Equal(t, "widget", m["name"])
		assert.Equal(t, true, m["in_stock"])
		assert.Equal(t, map[string]any{"long.timestamp-millis": ts}, m["created_at"])
	})

	// --- JSON-realistic record ---

	t.Run("JSON-realistic record", func(t *testing.T) {
		s := schema.Common{
			Name: "products",
			Type: schema.Object,
			Children: []schema.Common{
				{Name: "id", Type: schema.Int32},
				{Name: "name", Type: schema.String},
				{Name: "price", Type: schema.Float64},
				{Name: "created_at", Type: schema.Timestamp, Optional: true},
			},
		}
		data := map[string]any{
			"id":         float64(79),
			"name":       "gadget",
			"price":      float64(29.99),
			"created_at": "2026-03-19T10:05:09.934345Z",
		}
		expectedTs, err := time.Parse(time.RFC3339Nano, "2026-03-19T10:05:09.934345Z")
		require.NoError(t, err)

		result, err := normalizeForAvro(data, s, true)
		require.NoError(t, err)
		m := result.(map[string]any)
		assert.Equal(t, int32(79), m["id"])
		assert.Equal(t, "gadget", m["name"])
		assert.Equal(t, float64(29.99), m["price"])
		assert.Equal(t, map[string]any{"long.timestamp-millis": expectedTs}, m["created_at"])
	})
}

func TestNormalizeForAvroRoundTrip(t *testing.T) {
	common := schema.Common{
		Name: "TestRecord",
		Type: schema.Object,
		Children: []schema.Common{
			{Name: "name", Type: schema.String},
			{Name: "age", Type: schema.Int32},
			{Name: "score", Type: schema.Int64},
			{Name: "active", Type: schema.Boolean},
			{Name: "rating", Type: schema.Float32},
			{Name: "factor", Type: schema.Float64},
			{Name: "blob", Type: schema.ByteArray},
			{Name: "created_at", Type: schema.Timestamp, Optional: true},
			{Name: "deleted_at", Type: schema.Timestamp, Optional: true},
			{Name: "tags", Type: schema.Array, Children: []schema.Common{
				{Type: schema.Int32},
			}},
			{Name: "labels", Type: schema.Map, Children: []schema.Common{
				{Type: schema.String},
			}},
		},
	}

	avroJSON, err := commonToAvroSchema(common, "TestRecord", "test.ns")
	require.NoError(t, err)

	codec, err := goavro.NewCodecForStandardJSONFull(avroJSON)
	require.NoError(t, err)

	ts := time.Date(2026, 3, 19, 10, 0, 0, 0, time.UTC)
	data := map[string]any{
		"name":       "alice",
		"age":        float64(30),
		"score":      float64(99999),
		"active":     true,
		"rating":     float64(4.5),
		"factor":     float64(1.23456789),
		"blob":       "binary-data",
		"created_at": "2026-03-19T10:00:00Z",
		"deleted_at": nil,
		"tags":       []any{float64(1), float64(2), float64(3)},
		"labels":     map[string]any{"env": "prod", "region": "us"},
	}

	normalized, err := normalizeForAvro(data, common, true)
	require.NoError(t, err)

	binary, err := codec.BinaryFromNative(nil, normalized)
	require.NoError(t, err)
	require.NotEmpty(t, binary)

	native, _, err := codec.NativeFromBinary(binary)
	require.NoError(t, err)

	m, ok := native.(map[string]any)
	require.True(t, ok)

	assert.Equal(t, "alice", m["name"])
	assert.Equal(t, int32(30), m["age"])
	assert.Equal(t, int64(99999), m["score"])
	assert.Equal(t, true, m["active"])
	assert.InDelta(t, float32(4.5), m["rating"], 0.01)
	assert.InDelta(t, float64(1.23456789), m["factor"], 0.0001)
	assert.Equal(t, []byte("binary-data"), m["blob"])

	// goavro decodes timestamp-millis as time.Time in StandardJSONFull mode.
	decodedTs, ok := m["created_at"].(map[string]any)
	require.True(t, ok)
	actualTs, ok := decodedTs["long.timestamp-millis"].(time.Time)
	require.True(t, ok)
	assert.True(t, ts.Equal(actualTs), "expected %v, got %v", ts, actualTs)

	// Null optional should round-trip as nil.
	assert.Nil(t, m["deleted_at"])

	decodedTags, ok := m["tags"].([]any)
	require.True(t, ok)
	assert.Len(t, decodedTags, 3)

	decodedLabels, ok := m["labels"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "prod", decodedLabels["env"])
	assert.Equal(t, "us", decodedLabels["region"])
}
