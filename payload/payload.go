package payload

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"

	"github.com/moleculer-go/moleculer"
)

// RawPayload is a payload implementation for raw types.
type RawPayload struct {
	source interface{}
}

func (rawPayload *RawPayload) Exists() bool {
	return rawPayload.source != nil
}

func (rawPayload *RawPayload) IsError() bool {
	valueType := GetValueType(&rawPayload.source)
	return valueType == "*errors.errorString"
}

func (rawPayload *RawPayload) Error() error {
	if rawPayload.IsError() {
		return rawPayload.source.(error)
	}
	return nil
}

func (rawPayload *RawPayload) Int() int {
	value, ok := rawPayload.source.(int)
	if !ok {
		if transformer := getNumberTransformer(&rawPayload.source); transformer != nil {
			value = transformer.toInt(&rawPayload.source)
		}
	}
	return value
}

func (rawPayload *RawPayload) Int64() int64 {
	value, ok := rawPayload.source.(int64)
	if !ok {
		if transformer := getNumberTransformer(&rawPayload.source); transformer != nil {
			value = transformer.toInt64(&rawPayload.source)
		}
	}
	return value
}

func (rawPayload *RawPayload) Bool() bool {
	value, ok := rawPayload.source.(bool)
	if !ok {
		value = strings.ToLower(fmt.Sprint(rawPayload.source)) == "true"
	}
	return value
}

func (rawPayload *RawPayload) Uint() uint64 {
	value, ok := rawPayload.source.(uint64)
	if !ok {
		if transformer := getNumberTransformer(&rawPayload.source); transformer != nil {
			value = transformer.toUint64(&rawPayload.source)
		}
	}
	return value
}

func (rawPayload *RawPayload) Time() time.Time {
	return rawPayload.source.(time.Time)
}

func (rawPayload *RawPayload) StringArray() []string {
	if source := rawPayload.Array(); source != nil {
		array := make([]string, len(source))
		for index, item := range source {
			array[index] = item.String()
		}
		return array
	}
	return nil
}

func (rawPayload *RawPayload) MapArray() []map[string]interface{} {
	if source := rawPayload.Array(); source != nil {
		array := make([]map[string]interface{}, len(source))
		for index, item := range source {
			array[index] = item.RawMap()
		}
		return array
	}
	return nil
}

func (rawPayload *RawPayload) ValueArray() []interface{} {
	if source := rawPayload.Array(); source != nil {
		array := make([]interface{}, len(source))
		for index, item := range source {
			array[index] = item.Value()
		}
		return array
	}
	return nil
}

func (rawPayload *RawPayload) IntArray() []int {
	if source := rawPayload.Array(); source != nil {
		array := make([]int, len(source))
		for index, item := range source {
			array[index] = item.Int()
		}
		return array
	}
	return nil
}

func (rawPayload *RawPayload) Int64Array() []int64 {
	if source := rawPayload.Array(); source != nil {
		array := make([]int64, len(source))
		for index, item := range source {
			array[index] = item.Int64()
		}
		return array
	}
	return nil
}

func (rawPayload *RawPayload) UintArray() []uint64 {
	if source := rawPayload.Array(); source != nil {
		array := make([]uint64, len(source))
		for index, item := range source {
			array[index] = item.Uint()
		}
		return array
	}
	return nil
}

func (rawPayload *RawPayload) Float32Array() []float32 {
	if source := rawPayload.Array(); source != nil {
		array := make([]float32, len(source))
		for index, item := range source {
			array[index] = item.Float32()
		}
		return array
	}
	return nil
}

func (rawPayload *RawPayload) FloatArray() []float64 {
	if source := rawPayload.Array(); source != nil {
		array := make([]float64, len(source))
		for index, item := range source {
			array[index] = item.Float()
		}
		return array
	}
	return nil
}

func (rawPayload *RawPayload) BoolArray() []bool {
	if source := rawPayload.Array(); source != nil {
		array := make([]bool, len(source))
		for index, item := range source {
			array[index] = item.Bool()
		}
		return array
	}
	return nil
}

func (rawPayload *RawPayload) TimeArray() []time.Time {
	if source := rawPayload.Array(); source != nil {
		array := make([]time.Time, len(source))
		for index, item := range source {
			array[index] = item.Time()
		}
		return array
	}
	return nil
}

func (rawPayload *RawPayload) Array() []moleculer.Payload {
	if transformer := ArrayTransformer(&rawPayload.source); transformer != nil {
		source := transformer.InterfaceArray(&rawPayload.source)
		array := make([]moleculer.Payload, len(source))
		for index, item := range source {
			array[index] = Create(item)
		}
		return array
	}
	return nil
}

func (rawPayload *RawPayload) ForEach(iterator func(key interface{}, value moleculer.Payload) bool) {
	if rawPayload.IsArray() {
		list := rawPayload.Array()
		for index, value := range list {
			if !iterator(index, value) {
				break
			}
		}
	} else if rawPayload.IsMap() {
		mapValue := rawPayload.Map()
		for key, value := range mapValue {
			if !iterator(key, value) {
				break
			}
		}
	} else {
		iterator(nil, rawPayload)
	}
}

func (rawPayload *RawPayload) IsArray() bool {
	transformer := ArrayTransformer(&rawPayload.source)
	return transformer != nil
}

func (rawPayload *RawPayload) IsMap() bool {
	transformer := MapTransformer(&rawPayload.source)
	return transformer != nil
}

func (rawPayload *RawPayload) Float() float64 {
	value, ok := rawPayload.source.(float64)
	if !ok {
		if transformer := getNumberTransformer(&rawPayload.source); transformer != nil {
			value = transformer.toFloat64(&rawPayload.source)
		}
	}
	return value
}

func (rawPayload *RawPayload) Float32() float32 {
	value, ok := rawPayload.source.(float32)
	if !ok {
		if transformer := getNumberTransformer(&rawPayload.source); transformer != nil {
			value = transformer.toFloat32(&rawPayload.source)
		}
	}
	return value
}

func orderedKeys(m map[string]moleculer.Payload) []string {
	keys := make([]string, len(m))
	i := 0
	for key := range m {
		keys[i] = key
		i++
	}
	sort.Strings(keys)
	return keys
}

func (raw *RawPayload) String() string {
	ident := "  "
	if raw.IsMap() {
		m := raw.Map()

		out := "(len=" + strconv.Itoa(len(m)) + ") {\n"
		for _, key := range orderedKeys(m) {
			out = out + ident + `"` + key + `": ` + m[key].String() + "," + "\n"
		}
		if len(m) == 0 {
			out = out + "\n"
		}
		out = out + "}"
		return out
	}
	rawString, ok := raw.source.(string)
	if !ok {
		return fmt.Sprintf("%v", raw.source)
	}
	return rawString
}

func (rawPayload *RawPayload) Map() map[string]moleculer.Payload {
	if transformer := MapTransformer(&rawPayload.source); transformer != nil {
		source := transformer.AsMap(&rawPayload.source)
		newMap := make(map[string]moleculer.Payload, len(source))
		for key, item := range source {
			newPayload := RawPayload{item}
			newMap[key] = &newPayload
		}
		return newMap
	}
	return nil
}

func (rawPayload *RawPayload) RawMap() map[string]interface{} {
	if transformer := MapTransformer(&rawPayload.source); transformer != nil {
		return transformer.AsMap(&rawPayload.source)
	}
	return nil
}

func (raw *RawPayload) Bson() bson.M {
	valueType := GetValueType(&raw.source)
	if valueType == "bson.M" {
		return raw.source.(bson.M)
	}
	if raw.IsMap() {
		bm := bson.M{}
		for key, value := range raw.Map() {
			if value.IsMap() {
				bm[key] = value.Bson()
			} else {
				bm[key] = value.Value()
			}
		}
		return bm
	}
	return nil
}

// mapGet try to get the value at the path assuming the source is a map
func (rawPayload *RawPayload) mapGet(path string) (interface{}, bool) {
	if transformer := MapTransformer(&rawPayload.source); transformer != nil {
		return transformer.get(path, &rawPayload.source)
	}
	return nil, false
}

func (rawPayload *RawPayload) Get(path string) moleculer.Payload {
	if value, ok := rawPayload.mapGet(path); ok {
		return Create(value)
	}
	return Create(nil)
}

func (rawPayload *RawPayload) Value() interface{} {
	return rawPayload.source
}

func (rawPayload *RawPayload) Merge(toAdd map[string]interface{}) moleculer.Payload {
	m := rawPayload.RawMap()
	for key, value := range toAdd {
		m[key] = value
	}
	return Create(m)
}

//Merge merge paylaod with new fields/values in a map.
func Merge(in moleculer.Payload, toAdd map[string]interface{}) moleculer.Payload {
	if !in.IsMap() {
		return Error("payload.Merge can only accept map payloads.")
	}
	return in.Merge(toAdd)
}

func Error(msgs ...interface{}) moleculer.Payload {
	return Create(errors.New(fmt.Sprint(msgs...)))
}

func Create(source interface{}) moleculer.Payload {
	valueType := GetValueType(&source)
	if valueType == "*payload.RawPayload" {
		return source.(moleculer.Payload)
	} else if valueType == "serializer.JSONPayload" {
		//TODO make this flexible to other factories can be created for custom types
		return source.(moleculer.Payload)
	}
	return &RawPayload{source}
}
