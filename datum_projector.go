package avro

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
)

func NewDatumProjector(readerSchema, writerSchema Schema) *DatumProjector {
	//TODO check cache for the same combination of schemas
	return &DatumProjector{
		projector: newProjector(readerSchema, writerSchema),
	}
}

type DatumProjector struct {
	projector projector
}

func (reader *DatumProjector) Read(target interface{}, dec Decoder) error {
	rv := reflect.ValueOf(target)
	if rv.Kind() != reflect.Ptr || rv.IsNil() {
		return errors.New("not applicable for non-pointer types or nil")
	}
	return reader.projector.Project(rv, dec)
}

type projector interface {
	Project(target reflect.Value, dec Decoder) error
	Unwrap(dec Decoder) (interface{}, error)
}

//default projector relies wholly on Unwrap function - it is used for primitive values
type defaultProjector struct {
	unwrap func(dec Decoder) (interface{}, error)
}

func (p *defaultProjector) Project(target reflect.Value, dec Decoder) error {
	if v, err := p.Unwrap(dec); err != nil {
		return err
	} else if v != nil {
		if target.Kind() == reflect.Ptr {
			target.Set(reflect.New(target.Type().Elem()))
			target = target.Elem()
		}
		rv := reflect.ValueOf(v)
		if rv.IsValid() {
			target.Set(rv)
		}
	}
	return nil
}

func (p *defaultProjector) Unwrap(dec Decoder) (interface{}, error) {
	return p.unwrap(dec)
}

func newProjector(readerSchema, writerSchema Schema) projector {

	if writerSchema.Type() == Union {
		if readerSchema.Type() == Union {
			writerUnionSchema := writerSchema.(*UnionSchema)
			variants := make(map[int32]projector)
			for i, t := range writerUnionSchema.Types {
				variants[int32(i)] = newProjector(t, t)
			}
			return newUnionProjector(variants)
		} else {
			for i, t := range writerSchema.(*UnionSchema).Types {
				if t.Type() == readerSchema.Type() && t.GetName() == readerSchema.GetName() {
					variants := make(map[int32]projector)
					variants[int32(i)] = newProjector(readerSchema, t)
					return newUnionProjector(variants)
				}
			}
			panic(fmt.Errorf("writer Union does not match reader schema: %v", readerSchema))
		}
	} else if readerSchema.Type() == Union {
		for _, t := range readerSchema.(*UnionSchema).Types {
			if t.Type() == writerSchema.Type() && t.GetName() == writerSchema.GetName() {
				return newProjector(t, writerSchema)
			}
		}
	}

	switch readerSchema.Type() {
	case Null:
		switch writerSchema.Type() {
		case Null:
			return &defaultProjector{
				func(dec Decoder) (interface{}, error) {
				return nil, nil
			}}
		default:
			panic(fmt.Errorf("impossible projection from %q to %q", writerSchema, readerSchema))
		}
	case Boolean:
		switch writerSchema.Type() {
		case Boolean:
			return &defaultProjector{
				func(dec Decoder) (interface{}, error) {
				return dec.ReadBoolean()
			}}
		default:
			panic(fmt.Errorf("impossible projection from %q to %q", writerSchema, readerSchema))
		}

	case Int:
		switch writerSchema.Type() {
		case Int:
			return &defaultProjector{
				func(dec Decoder) (interface{}, error) {
				return dec.ReadInt()
			}}
		default:
			panic(fmt.Errorf("impossible projection from %q to %q", writerSchema, readerSchema))
		}

	case Long:
		switch writerSchema.Type() {
		case Long:
			return &defaultProjector{
				func(dec Decoder) (interface{}, error) {
				return dec.ReadLong()
			}}
		case Int:
			return &defaultProjector{
				func(dec Decoder) (interface{}, error) {
				v, err := dec.ReadInt()
				return int64(v), err
			}}
		default:
			panic(fmt.Errorf("impossible projection from %q to %q", writerSchema, readerSchema))
		}
	case Float:
		switch writerSchema.Type() {
		case Float:
			return &defaultProjector{
				func(dec Decoder) (interface{}, error) {
				return dec.ReadFloat()
			}}
		case Int:
			return &defaultProjector{
				func(dec Decoder) (interface{}, error) {
				v, err := dec.ReadInt()
				return float32(v), err
			}}
		case Long:
			return &defaultProjector{
				func(dec Decoder) (interface{}, error) {
				v, err := dec.ReadLong()
				return float32(v), err
			}}
		default:
			panic(fmt.Errorf("impossible projection from %q to %q", writerSchema, readerSchema))
		}

	case Double:
		switch writerSchema.Type() {
		case Double:
			return &defaultProjector{
				func(dec Decoder) (interface{}, error) {
				return dec.ReadDouble()
			}}
		case Int:
			return &defaultProjector{
				func(dec Decoder) (interface{}, error) {
				v, err := dec.ReadInt()
				return float64(v), err
			}}
		case Long:
			return &defaultProjector{
				func(dec Decoder) (interface{}, error) {
				v, err := dec.ReadLong()
				return float64(v), err
			}}
		case Float:
			return &defaultProjector{
				func(dec Decoder) (interface{}, error) {
				v, err := dec.ReadFloat()
				return float64(v), err
			}}
		default:
			panic(fmt.Errorf("impossible projection from %q to %q", writerSchema, readerSchema))
		}
	case Bytes:
		switch writerSchema.Type() {
		case Bytes:
			return &defaultProjector{
				func(dec Decoder) (interface{}, error) {
				return dec.ReadBytes()
			}}
		case String:
			return &defaultProjector{
				func(dec Decoder) (interface{}, error) {
				v, err := dec.ReadString()
				return []byte(v), err
			}}
		default:
			panic(fmt.Errorf("impossible projection from %q to %q", writerSchema, readerSchema))
		}

	case String:
		switch writerSchema.Type() {
		case String:
			return &defaultProjector{
				func(dec Decoder) (interface{}, error) {
				return dec.ReadString()
			}}
		case Bytes:
			return &defaultProjector{
				func(dec Decoder) (interface{}, error) {
				v, err := dec.ReadBytes()
				return string(v), err
			}}
		default:
			panic(fmt.Errorf("impossible projection from %q to %q", writerSchema, readerSchema))
		}
	case Fixed:
		size := writerSchema.(*FixedSchema).Size
		switch {
		case writerSchema.Type() == Fixed && readerSchema.(*FixedSchema).Size == size:
			return &defaultProjector{
				func(dec Decoder) (interface{}, error) {
				fixed := make([]byte, size)
				return fixed, dec.ReadFixed(fixed)
			}}
		default:
			panic(fmt.Errorf("impossible projection from %q to %q", writerSchema, readerSchema))
		}
	case Enum:
		switch writerSchema.Type() {
		case Enum:
			//TODO once there is an efficient schema fingerprint we can first compare the 2 enums to see whether they are same before applying expensive projection below
			return &defaultProjector{
				func(dec Decoder) (interface{}, error) {
				if enumIndex, err := dec.ReadEnum(); err != nil {
					return nil, err
				} else {
					writerSymbol := writerSchema.(*EnumSchema).Symbols[enumIndex]
					//TODO it would be good to have a symbol-to-index available in EnumSchema instead of seeking for each record
					for r, readerSymbol := range readerSchema.(*EnumSchema).Symbols {
						if readerSymbol == writerSymbol {
							return r, nil
						}
					}
					return nil, fmt.Errorf("reader enum schema doesn't contain )")
				}
			}}
		default:
			panic(fmt.Errorf("impossible projection from %q to %q", writerSchema, readerSchema))
		}

	case Array:
		readerArraySchema := readerSchema.(*ArraySchema)
		switch writerSchema.Type() {
		case Array:
			writerArraySchema := writerSchema.(*ArraySchema)
			return newArrayProjector(readerArraySchema, writerArraySchema)
		default:
			panic(fmt.Errorf("impossible projection from %q to %q", writerSchema, readerSchema))
		}

	case Map:
		readerMapSchema := readerSchema.(*MapSchema)
		switch writerSchema.Type() {
		case Map:
			writerMapSchema := writerSchema.(*MapSchema)
			return newMapProjector(readerMapSchema, writerMapSchema)

		default:
			panic(fmt.Errorf("impossible projection from %q to %q", writerSchema, readerSchema))
		}
	case Record:
		readerRecordSchema := readerSchema.(*RecordSchema)
		switch writerSchema.Type() {
		case Record:
			writerRecordSchema := writerSchema.(*RecordSchema)
			return newRecordProjector(readerRecordSchema, writerRecordSchema)
		default:
			panic(fmt.Errorf("impossible projection from %q to %q", writerSchema, readerSchema))
		}
	case Recursive:
		rs := readerSchema.(*RecursiveSchema).Actual
		switch writerSchema.Type() {
		case Record:
			return newProjector(rs, writerSchema.(*RecordSchema))
		case Recursive:
			return newProjector(rs, writerSchema.(*RecursiveSchema).Actual)
		default:
			panic(fmt.Errorf("impossible recurive schema projection: %v => %v", writerSchema.GetName(), readerSchema.GetName()))
		}

	default:
		panic(fmt.Errorf("not Implemented type: %v", readerSchema))
	}
}

func newUnionProjector(variants map[int32]projector) projector {
	return &unionProjector{
		variants: variants,
	}
}

type unionProjector struct {
	variants map[int32]projector
}

func (p *unionProjector) Unwrap(dec Decoder) (interface{}, error) {
	unionIndex, err := dec.ReadInt()
	if err != nil {
		return reflect.ValueOf(unionIndex), err
	}
	if p, ok := p.variants[unionIndex]; ! ok {
		return reflect.Value{}, fmt.Errorf("invalid union index %d", unionIndex)
	} else {
		return p.Unwrap(dec)
	}

}
func (p *unionProjector) Project(target reflect.Value, dec Decoder) error {
	unionIndex, err := dec.ReadInt()
	if err != nil {
		return err
	}
	if p, ok := p.variants[unionIndex]; ok {
		result := p.Project(target, dec)
		return result
	} else {
		return fmt.Errorf("invalid union index %d", unionIndex)
	}
}

func newArrayProjector(readerArraySchema *ArraySchema, writerArraySchema *ArraySchema) projector {
	return &arrayProjector{
		itemProjector: newProjector(readerArraySchema.Items, writerArraySchema.Items),
	}
}

type arrayProjector struct {
	itemProjector projector
}

func (p *arrayProjector) Unwrap(dec Decoder) (interface{}, error) {
	arrayLength, err := dec.ReadArrayStart()
	if err != nil {
		return nil, err
	}

	var array []interface{}
	for arrayLength > 0 {
		arrayPart := make([]interface{}, arrayLength, arrayLength)
		var i int64
		for ; i < arrayLength; i++ {
			val, err := p.itemProjector.Unwrap(dec)
			if err != nil {
				return nil, err
			}
			arrayPart[i] = val
		}
		concatArray := make([]interface{}, len(array)+int(arrayLength), cap(array)+int(arrayLength))
		copy(concatArray, array)
		copy(concatArray, arrayPart)
		array = concatArray
		arrayLength, err = dec.ArrayNext()
		if err != nil {
			return nil, err
		}
	}
	return array, nil
}

func (p *arrayProjector) Project(target reflect.Value, dec Decoder) error {
	arrayLength, err := dec.ReadArrayStart()
	if err != nil {
		return err
	}
	indirectArrayType := target.Type()
	if indirectArrayType.Kind() == reflect.Ptr {
		indirectArrayType = indirectArrayType.Elem()
	}
	array := reflect.MakeSlice(indirectArrayType, 0, 0)
	for arrayLength > 0 {
		arrayPart := reflect.MakeSlice(indirectArrayType, int(arrayLength), int(arrayLength))
		var i int64
		for ; i < arrayLength; i++ {
			current := arrayPart.Index(int(i))
			if err := p.itemProjector.Project(current, dec); err != nil {
				return err
			}
		}
		if array.Len() == 0 {
			array = arrayPart
		} else {
			array = reflect.AppendSlice(array, arrayPart)
		}
		arrayLength, err = dec.ArrayNext()
		if err != nil {
			return err
		}
	}
	target.Set(array)
	return nil
}

func newMapProjector(readerMapSchema *MapSchema, writerMapSchema *MapSchema) projector {
	return &mapProjector{
		keyProjector:   newProjector(&StringSchema{}, &StringSchema{}),
		valueProjector: newProjector(readerMapSchema.Values, writerMapSchema.Values),
	}
}

type mapProjector struct {
	keyProjector   projector
	valueProjector projector
}

func (p *mapProjector) Unwrap(dec Decoder) (interface{}, error) {
	mapLength, err := dec.ReadMapStart()
	if err != nil {
		return nil, err
	}
	resultMap := make(map[string]interface{}, mapLength)
	for mapLength > 0 {
		var i int64
		for ; i < mapLength; i++ {
			if key, err := p.keyProjector.Unwrap(dec); err != nil {
				return nil, err
			} else if val, err := p.valueProjector.Unwrap(dec); err != nil {
				return nil, err
			} else {
				resultMap[key.(string)] = val
			}
		}

		mapLength, err = dec.MapNext()

	}
	return resultMap, nil
}

func (p *mapProjector) Project(target reflect.Value, dec Decoder) error {
	mapLength, err := dec.ReadMapStart()
	if err != nil {
		return err
	}
	elemType := target.Type().Elem()
	elemIsPointer := elemType.Kind() == reflect.Ptr
	indirectMapType := target.Type()
	if indirectMapType.Kind() == reflect.Ptr {
		indirectMapType = indirectMapType.Elem()
	}
	resultMap := reflect.MakeMap(indirectMapType)
	for mapLength > 0 {
		var i int64
		for ; i < mapLength; i++ {
			k := ""
			key := reflect.ValueOf(&k).Elem()
			v := reflect.New(target.Type().Elem()).Interface()
			val := reflect.ValueOf(v).Elem()
			if err := p.keyProjector.Project(key, dec); err != nil {
				return err
			} else if err := p.valueProjector.Project(val, dec); err != nil {
				return err
			} else {
				if !elemIsPointer && val.Kind() == reflect.Ptr {
					resultMap.SetMapIndex(key, val.Elem())
				} else {
					resultMap.SetMapIndex(key, val)
				}
			}
		}

		mapLength, err = dec.MapNext()

	}
	target.Set(resultMap)
	return nil
}

func newRecordProjector(readerRecordSchema, writerRecordSchema *RecordSchema) projector {
	p := &RecordProjector{
		defaultUnwrapperMap: make(map[string]interface{}, 0),
		defaultIndexMap:     make(map[string]reflect.Value, 0),
		projectNameMap:      make([]string, len(writerRecordSchema.Fields)),
		projectIndexMap:     make([]projector, len(writerRecordSchema.Fields)),
	}

NextReaderField:

	//prepare field projectors
	for w, writerField := range writerRecordSchema.Fields {
		//match by name
		for _, readerField := range readerRecordSchema.Fields {
			if writerField.Name == readerField.Name {
				p.defaultIndexMap[readerField.Name] = reflect.ValueOf(nil)
				p.projectNameMap[w] = readerField.Name
				p.projectIndexMap[w] = newProjector(readerField.Type, writerField.Type)
				continue NextReaderField
			}
		}
		//match by alias
		for _, readerField := range readerRecordSchema.Fields {
			for _, intoFieldAlias := range readerField.Aliases {
				if writerField.Name == intoFieldAlias {
					p.defaultIndexMap[readerField.Name] = reflect.ValueOf(nil)
					p.projectNameMap[w] = readerField.Name
					p.projectIndexMap[w] = newProjector(readerField.Type, writerField.Type)
					continue NextReaderField
				}
			}
		}
		//removed fields
		p.projectIndexMap[w] = newProjector(writerField.Type, writerField.Type)
	}

	//prepare default values
	for _, readerField := range readerRecordSchema.Fields {
		if _, ok := p.defaultIndexMap[readerField.Name]; !ok {
			//TODO converting default values to native is now a function of Schema in another branch

			var defaultValue reflect.Value
			switch readerField.Type.Type() {
			case Array:
				a := readerField.Default.([]interface{})
				if len(a) > 0 {
					switch readerField.Type.(*ArraySchema).Items.Type() {
					case Long:
						defaultValue = reflect.MakeSlice(reflect.SliceOf(reflect.TypeOf(int64(0))), len(a), len(a))
						switch reflect.TypeOf(a[0]).Kind() {
						case reflect.Float64:
							for i, x := range a {
								defaultValue.Index(i).Set(reflect.ValueOf(int64(x.(float64))))
							}
						default:
							panic(fmt.Errorf("TODO default converter from %q", reflect.TypeOf(a[0])))
						}
					default:
						panic(fmt.Errorf("TODO default converter to %q", readerField.Type.(*ArraySchema).Items))
					}

				}
			default:
				defaultValue = reflect.ValueOf(readerField.Default)
			}
			p.defaultIndexMap[readerField.Name] = defaultValue
			if defaultValue.IsValid() {
				p.defaultUnwrapperMap[readerField.Name] = defaultValue.Interface()
			}
		} else {
			delete(p.defaultIndexMap, readerField.Name)
		}
	}

	return p
}

type RecordProjector struct {
	readerRecordSchema  *RecordSchema
	writerRecordSchema  *RecordSchema
	defaultUnwrapperMap map[string]interface{}
	defaultIndexMap     map[string]reflect.Value
	projectNameMap      []string
	projectIndexMap     []projector
}

func (p *RecordProjector) Unwrap(dec Decoder) (interface{}, error) {
	record := NewGenericRecord(p.readerRecordSchema)
	for f := range p.projectIndexMap {
		if writerValue, err := p.projectIndexMap[f].Unwrap(dec); err != nil {
			return nil, err
		} else if writerValue != nil {
			if p.projectNameMap[f] != "" { //deleted fields don't have a mapped name
				record.Set(p.projectNameMap[f], writerValue)
			}
		}
	}
	if len(p.defaultIndexMap) > 0 {
		for d := range p.defaultUnwrapperMap {
			record.Set(d, p.defaultUnwrapperMap[d])
		}
	}
	return record, nil
}

func (p *RecordProjector) Project(target reflect.Value, dec Decoder) error {
	if target.Kind() == reflect.Ptr && !target.Elem().IsValid() {
		target.Set(reflect.New(target.Type().Elem()))
	}
	target = dereference(target)
	switch target.Interface().(type) {
	case GenericRecord:
		record := target.Interface().(GenericRecord)
		for f := range p.projectIndexMap {
			if writerValue, err := p.projectIndexMap[f].Unwrap(dec); err != nil {
				return err
			} else if writerValue != nil {
				if p.projectNameMap[f] != "" { //deleted fields don't have a mapped name
					record.Set(p.projectNameMap[f], writerValue)
				}
			}
		}
		if len(p.defaultIndexMap) > 0 {
			for d := range p.defaultUnwrapperMap {
				record.Set(d, p.defaultUnwrapperMap[d])
			}
		}
	default:
		for f := range p.projectIndexMap {
			structField := target.FieldByName(p.projectNameMap[f])
			if !structField.IsValid() {
				structField = target.FieldByName(strings.Title(p.projectNameMap[f]))
				if !structField.IsValid() {
					p.projectIndexMap[f].Unwrap(dec) //still have to read deleted fields from the writer value
					continue
				}
			}
			if err := p.projectIndexMap[f].Project(structField, dec); err != nil {
				return err
			}
		}
		if len(p.defaultIndexMap) > 0 {
			for d := range p.defaultIndexMap {
				if field := target.FieldByName(d); field.IsValid() {
					field.Set(p.defaultIndexMap[d])
				} else {
					if field = target.FieldByName(strings.Title(d)); field.IsValid() && p.defaultIndexMap[d].IsValid() {
						//default value is converted in case it is a type alias
						field.Set(p.defaultIndexMap[d].Convert(field.Type()))
					}
				}
			}
		}
	}
	return nil
}
