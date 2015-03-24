package avro

import (
	"encoding/json"
	"fmt"
)

const (
	Record int = iota
	Enum
	Array
	Map
	Union
	Fixed
	String
	Bytes
	Int
	Long
	Float
	Double
	Boolean
	Null
	Recursive
)

const (
	type_record  = "record"
	type_union   = "union"
	type_enum    = "enum"
	type_array   = "array"
	type_map     = "map"
	type_fixed   = "fixed"
	type_string  = "string"
	type_bytes   = "bytes"
	type_int     = "int"
	type_long    = "long"
	type_float   = "float"
	type_double  = "double"
	type_boolean = "boolean"
	type_null    = "null"
)

const (
	schema_aliasesField   = "aliases"
	schema_defaultField   = "default"
	schema_docField       = "doc"
	schema_fieldsField    = "fields"
	schema_itemsField     = "items"
	schema_nameField      = "name"
	schema_namespaceField = "namespace"
	schema_sizeField      = "size"
	schema_symbolsField   = "symbols"
	schema_typeField      = "type"
	schema_valuesField    = "values"
)

// Schema is an interface representing a single Avro schema (both primitive and complex).
type Schema interface {
	// Returns an integer constant representing this schema type.
	Type() int

	// If this is a record, enum or fixed, returns its name, otherwise the name of the primitive type.
	GetName() string

	// Gets a custom non-reserved string property from this schema and a bool representing if it exists.
	Prop(key string) (string, bool)

	// Converts this schema to its JSON representation.
	String() string
}

// PRIMITIVES
type StringSchema struct{}

func (*StringSchema) String() string {
	return `{"type": "string"}`
}

func (*StringSchema) Type() int {
	return String
}

func (*StringSchema) GetName() string {
	return type_string
}

func (*StringSchema) Prop(key string) (string, bool) {
	return "", false
}

func (this *StringSchema) MarshalJSON() ([]byte, error) {
	return []byte(`"string"`), nil
}

type BytesSchema struct{}

func (*BytesSchema) String() string {
	return `{"type": "bytes"}`
}

func (*BytesSchema) Type() int {
	return Bytes
}

func (*BytesSchema) GetName() string {
	return type_bytes
}

func (*BytesSchema) Prop(key string) (string, bool) {
	return "", false
}

func (this *BytesSchema) MarshalJSON() ([]byte, error) {
	return []byte(`"bytes"`), nil
}

type IntSchema struct{}

func (*IntSchema) String() string {
	return `{"type": "int"}`
}

func (*IntSchema) Type() int {
	return Int
}

func (*IntSchema) GetName() string {
	return type_int
}

func (*IntSchema) Prop(key string) (string, bool) {
	return "", false
}

func (this *IntSchema) MarshalJSON() ([]byte, error) {
	return []byte(`"int"`), nil
}

type LongSchema struct{}

func (*LongSchema) String() string {
	return `{"type": "long"}`
}

func (*LongSchema) Type() int {
	return Long
}

func (*LongSchema) GetName() string {
	return type_long
}

func (*LongSchema) Prop(key string) (string, bool) {
	return "", false
}

func (this *LongSchema) MarshalJSON() ([]byte, error) {
	return []byte(`"long"`), nil
}

type FloatSchema struct{}

func (*FloatSchema) String() string {
	return `{"type": "float"}`
}

func (*FloatSchema) Type() int {
	return Float
}

func (*FloatSchema) GetName() string {
	return type_float
}

func (*FloatSchema) Prop(key string) (string, bool) {
	return "", false
}

func (this *FloatSchema) MarshalJSON() ([]byte, error) {
	return []byte(`"float"`), nil
}

type DoubleSchema struct{}

func (*DoubleSchema) String() string {
	return `{"type": "double"}`
}

func (*DoubleSchema) Type() int {
	return Double
}

func (*DoubleSchema) GetName() string {
	return type_double
}

func (*DoubleSchema) Prop(key string) (string, bool) {
	return "", false
}

func (this *DoubleSchema) MarshalJSON() ([]byte, error) {
	return []byte(`"double"`), nil
}

type BooleanSchema struct{}

func (*BooleanSchema) String() string {
	return `{"type": "boolean"}`
}

func (*BooleanSchema) Type() int {
	return Boolean
}

func (*BooleanSchema) GetName() string {
	return type_boolean
}

func (*BooleanSchema) Prop(key string) (string, bool) {
	return "", false
}

func (this *BooleanSchema) MarshalJSON() ([]byte, error) {
	return []byte(`"boolean"`), nil
}

type NullSchema struct{}

func (*NullSchema) String() string {
	return `{"type": "null"}`
}

func (*NullSchema) Type() int {
	return Null
}

func (*NullSchema) GetName() string {
	return type_null
}

func (*NullSchema) Prop(key string) (string, bool) {
	return "", false
}

func (this *NullSchema) MarshalJSON() ([]byte, error) {
	return []byte(`"null"`), nil
}

//COMPLEX
type RecordSchema struct {
	Name       string   `json:"name,omitempty"`
	Namespace  string   `json:"namespace,omitempty"`
	Doc        string   `json:"doc,omitempty"`
	Aliases    []string `json:"aliases,omitempty"`
	Properties map[string]string
	Fields     []*SchemaField `json:"fields,omitempty"`
}

func (this *RecordSchema) String() string {
	bytes, err := json.MarshalIndent(this, "", "    ")
	if err != nil {
		panic(err)
	}

	return string(bytes)
}

func (this *RecordSchema) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type      string         `json:"type,omitempty"`
		Namespace string         `json:"namespace,omitempty"`
		Name      string         `json:"name,omitempty"`
		Doc       string         `json:"doc,omitempty"`
		Aliases   []string       `json:"aliases,omitempty"`
		Fields    []*SchemaField `json:"fields,omitempty"`
	}{
		Type:      "record",
		Namespace: this.Namespace,
		Name:      this.Name,
		Doc:       this.Doc,
		Aliases:   this.Aliases,
		Fields:    this.Fields,
	})
}

func (*RecordSchema) Type() int {
	return Record
}

func (this *RecordSchema) GetName() string {
	return this.Name
}

func (this *RecordSchema) Prop(key string) (string, bool) {
	if this.Properties != nil {
		if prop, ok := this.Properties[key]; ok {
			return prop, true
		}
	}

	return "", false
}

type RecursiveSchema struct {
	Actual *RecordSchema
}

func newRecursiveSchema(parent *RecordSchema) *RecursiveSchema {
	return &RecursiveSchema{
		Actual: parent,
	}
}

func (this *RecursiveSchema) String() string {
	return fmt.Sprintf(`{"type": "%s"}`, this.Actual.GetName())
}

func (*RecursiveSchema) Type() int {
	return Recursive
}

func (this *RecursiveSchema) GetName() string {
	return this.Actual.GetName()
}

func (*RecursiveSchema) Prop(key string) (string, bool) {
	return "", false
}

func (this *RecursiveSchema) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf(`"%s"`, this.Actual.GetName())), nil
}

type SchemaField struct {
	Name    string      `json:"name,omitempty"`
	Doc     string      `json:"doc,omitempty"`
	Default interface{} `json:"default,omitempty"`
	Type    Schema      `json:"type,omitempty"`
}

func (this *SchemaField) String() string {
	return fmt.Sprintf("[SchemaField: Name: %s, Doc: %s, Default: %v, Type: %s]", this.Name, this.Doc, this.Default, this.Type)
}

type EnumSchema struct {
	Name       string
	Namespace  string
	Aliases    []string
	Doc        string
	Symbols    []string
	Properties map[string]string
}

func (this *EnumSchema) String() string {
	bytes, err := json.MarshalIndent(this, "", "    ")
	if err != nil {
		panic(err)
	}

	return string(bytes)
}

func (*EnumSchema) Type() int {
	return Enum
}

func (this *EnumSchema) GetName() string {
	return this.Name
}

func (this *EnumSchema) Prop(key string) (string, bool) {
	if this.Properties != nil {
		if prop, ok := this.Properties[key]; ok {
			return prop, true
		}
	}

	return "", false
}

func (this *EnumSchema) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type      string   `json:"type,omitempty"`
		Namespace string   `json:"namespace,omitempty"`
		Name      string   `json:"name,omitempty"`
		Doc       string   `json:"doc,omitempty"`
		Symbols   []string `json:"symbols,omitempty"`
	}{
		Type:      "enum",
		Namespace: this.Namespace,
		Name:      this.Name,
		Doc:       this.Doc,
		Symbols:   this.Symbols,
	})
}

type ArraySchema struct {
	Items      Schema
	Properties map[string]string
}

func (this *ArraySchema) String() string {
	bytes, err := json.MarshalIndent(this, "", "    ")
	if err != nil {
		panic(err)
	}

	return string(bytes)
}

func (*ArraySchema) Type() int {
	return Array
}

func (*ArraySchema) GetName() string {
	return type_array
}

func (this *ArraySchema) Prop(key string) (string, bool) {
	if this.Properties != nil {
		if prop, ok := this.Properties[key]; ok {
			return prop, true
		}
	}

	return "", false
}

func (this *ArraySchema) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type  string `json:"type,omitempty"`
		Items Schema `json:"items,omitempty"`
	}{
		Type:  "array",
		Items: this.Items,
	})
}

type MapSchema struct {
	Values     Schema
	Properties map[string]string
}

func (this *MapSchema) String() string {
	bytes, err := json.MarshalIndent(this, "", "    ")
	if err != nil {
		panic(err)
	}

	return string(bytes)
}

func (*MapSchema) Type() int {
	return Map
}

func (*MapSchema) GetName() string {
	return type_map
}

func (this *MapSchema) Prop(key string) (string, bool) {
	if this.Properties != nil {
		if prop, ok := this.Properties[key]; ok {
			return prop, true
		}
	}
	return "", false
}

func (this *MapSchema) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type   string `json:"type,omitempty"`
		Values Schema `json:"values,omitempty"`
	}{
		Type:   "map",
		Values: this.Values,
	})
}

type UnionSchema struct {
	Types []Schema
}

func (this *UnionSchema) String() string {
	bytes, err := json.MarshalIndent(this, "", "    ")
	if err != nil {
		panic(err)
	}

	return fmt.Sprintf(`{"type": %s}`, string(bytes))
}

func (*UnionSchema) Type() int {
	return Union
}

func (*UnionSchema) GetName() string {
	return type_union
}

func (*UnionSchema) Prop(key string) (string, bool) {
	return "", false
}

func (this *UnionSchema) MarshalJSON() ([]byte, error) {
	return json.Marshal(this.Types)
}

type FixedSchema struct {
	Name       string
	Size       int
	Properties map[string]string
}

func (this *FixedSchema) String() string {
	bytes, err := json.MarshalIndent(this, "", "    ")
	if err != nil {
		panic(err)
	}

	return string(bytes)
}

func (*FixedSchema) Type() int {
	return Fixed
}

func (this *FixedSchema) GetName() string {
	return this.Name
}

func (this *FixedSchema) Prop(key string) (string, bool) {
	if this.Properties != nil {
		if prop, ok := this.Properties[key]; ok {
			return prop, true
		}
	}
	return "", false
}

func (this *FixedSchema) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type string `json:"type,omitempty"`
		Size int    `json:"size,omitempty"`
		Name string `json:"name,omitempty"`
	}{
		Type: "fixed",
		Size: this.Size,
		Name: this.Name,
	})
}

// Parses a given schema without provided schemas to reuse.
// Equivalent to call ParseSchemaWithResistry(rawSchema, make(map[string]Schema))
// May return an error if schema is not parsable or has insufficient information about any type.
func ParseSchema(rawSchema string) (Schema, error) {
	return ParseSchemaWithRegistry(rawSchema, make(map[string]Schema))
}

// Parses a given schema using the provided registry for type lookup.
// Registry will be filled up during parsing.
// May return an error if schema is not parsable or has insufficient information about any type.
func ParseSchemaWithRegistry(rawSchema string, schemas map[string]Schema) (Schema, error) {
	var schema interface{}
	if err := json.Unmarshal([]byte(rawSchema), &schema); err != nil {
		schema = rawSchema
	}

	return schemaByType(schema, schemas)
}

func schemaByType(i interface{}, registry map[string]Schema) (Schema, error) {
	switch v := i.(type) {
	case nil:
		return new(NullSchema), nil
	case string:
		switch v {
		case type_null:
			return new(NullSchema), nil
		case type_boolean:
			return new(BooleanSchema), nil
		case type_int:
			return new(IntSchema), nil
		case type_long:
			return new(LongSchema), nil
		case type_float:
			return new(FloatSchema), nil
		case type_double:
			return new(DoubleSchema), nil
		case type_bytes:
			return new(BytesSchema), nil
		case type_string:
			return new(StringSchema), nil
		default:
			schema, ok := registry[v]
			if !ok {
				return nil, fmt.Errorf("Unknown type name: %s", v)
			}

			return schema, nil
		}
	case map[string][]interface{}:
		return parseUnionSchema(v[schema_typeField], registry)
	case map[string]interface{}:
		switch v[schema_typeField] {
		case type_null:
			return new(NullSchema), nil
		case type_boolean:
			return new(BooleanSchema), nil
		case type_int:
			return new(IntSchema), nil
		case type_long:
			return new(LongSchema), nil
		case type_float:
			return new(FloatSchema), nil
		case type_double:
			return new(DoubleSchema), nil
		case type_bytes:
			return new(BytesSchema), nil
		case type_string:
			return new(StringSchema), nil
		case type_array:
			items, err := schemaByType(v[schema_itemsField], registry)
			if err != nil {
				return nil, err
			}
			return &ArraySchema{Items: items, Properties: getProperties(v)}, nil
		case type_map:
			values, err := schemaByType(v[schema_valuesField], registry)
			if err != nil {
				return nil, err
			}
			return &MapSchema{Values: values, Properties: getProperties(v)}, nil
		case type_enum:
			return parseEnumSchema(v, registry)
		case type_fixed:
			return parseFixedSchema(v, registry)
		case type_record:
			return parseRecordSchema(v, registry)
		}
	case []interface{}:
		return parseUnionSchema(v, registry)
	}

	return nil, InvalidSchema
}

func parseEnumSchema(v map[string]interface{}, registry map[string]Schema) (Schema, error) {
	symbols := make([]string, len(v[schema_symbolsField].([]interface{})))
	for i, symbol := range v[schema_symbolsField].([]interface{}) {
		symbols[i] = symbol.(string)
	}

	schema := &EnumSchema{Name: v[schema_nameField].(string), Symbols: symbols}
	setOptionalField(&schema.Namespace, v, schema_namespaceField)
	setOptionalField(&schema.Doc, v, schema_docField)
	schema.Properties = getProperties(v)

	return addSchema(getFullName(v), schema, registry)
}

func parseFixedSchema(v map[string]interface{}, registry map[string]Schema) (Schema, error) {
	if size, ok := v[schema_sizeField].(float64); !ok {
		return nil, InvalidFixedSize
	} else {
		return addSchema(getFullName(v), &FixedSchema{Name: v[schema_nameField].(string), Size: int(size), Properties: getProperties(v)}, registry)
	}
}

func parseUnionSchema(v []interface{}, registry map[string]Schema) (Schema, error) {
	types := make([]Schema, len(v))
	var err error
	for i := range v {
		types[i], err = schemaByType(v[i], registry)
		if err != nil {
			return nil, err
		}
	}
	return &UnionSchema{Types: types}, nil
}

func parseRecordSchema(v map[string]interface{}, registry map[string]Schema) (Schema, error) {
	schema := &RecordSchema{Name: v[schema_nameField].(string)}
	registry[schema.Name] = newRecursiveSchema(schema)
	fields := make([]*SchemaField, len(v[schema_fieldsField].([]interface{})))
	for i := range fields {
		field, err := parseSchemaField(v[schema_fieldsField].([]interface{})[i], registry)
		if err != nil {
			return nil, err
		}
		fields[i] = field
	}
	schema.Fields = fields
	setOptionalField(&schema.Namespace, v, schema_namespaceField)
	setOptionalField(&schema.Doc, v, schema_docField)
	schema.Properties = getProperties(v)

	return schema, nil
}

func parseSchemaField(i interface{}, registry map[string]Schema) (*SchemaField, error) {
	switch v := i.(type) {
	case map[string]interface{}:
		schemaField := &SchemaField{Name: v[schema_nameField].(string)}
		setOptionalField(&schemaField.Doc, v, schema_docField)
		fieldType, err := schemaByType(v[schema_typeField], registry)
		if err != nil {
			return nil, err
		}
		schemaField.Type = fieldType
		if def, exists := v[schema_defaultField]; exists {
			schemaField.Default = def
		}
		return schemaField, nil
	}

	return nil, InvalidSchema
}

func setOptionalField(where *string, v map[string]interface{}, fieldName string) {
	if field, exists := v[fieldName]; exists {
		*where = field.(string)
	}
}

func addSchema(name string, schema Schema, schemas map[string]Schema) (Schema, error) {
	if schemas != nil {
		if sch, ok := schemas[name]; ok {
			return sch, nil
		} else {
			schemas[name] = schema
		}
	}

	return schema, nil
}

func getFullName(v map[string]interface{}) string {
	ns, ok := v[schema_namespaceField].(string)

	if len(ns) > 0 && ok {
		return ns + "." + v[schema_nameField].(string)
	} else {
		return v[schema_nameField].(string)
	}
}

// gets custom string properties from a given schema
func getProperties(v map[string]interface{}) map[string]string {
	props := make(map[string]string)

	for name, value := range v {
		if !isReserved(name) {
			if val, ok := value.(string); ok {
				props[name] = val
			}
		}
	}

	return props
}

func isReserved(name string) bool {
	switch name {
	case schema_aliasesField:
	case schema_docField:
	case schema_fieldsField:
	case schema_itemsField:
	case schema_nameField:
	case schema_namespaceField:
	case schema_sizeField:
	case schema_symbolsField:
	case schema_typeField:
	case schema_valuesField:
		return true
	}

	return false
}
