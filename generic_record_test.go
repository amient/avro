package avro

import (
	"testing"
)

func TestGenericDatumFromMap(t *testing.T) {
	schema := MustParseSchema(`{
	    "type": "record",
	    "name": "Rec",
	    "fields": [
	        {
	            "name": "dict",
	            "type": {
					"type": "map", 
					"values": { "type": "array", "items": "string" }
				}
	        }
	    ]
	}`)

	generic := NewGenericRecord(schema)

	type X string
	generic.SetAll(map[string]interface{} {
		"dict": map[interface{}]interface{} {
			"A1": []interface{} { "abc", "def" },
			"G1": []interface{} { "ghi", "jkl" },
		},
	})

	assert(t, generic.String(), `{"dict":{"A1":["abc","def"],"G1":["ghi","jkl"]}}`)

}

