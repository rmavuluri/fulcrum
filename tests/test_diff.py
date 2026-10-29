import json

from fulcrum_mcp.diff import diff_schemas

AVRO_V1 = {
    "type": "record",
    "name": "Order",
    "fields": [
        {"name": "id", "type": "string"},
        {"name": "amount", "type": "int"},
        {"name": "addr", "type": {"type": "record", "name": "A", "fields": [{"name": "zip", "type": "string"}]}},
    ],
}
AVRO_V2 = {
    "type": "record",
    "name": "Order",
    "fields": [
        {"name": "id", "type": "string"},
        {"name": "amount", "type": "long"},
        {"name": "currency", "type": "string", "default": "USD"},
        {
            "name": "addr",
            "type": ["null", {"type": "record", "name": "A", "fields": [{"name": "zip", "type": "int"}]}],
        },
    ],
}


def test_identical_ignores_formatting():
    a = json.dumps(AVRO_V1)
    b = json.dumps(AVRO_V1, indent=4)
    result = diff_schemas(a, b, "AVRO", "v1", "v2")
    assert not result.changed and result.unified == ""


def test_avro_field_changes():
    result = diff_schemas(json.dumps(AVRO_V1), json.dumps(AVRO_V2), "AVRO", "v1", "v2")
    changes = {(kind, name) for kind, name, _ in result.structural}
    assert ("added", "currency") in changes
    assert ("changed", "amount") in changes
    assert ("changed", "addr.zip") in changes
    assert "--- v1" in result.unified and "+++ v2" in result.unified


def test_json_schema_changes():
    v1 = {"type": "object", "properties": {"id": {"type": "string"}, "old": {"type": "number"}}, "required": ["id"]}
    v2 = {
        "type": "object",
        "properties": {"id": {"type": "string"}, "meta": {"type": "object", "properties": {"k": {"type": "string"}}}},
        "required": ["id", "meta"],
    }
    result = diff_schemas(json.dumps(v1), json.dumps(v2), "JSON", "v1", "v2")
    changes = {(kind, name) for kind, name, _ in result.structural}
    assert changes == {("added", "meta"), ("added", "meta.k"), ("removed", "old")}


def test_protobuf_changes():
    v1 = 'syntax = "proto3";\nmessage O {\n  string id = 1;\n  int32 qty = 2;\n}\n'
    v2 = 'syntax = "proto3";\nmessage O {\n  string id = 1;\n  int64 qty = 2;\n  repeated string tags = 3;\n}\n'
    result = diff_schemas(v1, v2, "PROTOBUF", "v1", "v2")
    changes = {(kind, name) for kind, name, _ in result.structural}
    assert changes == {("changed", "qty"), ("added", "tags")}


def test_unparseable_falls_back_to_text_diff():
    result = diff_schemas("{not json", "{still not", "AVRO", "v1", "v2")
    assert result.changed and result.structural == [] and result.unified
