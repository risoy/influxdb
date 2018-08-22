package storage_test

import (
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/gogo/protobuf/types"
	"github.com/influxdata/influxdb/services/storage"
)

func TestFoo(t *testing.T) {
	var rr storage.ReadRequest

	var src storage.ReadSource
	src.Database = "foo"

	var err error
	rr.ReadSource, err = types.MarshalAny(&src)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	buf, _ := proto.Marshal(&rr)

	var r2 storage.ReadRequest
	err = r2.Unmarshal(buf)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	var dyn types.DynamicAny
	types.UnmarshalAny(r2.ReadSource, &dyn)

	if rs, ok := dyn.Message.(*storage.ReadSource); ok {
		t.Log(rs.Database)
	}
}
