// Code generated by the FlatBuffers compiler. DO NOT EDIT.

package models

import (
	flatbuffers "github.com/google/flatbuffers/go"
)

type Rows struct {
	_tab flatbuffers.Table
}

func GetRootAsRows(buf []byte, offset flatbuffers.UOffsetT) *Rows {
	n := flatbuffers.GetUOffsetT(buf[offset:])
	x := &Rows{}
	x.Init(buf, n+offset)
	return x
}

func GetSizePrefixedRootAsRows(buf []byte, offset flatbuffers.UOffsetT) *Rows {
	n := flatbuffers.GetUOffsetT(buf[offset+flatbuffers.SizeUint32:])
	x := &Rows{}
	x.Init(buf, n+offset+flatbuffers.SizeUint32)
	return x
}

func (rcv *Rows) Init(buf []byte, i flatbuffers.UOffsetT) {
	rcv._tab.Bytes = buf
	rcv._tab.Pos = i
}

func (rcv *Rows) Table() flatbuffers.Table {
	return rcv._tab
}

func (rcv *Rows) Rows(obj *Row, j int) bool {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(4))
	if o != 0 {
		x := rcv._tab.Vector(o)
		x += flatbuffers.UOffsetT(j) * 4
		x = rcv._tab.Indirect(x)
		obj.Init(rcv._tab.Bytes, x)
		return true
	}
	return false
}

func (rcv *Rows) RowsLength() int {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(4))
	if o != 0 {
		return rcv._tab.VectorLen(o)
	}
	return 0
}

func RowsStart(builder *flatbuffers.Builder) {
	builder.StartObject(1)
}
func RowsAddRows(builder *flatbuffers.Builder, rows flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(0, flatbuffers.UOffsetT(rows), 0)
}
func RowsStartRowsVector(builder *flatbuffers.Builder, numElems int) flatbuffers.UOffsetT {
	return builder.StartVector(4, numElems, 4)
}
func RowsEnd(builder *flatbuffers.Builder) flatbuffers.UOffsetT {
	return builder.EndObject()
}
