package main

// This file lifted wholesale from mountacnosdb by Mark Rushakoff.

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"
	"time"
	"unsafe"

	"github.com/cnosdb/tsdb-comparisons/cmd/load_cnosdb/models"
	proto "github.com/cnosdb/tsdb-comparisons/cmd/load_cnosdb/proto"
	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/valyala/fasthttp"
	"google.golang.org/grpc"
)

const (
	httpClientName        = "load_cnosdb"
	headerContentEncoding = "Content-Encoding"
	headerGzip            = "gzip"
)

var (
	errBackoff          = fmt.Errorf("backpressure is needed")
	backoffMagicWords0  = []byte("engine: cache maximum memory size exceeded")
	backoffMagicWords1  = []byte("write failed: hinted handoff queue not empty")
	backoffMagicWords2a = []byte("write failed: read message type: read tcp")
	backoffMagicWords2b = []byte("i/o timeout")
	backoffMagicWords3  = []byte("write failed: engine: cache-max-memory-size exceeded")
	backoffMagicWords4  = []byte("timeout")
	backoffMagicWords5  = []byte("write failed: can not exceed max connections of 500")
)

// HTTPWriterConfig is the configuration used to create an HTTPWriter.
type HTTPWriterConfig struct {
	// URL of the host, in form "http://example.com:8086"
	Host string

	// Name of the target database into which points will be written.
	Database string

	Auth string

	// Debug label for more informative errors.
	DebugInfo string
}

// HTTPWriter is a Writer that writes to an CnosDB HTTP server.
type HTTPWriter struct {
	client     fasthttp.Client
	grpcClient proto.TSKVService_WritePointsClient

	c   HTTPWriterConfig
	url []byte
}

// NewHTTPWriter returns a new HTTPWriter from the supplied HTTPWriterConfig.
func NewHTTPWriter(c HTTPWriterConfig, consistency string) *HTTPWriter {
	conn, err := grpc.Dial(c.Host, grpc.WithInsecure(), grpc.WithBlock(), grpc.WithTimeout(10*time.Second))
	if err != nil {
		panic(err)
	}
	grpcClient := proto.NewTSKVServiceClient(conn)
	writePointsCli, err := grpcClient.WritePoints(context.Background())
	if err != nil {
		panic(err)
	}

	return &HTTPWriter{
		client: fasthttp.Client{
			Name: httpClientName,
		},
		grpcClient: writePointsCli,
		c:          c,
		url:        []byte(c.Host + "/api/v1/write?db=" + c.Database),
	}
}

var (
	methodPost = []byte("POST")
	textPlain  = []byte("text/plain")
)

func (w *HTTPWriter) initializeReq(req *fasthttp.Request, body []byte, isGzip bool) {
	req.Header.SetContentTypeBytes(textPlain)
	req.Header.SetMethodBytes(methodPost)
	req.Header.SetRequestURIBytes(w.url)
	req.Header.Set("AUTHORIZATION", w.c.Auth)
	if isGzip {
		req.Header.Add(headerContentEncoding, headerGzip)
	}
	req.SetBody(body)
}

func (w *HTTPWriter) executeReq(req *fasthttp.Request, resp *fasthttp.Response) (int64, error) {
	start := time.Now()
	err := w.client.Do(req, resp)
	lat := time.Since(start).Nanoseconds()
	if err == nil {
		sc := resp.StatusCode()
		if sc == 500 && backpressurePred(resp.Body()) {
			err = errBackoff
		} else if sc != fasthttp.StatusOK {
			err = fmt.Errorf("[DebugInfo: %s] Invalid write response (status %d): %s", w.c.DebugInfo, sc, resp.Body())
		}
	}
	return lat, err
}

func printFlatbuffersBody(body []byte) {
	fmt.Printf("\nBody Length: %v\n", len(body))
	points := models.GetRootAsPoints(body, 0)
	point := &models.Point{}
	tag := &models.Tag{}
	field := &models.Field{}
	for i := 0; i < points.PointsLength(); i++ {
		points.Points(point, i)
		fmt.Printf("Tags[%d]: ", point.TagsLength())
		for j := 0; j < point.TagsLength(); j++ {
			point.Tags(tag, j)
			if tag.KeyLength() == 0 {
				println("Key is empty")
			}
			tagKey := string(tag.KeyBytes())
			fmt.Printf("{ %s: ", tagKey)
			if tag.KeyLength() == 0 {
				println("Value is empty")
			}
			tagValue := string(tag.ValueBytes())
			fmt.Printf("%s }, ", tagValue)
		}
		fmt.Printf("\nFields[%d]: ", point.FieldsLength())
		for j := 0; j < point.FieldsLength(); j++ {
			point.Fields(field, j)
			fieldName := string(field.NameBytes())
			fmt.Printf("{ %s: ", fieldName)
			fieldType := field.Type()
			switch fieldType {
			case models.FieldTypeInteger:
				fieldValue := binary.BigEndian.Uint64(field.ValueBytes())
				fmt.Printf("%d, ", int64(fieldValue))
			case models.FieldTypeUnsigned:
				fieldValue := binary.BigEndian.Uint64(field.ValueBytes())
				fmt.Printf("%d, ", fieldValue)
			case models.FieldTypeFloat:
				fieldValue := binary.BigEndian.Uint64(field.ValueBytes())
				fmt.Printf("%f, ", float64(fieldValue))
			case models.FieldTypeBoolean:
				fieldValue := field.ValueBytes()
				if fieldValue[0] == 1 {
					fmt.Printf("true, ")
				} else {
					fmt.Printf("false, ")
				}
			case models.FieldTypeString:
				fieldValue := string(field.ValueBytes())
				fmt.Printf("%s, ", fieldValue)
			default:

			}
			fmt.Printf("%d }, ", field.Type())
		}
		fmt.Println()
	}
}

// WriteLineProtocol writes the given byte slice to the HTTP server described in the Writer's HTTPWriterConfig.
// It returns the latency in nanoseconds and any error received while sending the data over HTTP,
// or it returns a new error if the HTTP response isn't as expected.
func (w *HTTPWriter) WriteLineProtocol(body []byte, isGzip bool) (int64, error) {
	//req := fasthttp.AcquireRequest()
	//defer fasthttp.ReleaseRequest(req)
	//w.initializeReq(req, body, isGzip)
	//
	//resp := fasthttp.AcquireResponse()
	//defer fasthttp.ReleaseResponse(resp)
	//
	//return w.executeReq(req, resp)

	err := w.grpcClient.Send(&proto.WritePointsRpcRequest{
		Points: body,
	})
	if err != nil {
		panic(err)
	}

	//_, err = w.grpcClient.Recv()
	//if err != nil {
	//	panic(err)
	//}
	//w.grpcClient.CloseSend()
	return 200, nil
}

func parserLine(lines []byte) []byte {
	//pointFb := flatbuffers.NewBuilder(0)
	fb := flatbuffers.NewBuilder(0)
	database := fb.CreateByteString([]byte("public"))
	numLines := bytes.Count(lines, []byte{'\n'})

	var pointOffs []flatbuffers.UOffsetT
	var (
		pos   int
		block []byte
	)
	for pos < len(lines) {
		pos, block = scanLine(lines, pos)
		if len(block) == 0 {
			continue
		}
		start := skipWhitespace(block, 0)
		if start >= len(block) {
			continue
		}
		if block[start] == '#' {
			continue
		}
		if block[len(block)-1] == '\n' {
			block = block[:len(block)-1]
		}
		pointOffs = append(pointOffs, parsePoint(fb, block[start:]))
	}
	models.PointsStartPointsVector(fb, numLines)
	for _, i := range pointOffs {
		fb.PrependUOffsetT(i)
	}
	ptVec := fb.EndVector(numLines)
	models.PointsStart(fb)
	models.PointsAddPoints(fb, ptVec)
	models.PointsAddDb(fb, database)
	endPoints := models.PointsEnd(fb)
	fb.Finish(endPoints)
	return fb.FinishedBytes()
}

func backpressurePred(body []byte) bool {
	if bytes.Contains(body, backoffMagicWords0) {
		return true
	} else if bytes.Contains(body, backoffMagicWords1) {
		return true
	} else if bytes.Contains(body, backoffMagicWords2a) && bytes.Contains(body, backoffMagicWords2b) {
		return true
	} else if bytes.Contains(body, backoffMagicWords3) {
		return true
	} else if bytes.Contains(body, backoffMagicWords4) {
		return true
	} else if bytes.Contains(body, backoffMagicWords5) {
		return true
	} else {
		return false
	}
}

func scanLine(buf []byte, i int) (int, []byte) {
	start := i
	quoted := false
	fields := false

	// tracks how many '=' and commas we've seen
	// this duplicates some of the functionality in scanFields
	equals := 0
	commas := 0
	for {
		// reached the end of buf?
		if i >= len(buf) {
			break
		}

		// skip past escaped characters
		if buf[i] == '\\' && i+2 < len(buf) {
			i += 2
			continue
		}

		if buf[i] == ' ' {
			fields = true
		}

		// If we see a double quote, makes sure it is not escaped
		if fields {
			if !quoted && buf[i] == '=' {
				i++
				equals++
				continue
			} else if !quoted && buf[i] == ',' {
				i++
				commas++
				continue
			} else if buf[i] == '"' && equals > commas {
				i++
				quoted = !quoted
				continue
			}
		}

		if buf[i] == '\n' && !quoted {
			break
		}

		i++

	}

	return i + 1, buf[start:i]
}

func skipWhitespace(buf []byte, i int) int {
	for i < len(buf) {
		if buf[i] != ' ' && buf[i] != '\t' && buf[i] != 0 {
			break
		}
		i++
	}
	return i
}

func parsePoint(fb *flatbuffers.Builder, buf []byte) flatbuffers.UOffsetT {
	dbOff := fb.CreateByteVector([]byte("tsdb-comparisons"))
	pos, key, err, mPos := scanKey(buf, 0)

	if err != nil {
		panic(err)
	}
	if len(key) == 0 {
		panic(fmt.Errorf("missing measurement"))
	}

	if len(key) > 65535 {
		panic(fmt.Errorf("max key length exceeded: %v > %v", len(key), 65535))
	}
	tableOff := fb.CreateByteVector(buf[0 : mPos-1])

	tkValOffs, tvValOffs := parseTags(fb, key[mPos:])
	tagOffs := make([]flatbuffers.UOffsetT, len(tkValOffs))
	for i := 0; i < len(tkValOffs); i++ {
		models.TagStart(fb)
		models.TagAddKey(fb, tkValOffs[i])
		models.TagAddValue(fb, tvValOffs[i])
		tagOffs[i] = models.TagEnd(fb)
	}
	models.PointStartTagsVector(fb, len(tagOffs))
	for _, off := range tagOffs {
		fb.PrependUOffsetT(off)
	}
	tagsOff := fb.EndVector(len(tagOffs))

	pos, fields, fieldTypes, err := scanFields(buf, pos)
	if err != nil {
		panic(err)
	}
	if len(fields) == 0 {
		panic(fmt.Errorf("missing fields"))
	}
	i := 0

	var fkOffs []flatbuffers.UOffsetT
	var fvOffs []flatbuffers.UOffsetT
	var fTyps []models.FieldType
	err = walkFields(fields, func(k, v []byte) bool {
		//fmt.Print("FieldKey: " + string(k) + ", FieldValue: " + string(v))
		fkOffs = append(fkOffs, fb.CreateByteVector(k))

		isNegative := false
		fieldValStr := string(v)
		if fieldValStr[0] == '-' {
			isNegative = true
			fieldValStr = fieldValStr[1:]
		} else if fieldValStr[0] == '+' {
			fieldValStr = fieldValStr[1:]
		}

		fType := fieldTypes[0]
		fieldTypes = fieldTypes[1:]
		numBuf := make([]byte, 8)
		switch fType {
		case models.FieldTypeInteger:
			//fmt.Println(", FieldType: Integer")
			fv, _ := strconv.ParseInt(fieldValStr[:len(fieldValStr)-1], 10, 64)
			if isNegative {
				fv = -fv
			}
			binary.BigEndian.PutUint64(numBuf, uint64(fv))
			fvOffs = append(fvOffs, fb.CreateByteVector(numBuf))
			fTyps = append(fTyps, models.FieldTypeInteger)
		case models.FieldTypeUnsigned:
			//fmt.Println(", FieldType: Unsigned")
			fv, _ := strconv.ParseUint(fieldValStr[:len(fieldValStr)-1], 10, 64)
			if isNegative {
				fv = -fv
			}
			binary.BigEndian.PutUint64(numBuf, fv)
			fvOffs = append(fvOffs, fb.CreateByteVector(numBuf))
			fTyps = append(fTyps, models.FieldTypeUnsigned)
		case models.FieldTypeFloat:
			//fmt.Println(", FieldType: Float")
			fv, _ := strconv.ParseFloat(fieldValStr, 10)
			if isNegative {
				fv = -fv
			}
			binary.BigEndian.PutUint64(numBuf, uint64(fv))
			fvOffs = append(fvOffs, fb.CreateByteVector(numBuf))
			fTyps = append(fTyps, models.FieldTypeFloat)
		case models.FieldTypeBoolean:
			//fmt.Println(", FieldType: Boolean")
			if fieldValStr[0] == 't' || fieldValStr[0] == 'T' {
				fvOffs = append(fvOffs, fb.CreateByteVector([]byte{1}))
				fTyps = append(fTyps, models.FieldTypeBoolean)
			} else if fieldValStr[0] == 'f' || fieldValStr[0] == 'F' {
				fvOffs = append(fvOffs, fb.CreateByteVector([]byte{0}))
				fTyps = append(fTyps, models.FieldTypeBoolean)
			}
		case models.FieldTypeString:
			//fmt.Println(", FieldType: String")
			if fieldValStr[0] == '"' {
				fvOffs = append(fvOffs, fb.CreateByteVector([]byte(fieldValStr[1:len(fieldValStr)-1])))
				fTyps = append(fTyps, models.FieldTypeString)
			} else {
				fvOffs = append(fvOffs, fb.CreateByteVector(v))
				fTyps = append(fTyps, models.FieldTypeString)
			}
		default:
			//fmt.Println(", FieldType: Unknown")
			fvOffs = append(fvOffs, fb.CreateByteVector(v))
			fTyps = append(fTyps, models.FieldTypeString)
		}

		i++
		return true
	})
	fieldOffs := make([]flatbuffers.UOffsetT, len(fkOffs))
	for i := 0; i < len(fkOffs); i++ {
		models.FieldStart(fb)
		models.FieldAddName(fb, fkOffs[i])
		models.FieldAddValue(fb, fvOffs[i])
		models.FieldAddType(fb, fTyps[i])
		fieldOffs[i] = models.FieldEnd(fb)
	}
	models.PointStartFieldsVector(fb, len(fieldOffs))
	for _, off := range fieldOffs {
		fb.PrependUOffsetT(off)
	}
	fieldsOff := fb.EndVector(len(fieldOffs))

	pos, ts, err := scanTime(buf, pos)
	tsInt, err := strconv.Atoi(string(ts))
	if err != nil {
		panic(err)
	}

	models.PointStart(fb)
	models.PointAddDb(fb, dbOff)
	models.PointAddTab(fb, tableOff)
	models.PointAddTags(fb, tagsOff)
	models.PointAddFields(fb, fieldsOff)
	models.PointAddTimestamp(fb, int64(tsInt))
	return models.PointEnd(fb)
}

func scanKey(buf []byte, i int) (int, []byte, error, int) {
	start := skipWhitespace(buf, i)

	i = start

	// Determines whether the tags are sort, assume they are
	sorted := true

	// indices holds the indexes within buf of the start of each tag.  For example,
	// a buf of 'cpu,host=a,region=b,zone=c' would have indices slice of [4,11,20]
	// which indicates that the first tag starts at buf[4], seconds at buf[11], and
	// last at buf[20]
	indices := make([]int, 100)

	// tracks how many commas we've seen so we know how many values are indices.
	// Since indices is an arbitrarily large slice,
	// we need to know how many values in the buffer are in use.
	commas := 0

	// First scan the Point's measurement.
	state, i, err := scanMeasurement(buf, i)
	meaPos := i
	if err != nil {
		return i, buf[start:i], err, meaPos
	}

	// Optionally scan tags if needed.
	if state == tagKeyState {
		i, commas, indices, err = scanTags(buf, i, indices)
		if err != nil {
			return i, buf[start:i], err, meaPos
		}
	}

	// Iterate over tags keys ensure that we do not encounter any
	// of the reserved tag keys such as _measurement or _field.
	for j := 0; j < commas; j++ {
		_, key := scanTo(buf[indices[j]:indices[j+1]-1], 0, '=')

		for _, reserved := range reservedTagKeys {
			if bytes.Equal(key, reserved) {
				return i, buf[start:i], fmt.Errorf("cannot use reserved tag key %q", key), meaPos
			}
		}
	}

	// Now we know where the key region is within buf, and the location of tags, we
	// need to determine if duplicate tags exist and if the tags are sorted. This iterates
	// over the list comparing each tag in the sequence with each other.
	for j := 0; j < commas-1; j++ {
		// get the left and right tags
		_, left := scanTo(buf[indices[j]:indices[j+1]-1], 0, '=')
		_, right := scanTo(buf[indices[j+1]:indices[j+2]-1], 0, '=')

		// If left is greater than right, the tags are not sorted. We do not have to
		// continue because the short path no longer works.
		// If the tags are equal, then there are duplicate tags, and we should abort.
		// If the tags are not sorted, this pass may not find duplicate tags and we
		// need to do a more exhaustive search later.
		if cmp := bytes.Compare(left, right); cmp > 0 {
			sorted = false
			break
		} else if cmp == 0 {
			return i, buf[start:i], fmt.Errorf("duplicate tags"), meaPos
		}
	}

	// If the tags are not sorted, then sort them.  This sort is inline and
	// uses the tag indices we created earlier.  The actual buffer is not sorted, the
	// indices are using the buffer for value comparison.  After the indices are sorted,
	// the buffer is reconstructed from the sorted indices.
	if !sorted && commas > 0 {
		// Get the measurement name for later
		measurement := buf[start : indices[0]-1]

		// Sort the indices
		indices := indices[:commas]
		insertionSort(0, commas, buf, indices)

		// Create a new key using the measurement and sorted indices
		b := make([]byte, len(buf[start:i]))
		pos := copy(b, measurement)
		for _, i := range indices {
			b[pos] = ','
			pos++
			_, v := scanToSpaceOr(buf, i, ',')
			pos += copy(b[pos:], v)
		}

		// Check again for duplicate tags now that the tags are sorted.
		for j := 0; j < commas-1; j++ {
			// get the left and right tags
			_, left := scanTo(buf[indices[j]:], 0, '=')
			_, right := scanTo(buf[indices[j+1]:], 0, '=')

			// If the tags are equal, then there are duplicate tags, and we should abort.
			// If the tags are not sorted, this pass may not find duplicate tags and we
			// need to do a more exhaustive search later.
			if bytes.Equal(left, right) {
				return i, b, fmt.Errorf("duplicate tags"), meaPos
			}
		}

		return i, b, nil, meaPos
	}

	return i, buf[start:i], nil, meaPos
}

func scanMeasurement(buf []byte, i int) (int, int, error) {
	// Check first byte of measurement, anything except a comma is fine.
	// It can't be a space, since whitespace is stripped prior to this
	// function call.
	if i >= len(buf) || buf[i] == ',' {
		return -1, i, fmt.Errorf("missing measurement")
	}

	for {
		i++
		if i >= len(buf) {
			// cpu
			return -1, i, fmt.Errorf("missing fields")
		}

		if buf[i-1] == '\\' {
			// Skip character (it's escaped).
			continue
		}

		// Unescaped comma; move onto scanning the tags.
		if buf[i] == ',' {
			return tagKeyState, i + 1, nil
		}

		// Unescaped space; move onto scanning the fields.
		if buf[i] == ' ' {
			// cpu value=1.0
			return fieldsState, i, nil
		}
	}
}

const (
	tagKeyState = iota
	tagValueState
	fieldsState
)

func scanTags(buf []byte, i int, indices []int) (int, int, []int, error) {
	var (
		err    error
		commas int
		state  = tagKeyState
	)

	for {
		switch state {
		case tagKeyState:
			// Grow our indices slice if we have too many tags.
			if commas >= len(indices) {
				newIndics := make([]int, cap(indices)*2)
				copy(newIndics, indices)
				indices = newIndics
			}
			indices[commas] = i
			commas++

			i, err = scanTagsKey(buf, i)
			state = tagValueState // tag value always follows a tag key
		case tagValueState:
			state, i, err = scanTagsValue(buf, i)
		case fieldsState:
			// Grow our indices slice if we had exactly enough tags to fill it
			if commas >= len(indices) {
				// The parser is in `fieldsState`, so there are no more
				// tags. We only need 1 more entry in the slice to store
				// the final entry.
				newIndics := make([]int, cap(indices)+1)
				copy(newIndics, indices)
				indices = newIndics
			}
			indices[commas] = i + 1
			return i, commas, indices, nil
		}

		if err != nil {
			return i, commas, indices, err
		}
	}
}

func scanTo(buf []byte, i int, stop byte) (int, []byte) {
	start := i
	for {
		// reached the end of buf?
		if i >= len(buf) {
			break
		}

		// Reached unescaped stop value?
		if buf[i] == stop && (i == 0 || buf[i-1] != '\\') {
			break
		}
		i++
	}

	return i, buf[start:i]
}

func insertionSort(l, r int, buf []byte, indices []int) {
	for i := l + 1; i < r; i++ {
		for j := i; j > l && less(buf, indices, j, j-1); j-- {
			indices[j], indices[j-1] = indices[j-1], indices[j]
		}
	}
}

func less(buf []byte, indices []int, i, j int) bool {
	// This grabs the tag names for i & j, it ignores the values
	_, a := scanTo(buf, indices[i], '=')
	_, b := scanTo(buf, indices[j], '=')
	return bytes.Compare(a, b) < 0
}

func scanToSpaceOr(buf []byte, i int, stop byte) (int, []byte) {
	start := i
	if buf[i] == stop || buf[i] == ' ' {
		return i, buf[start:i]
	}

	for {
		i++
		if buf[i-1] == '\\' {
			continue
		}

		// reached the end of buf?
		if i >= len(buf) {
			return i, buf[start:i]
		}

		// reached end of block?
		if buf[i] == stop || buf[i] == ' ' {
			return i, buf[start:i]
		}
	}
}

func scanTagsValue(buf []byte, i int) (int, int, error) {
	// Tag value cannot be empty.
	if i >= len(buf) || buf[i] == ',' || buf[i] == ' ' {
		// cpu,tag={',', ' '}
		return -1, i, fmt.Errorf("missing tag value")
	}

	// Examine each character in the tag value until we hit an unescaped
	// comma (move onto next tag key), an unescaped space (move onto
	// fields), or we error out.
	for {
		i++
		if i >= len(buf) {
			// cpu,tag=value
			return -1, i, fmt.Errorf("missing fields")
		}

		// An unescaped equals sign is an invalid tag value.
		if buf[i] == '=' && buf[i-1] != '\\' {
			// cpu,tag={'=', 'fo=o'}
			return -1, i, fmt.Errorf("invalid tag format")
		}

		if buf[i] == ',' && buf[i-1] != '\\' {
			// cpu,tag=foo,
			return tagKeyState, i + 1, nil
		}

		// cpu,tag=foo value=1.0
		// cpu, tag=foo\= value=1.0
		if buf[i] == ' ' && buf[i-1] != '\\' {
			return fieldsState, i, nil
		}
	}
}

func scanTagsKey(buf []byte, i int) (int, error) {
	// First character of the key.
	if i >= len(buf) || buf[i] == ' ' || buf[i] == ',' || buf[i] == '=' {
		// cpu,{'', ' ', ',', '='}
		return i, fmt.Errorf("missing tag key")
	}

	// Examine each character in the tag key until we hit an unescaped
	// equals (the tag value), or we hit an error (i.e., unescaped
	// space or comma).
	for {
		i++

		// Either we reached the end of the buffer or we hit an
		// unescaped comma or space.
		if i >= len(buf) ||
			((buf[i] == ' ' || buf[i] == ',') && buf[i-1] != '\\') {
			// cpu,tag{'', ' ', ','}
			return i, fmt.Errorf("missing tag value")
		}

		if buf[i] == '=' && buf[i-1] != '\\' {
			// cpu,tag=
			return i + 1, nil
		}
	}
}

const (
	// Values used to store the field key and measurement name as special internal tags.
	FieldKeyTagKey    = "\xff"
	MeasurementTagKey = "\x00"

	// reserved tag keys which when present cause the point to be discarded
	// and an error returned
	reservedFieldTagKey       = "_field"
	reservedMeasurementTagKey = "_measurement"
	reservedTimeTagKey        = "time"
)

var (
	// Predefined byte representations of special tag keys.
	FieldKeyTagKeyBytes    = []byte(FieldKeyTagKey)
	MeasurementTagKeyBytes = []byte(MeasurementTagKey)

	// set of reserved tag keys which cannot be present when a point is being parsed.
	reservedTagKeys = [][]byte{
		FieldKeyTagKeyBytes,
		MeasurementTagKeyBytes,
		[]byte(reservedFieldTagKey),
		[]byte(reservedMeasurementTagKey),
		[]byte(reservedTimeTagKey),
	}
)

func parseTags(fb *flatbuffers.Builder, buf []byte) ([]flatbuffers.UOffsetT, []flatbuffers.UOffsetT) {
	if len(buf) == 0 {
		panic("unexpected has 0 tags")
	}

	// Series keys can contain escaped commas, therefore the number of commas
	// in a series key only gives an estimation of the upper bound on the number
	// of tags.
	var tgkoff []flatbuffers.UOffsetT
	var tgvoff []flatbuffers.UOffsetT
	var i int
	walkTags(buf, func(key, value []byte) bool {
		tgkoff = append(tgkoff, fb.CreateByteVector(key))
		tgvoff = append(tgvoff, fb.CreateByteVector(value))
		i++
		return true
	})
	return tgkoff, tgvoff
}

func walkTags(buf []byte, fn func(key, value []byte) bool) {
	if len(buf) == 0 {
		return
	}

	pos, name := scanTo(buf, 0, ',')

	// it's an empty key, so there are no tags
	if len(name) == 0 {
		return
	}

	hasEscape := bytes.IndexByte(buf, '\\') != -1
	i := pos + 1
	var key, value []byte
	for {
		if i >= len(buf) {
			break
		}
		i, key = scanTo(buf, i, '=')
		i, value = scanTagValue(buf, i+1)

		if len(value) == 0 {
			continue
		}

		if hasEscape {
			if !fn(unescapeTag(key), unescapeTag(value)) {
				return
			}
		} else {
			if !fn(key, value) {
				return
			}
		}

		i++
	}
}

func scanTagValue(buf []byte, i int) (int, []byte) {
	start := i
	for {
		if i >= len(buf) {
			break
		}

		if buf[i] == ',' && buf[i-1] != '\\' {
			break
		}
		i++
	}
	if i > len(buf) {
		return i, nil
	}
	return i, buf[start:i]
}

func unescapeTag(in []byte) []byte {
	if bytes.IndexByte(in, '\\') == -1 {
		return in
	}

	for i := range tagEscapeCodes {
		c := &tagEscapeCodes[i]
		if bytes.IndexByte(in, c.k[0]) != -1 {
			in = bytes.Replace(in, c.esc[:], c.k[:], -1)
		}
	}
	return in
}

var (
	measurementEscapeCodes = [...]escapeSet{
		{k: [1]byte{','}, esc: [2]byte{'\\', ','}},
		{k: [1]byte{' '}, esc: [2]byte{'\\', ' '}},
	}

	tagEscapeCodes = [...]escapeSet{
		{k: [1]byte{','}, esc: [2]byte{'\\', ','}},
		{k: [1]byte{' '}, esc: [2]byte{'\\', ' '}},
		{k: [1]byte{'='}, esc: [2]byte{'\\', '='}},
	}

	// ErrPointMustHaveAField is returned when operating on a point that does not have any fields.
	ErrPointMustHaveAField = errors.New("point without fields is unsupported")

	// ErrInvalidNumber is returned when a number is expected but not provided.
	ErrInvalidNumber = errors.New("invalid number")

	// ErrInvalidPoint is returned when a point cannot be parsed correctly.
	ErrInvalidPoint = errors.New("point is invalid")

	// ErrInvalidKevValuePairs is returned when the number of key, value pairs
	// is odd, indicating a missing value.
	ErrInvalidKevValuePairs = errors.New("key/value pairs is an odd length")
)

type escapeSet struct {
	k   [1]byte
	esc [2]byte
}

func scanFields(buf []byte, i int) (int, []byte, []models.FieldType, error) {
	start := skipWhitespace(buf, i)
	i = start
	quoted := false

	// tracks how many '=' we've seen
	equals := 0

	// tracks how many commas we've seen
	commas := 0

	var types []models.FieldType
	var typ models.FieldType
	for {
		// reached the end of buf?
		if i >= len(buf) {
			break
		}

		// escaped characters?
		if buf[i] == '\\' && i+1 < len(buf) {
			i += 2
			continue
		}

		// If the value is quoted, scan until we get to the end quote
		// Only quote values in the field value since quotes are not significant
		// in the field key
		if buf[i] == '"' && equals > commas {
			types = append(types, models.FieldTypeBoolean)
			quoted = !quoted
			i++
			continue
		}

		// If we see an =, ensure that there is at least on char before and after it
		if buf[i] == '=' && !quoted {
			equals++

			// check for "... =123" but allow "a\ =123"
			if buf[i-1] == ' ' && buf[i-2] != '\\' {
				return i, buf[start:i], types, fmt.Errorf("missing field key")
			}

			// check for "...a=123,=456" but allow "a=123,a\,=456"
			if buf[i-1] == ',' && buf[i-2] != '\\' {
				return i, buf[start:i], types, fmt.Errorf("missing field key")
			}

			// check for "... value="
			if i+1 >= len(buf) {
				return i, buf[start:i], types, fmt.Errorf("missing field value")
			}

			// check for "... value=,value2=..."
			if buf[i+1] == ',' || buf[i+1] == ' ' {
				return i, buf[start:i], types, fmt.Errorf("missing field value")
			}

			if isNumeric(buf[i+1]) || buf[i+1] == '-' || buf[i+1] == 'N' || buf[i+1] == 'n' {
				var err error
				typ, i, err = scanNumber(buf, i+1)
				if err != nil {
					return i, buf[start:i], types, err
				}
				types = append(types, typ)
				continue
			}
			// If next byte is not a double-quote, the value must be a boolean
			if buf[i+1] != '"' {
				var err error
				i, _, err = scanBoolean(buf, i+1)
				types = append(types, models.FieldTypeBoolean)
				if err != nil {
					return i, buf[start:i], types, err
				}
				continue
			}
		}

		if buf[i] == ',' && !quoted {
			commas++
		}

		// reached end of block?
		if buf[i] == ' ' && !quoted {
			break
		}
		i++
	}

	if quoted {
		return i, buf[start:i], types, fmt.Errorf("unbalanced quotes")
	}

	// check that all field sections had key and values (e.g. prevent "a=1,b"
	if equals == 0 || commas != equals-1 {
		return i, buf[start:i], types, fmt.Errorf("invalid field format")
	}

	return i, buf[start:i], types, nil
}

func isNumeric(b byte) bool {
	return (b >= '0' && b <= '9') || b == '.'
}

func scanNumber(buf []byte, i int) (models.FieldType, int, error) {
	start := i
	var isInt, isUnsigned bool

	// Is negative number?
	if i < len(buf) && buf[i] == '-' {
		i++
		// There must be more characters now, as just '-' is illegal.
		if i == len(buf) {
			return models.FieldTypeUnknown, i, ErrInvalidNumber
		}
	}

	// how many decimal points we've see
	decimal := false

	// indicates the number is float in scientific notation
	scientific := false

	for {
		if i >= len(buf) {
			break
		}

		if buf[i] == ',' || buf[i] == ' ' {
			break
		}

		if buf[i] == 'i' && i > start && !(isInt || isUnsigned) {
			isInt = true
			i++
			continue
		} else if buf[i] == 'u' && i > start && !(isInt || isUnsigned) {
			isUnsigned = true
			i++
			continue
		}

		if buf[i] == '.' {
			// Can't have more than 1 decimal (e.g. 1.1.1 should fail)
			if decimal {
				return models.FieldTypeUnknown, i, ErrInvalidNumber
			}
			decimal = true
		}

		// `e` is valid for floats but not as the first char
		if i > start && (buf[i] == 'e' || buf[i] == 'E') {
			scientific = true
			i++
			continue
		}

		// + and - are only valid at this point if they follow an e (scientific notation)
		if (buf[i] == '+' || buf[i] == '-') && (buf[i-1] == 'e' || buf[i-1] == 'E') {
			i++
			continue
		}

		// NaN is an unsupported value
		if i+2 < len(buf) && (buf[i] == 'N' || buf[i] == 'n') {
			return models.FieldTypeUnknown, i, ErrInvalidNumber
		}

		if !isNumeric(buf[i]) {
			return models.FieldTypeUnknown, i, ErrInvalidNumber
		}
		i++
	}

	if (isInt || isUnsigned) && (decimal || scientific) {
		return models.FieldTypeUnknown, i, ErrInvalidNumber
	}

	numericDigits := i - start
	if isInt {
		numericDigits--
	}
	if decimal {
		numericDigits--
	}
	if buf[start] == '-' {
		numericDigits--
	}

	if numericDigits == 0 {
		return models.FieldTypeUnknown, i, ErrInvalidNumber
	}

	numType := models.FieldTypeUnknown

	// It's more common that numbers will be within min/max range for their type but we need to prevent
	// out or range numbers from being parsed successfully.  This uses some simple heuristics to decide
	// if we should parse the number to the actual type.  It does not do it all the time because it incurs
	// extra allocations and we end up converting the type again when writing points to disk.
	if isInt {
		// Make sure the last char is an 'i' for integers (e.g. 9i10 is not valid)
		if buf[i-1] != 'i' {
			return models.FieldTypeUnknown, i, ErrInvalidNumber
		}
		// Parse the int to check bounds the number of digits could be larger than the max range
		// We subtract 1 from the index to remove the `i` from our tests
		if len(buf[start:i-1]) >= maxInt64Digits || len(buf[start:i-1]) >= minInt64Digits {
			if _, err := parseIntBytes(buf[start:i-1], 10, 64); err != nil {
				return models.FieldTypeUnknown, i, fmt.Errorf("unable to parse integer %s: %s", buf[start:i-1], err)
			}
		}
		numType = models.FieldTypeInteger
	} else if isUnsigned {
		// Make sure the last char is a 'u' for unsigned
		if buf[i-1] != 'u' {
			return models.FieldTypeUnknown, i, ErrInvalidNumber
		}
		// Make sure the first char is not a '-' for unsigned
		if buf[start] == '-' {
			return models.FieldTypeUnknown, i, ErrInvalidNumber
		}
		// Parse the uint to check bounds the number of digits could be larger than the max range
		// We subtract 1 from the index to remove the `u` from our tests
		if len(buf[start:i-1]) >= maxUint64Digits {
			if _, err := parseUintBytes(buf[start:i-1], 10, 64); err != nil {
				return models.FieldTypeUnknown, i, fmt.Errorf("unable to parse unsigned %s: %s", buf[start:i-1], err)
			}
		}
		numType = models.FieldTypeUnsigned
	} else {
		// Parse the float to check bounds if it's scientific or the number of digits could be larger than the max range
		if scientific || len(buf[start:i]) >= maxFloat64Digits || len(buf[start:i]) >= minFloat64Digits {
			if _, err := parseFloatBytes(buf[start:i], 64); err != nil {
				return models.FieldTypeUnknown, i, fmt.Errorf("invalid float")
			}
		}
		numType = models.FieldTypeFloat
	}

	return numType, i, nil
}

func scanBoolean(buf []byte, i int) (int, []byte, error) {
	start := i

	if i < len(buf) && (buf[i] != 't' && buf[i] != 'f' && buf[i] != 'T' && buf[i] != 'F') {
		return i, buf[start:i], fmt.Errorf("invalid boolean")
	}

	i++
	for {
		if i >= len(buf) {
			break
		}

		if buf[i] == ',' || buf[i] == ' ' {
			break
		}
		i++
	}

	// Single char bool (t, T, f, F) is ok
	if i-start == 1 {
		return i, buf[start:i], nil
	}

	// length must be 4 for true or TRUE
	if (buf[start] == 't' || buf[start] == 'T') && i-start != 4 {
		return i, buf[start:i], fmt.Errorf("invalid boolean")
	}

	// length must be 5 for false or FALSE
	if (buf[start] == 'f' || buf[start] == 'F') && i-start != 5 {
		return i, buf[start:i], fmt.Errorf("invalid boolean")
	}

	// Otherwise
	valid := false
	switch buf[start] {
	case 't':
		valid = bytes.Equal(buf[start:i], []byte("true"))
	case 'f':
		valid = bytes.Equal(buf[start:i], []byte("false"))
	case 'T':
		valid = bytes.Equal(buf[start:i], []byte("TRUE")) || bytes.Equal(buf[start:i], []byte("True"))
	case 'F':
		valid = bytes.Equal(buf[start:i], []byte("FALSE")) || bytes.Equal(buf[start:i], []byte("False"))
	}

	if !valid {
		return i, buf[start:i], fmt.Errorf("invalid boolean")
	}

	return i, buf[start:i], nil

}

const (
	// the number of characters for the largest possible int64 (9223372036854775807)
	maxInt64Digits = 19

	// the number of characters for the smallest possible int64 (-9223372036854775808)
	minInt64Digits = 20

	// the number of characters for the largest possible uint64 (18446744073709551615)
	maxUint64Digits = 20

	// the number of characters required for the largest float64 before a range check
	// would occur during parsing
	maxFloat64Digits = 25

	// the number of characters required for smallest float64 before a range check occur
	// would occur during parsing
	minFloat64Digits = 27
)

// parseIntBytes is a zero-alloc wrapper around strconv.ParseInt.
func parseIntBytes(b []byte, base int, bitSize int) (i int64, err error) {
	s := unsafeBytesToString(b)
	return strconv.ParseInt(s, base, bitSize)
}

// parseUintBytes is a zero-alloc wrapper around strconv.ParseUint.
func parseUintBytes(b []byte, base int, bitSize int) (i uint64, err error) {
	s := unsafeBytesToString(b)
	return strconv.ParseUint(s, base, bitSize)
}

// parseFloatBytes is a zero-alloc wrapper around strconv.ParseFloat.
func parseFloatBytes(b []byte, bitSize int) (float64, error) {
	s := unsafeBytesToString(b)
	return strconv.ParseFloat(s, bitSize)
}

// parseBoolBytes is a zero-alloc wrapper around strconv.ParseBool.
func parseBoolBytes(b []byte) (bool, error) {
	return strconv.ParseBool(unsafeBytesToString(b))
}

// unsafeBytesToString converts a []byte to a string without a heap allocation.
func unsafeBytesToString(in []byte) string {
	return *(*string)(unsafe.Pointer(&in))
}

// is stopped.  The values are the raw byte slices and not the converted types.
func walkFields(buf []byte, fn func(key, value []byte) bool) error {
	var i int
	var key, val []byte
	for len(buf) > 0 {
		i, key = scanTo(buf, 0, '=')
		if i > len(buf)-2 {
			return fmt.Errorf("invalid value: field-key=%s", key)
		}
		buf = buf[i+1:]
		i, val = scanFieldValue(buf, 0)
		buf = buf[i:]
		if !fn(key, val) {
			break
		}

		// slice off comma
		if len(buf) > 0 {
			buf = buf[1:]
		}
	}
	return nil
}

func scanFieldValue(buf []byte, i int) (int, []byte) {
	start := i
	quoted := false
	for i < len(buf) {
		// Only escape char for a field value is a double-quote and backslash
		if buf[i] == '\\' && i+1 < len(buf) && (buf[i+1] == '"' || buf[i+1] == '\\') {
			i += 2
			continue
		}

		// Quoted value? (e.g. string)
		if buf[i] == '"' {
			i++
			quoted = !quoted
			continue
		}

		if buf[i] == ',' && !quoted {
			break
		}
		i++
	}
	return i, buf[start:i]
}

func scanTime(buf []byte, i int) (int, []byte, error) {
	start := skipWhitespace(buf, i)
	i = start

	for {
		// reached the end of buf?
		if i >= len(buf) {
			break
		}

		// Reached end of block or trailing whitespace?
		if buf[i] == '\n' || buf[i] == ' ' {
			break
		}

		// Handle negative timestamps
		if i == start && buf[i] == '-' {
			i++
			continue
		}

		// Timestamps should be integers, make sure they are so we don't need
		// to actually  parse the timestamp until needed.
		if buf[i] < '0' || buf[i] > '9' {
			return i, buf[start:i], fmt.Errorf("bad timestamp")
		}
		i++
	}
	return i, buf[start:i], nil
}
