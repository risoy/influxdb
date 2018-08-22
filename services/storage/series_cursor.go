package storage

import (
	"context"
	"errors"
	"sort"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxql"
	"github.com/opentracing/opentracing-go"
)

const (
	measurementKey = "_measurement"
	fieldKey       = "_field"
)

var (
	measurementKeyBytes = []byte(measurementKey)
	fieldKeyBytes       = []byte(fieldKey)
)

type SeriesIndex interface {
	CreateCursor(ctx context.Context, req *ReadRequest, shards []*tsdb.Shard)
}

type SeriesCursor interface {
	Close()
	Next() *SeriesRow
	Err() error
}

type SeriesRow struct {
	sortKey   []byte
	name      []byte      // measurement name
	stags     models.Tags // unmodified series tags
	field     string
	tags      models.Tags
	query     tsdb.CursorIterators
	valueCond influxql.Expr
}

type indexSeriesCursor struct {
	sqry            tsdb.SeriesCursor
	fields          measurementFields
	nf              []field
	field           field
	err             error
	tags            models.Tags
	cond            influxql.Expr
	measurementCond influxql.Expr
	row             SeriesRow
	eof             bool
	hasFieldExpr    bool
	hasValueExpr    bool
}

func newIndexSeriesCursor(ctx context.Context, predicate *Predicate, shards []*tsdb.Shard) (*indexSeriesCursor, error) {
	queries, err := tsdb.CreateCursorIterators(ctx, shards)
	if err != nil {
		return nil, err
	}

	if queries == nil {
		return nil, nil
	}

	span := opentracing.SpanFromContext(ctx)
	if span != nil {
		span = opentracing.StartSpan("index_cursor.create", opentracing.ChildOf(span.Context()))
		defer span.Finish()
	}

	opt := query.IteratorOptions{
		Aux:        []influxql.VarRef{{Val: "key"}},
		Authorizer: query.OpenAuthorizer,
		Ascending:  true,
		Ordered:    true,
	}
	p := &indexSeriesCursor{row: SeriesRow{query: queries}}

	if root := predicate.GetRoot(); root != nil {
		if p.cond, err = NodeToExpr(root, measurementRemap); err != nil {
			return nil, err
		}

		p.hasFieldExpr, p.hasValueExpr = HasFieldKeyOrValue(p.cond)
		if !(p.hasFieldExpr || p.hasValueExpr) {
			p.measurementCond = p.cond
			opt.Condition = p.cond
		} else {
			p.measurementCond = influxql.Reduce(RewriteExprRemoveFieldValue(influxql.CloneExpr(p.cond)), nil)
			if isTrueBooleanLiteral(p.measurementCond) {
				p.measurementCond = nil
			}

			opt.Condition = influxql.Reduce(RewriteExprRemoveFieldKeyAndValue(influxql.CloneExpr(p.cond)), nil)
			if isTrueBooleanLiteral(opt.Condition) {
				opt.Condition = nil
			}
		}
	}

	var mitr tsdb.MeasurementIterator
	name, singleMeasurement := HasSingleMeasurementNoOR(p.measurementCond)
	if singleMeasurement {
		mitr = tsdb.NewMeasurementSliceIterator([][]byte{[]byte(name)})
	}

	sg := tsdb.Shards(shards)
	p.sqry, err = sg.CreateSeriesCursor(ctx, tsdb.SeriesCursorRequest{Measurements: mitr}, opt.Condition)
	if p.sqry != nil && err == nil {
		// Optimisation to check if request is only interested in results for a
		// single measurement. In this case we can efficiently produce all known
		// field keys from the collection of shards without having to go via
		// the query engine.
		if singleMeasurement {
			fkeys := sg.FieldKeysByMeasurement([]byte(name))
			if len(fkeys) == 0 {
				goto CLEANUP
			}

			fields := make([]field, 0, len(fkeys))
			for _, key := range fkeys {
				fields = append(fields, field{n: key, nb: []byte(key)})
			}
			p.fields = map[string][]field{name: fields}
			return p, nil
		}

		var (
			itr query.Iterator
			fi  query.FloatIterator
		)
		if itr, err = sg.CreateIterator(ctx, &influxql.Measurement{SystemIterator: "_fieldKeys"}, opt); itr != nil && err == nil {
			if fi, err = toFloatIterator(itr); err != nil {
				goto CLEANUP
			}

			p.fields = extractFields(fi)
			fi.Close()
			if len(p.fields) == 0 {
				goto CLEANUP
			}
			return p, nil
		}
	}

CLEANUP:
	p.Close()
	return nil, err
}

func (c *indexSeriesCursor) Close() {
	if !c.eof {
		c.eof = true
		if c.sqry != nil {
			c.sqry.Close()
			c.sqry = nil
		}
	}
}

func copyTags(dst, src models.Tags) models.Tags {
	if cap(dst) < src.Len() {
		dst = make(models.Tags, src.Len())
	} else {
		dst = dst[:src.Len()]
	}
	copy(dst, src)
	return dst
}

func (c *indexSeriesCursor) Next() *SeriesRow {
	if c.eof {
		return nil
	}

	for {
		if len(c.nf) == 0 {
			// next series key
			sr, err := c.sqry.Next()
			if err != nil {
				c.err = err
				c.Close()
				return nil
			} else if sr == nil {
				c.Close()
				return nil
			}

			c.row.name = sr.Name
			c.row.stags = sr.Tags
			c.tags = copyTags(c.tags, sr.Tags)
			c.tags.Set(measurementKeyBytes, sr.Name)

			c.nf = c.fields[string(sr.Name)]
			// c.nf may be nil if there are no fields
		} else {
			c.field, c.nf = c.nf[0], c.nf[1:]

			if c.measurementCond == nil || evalExprBool(c.measurementCond, c) {
				break
			}
		}
	}

	c.tags.Set(fieldKeyBytes, c.field.nb)
	c.row.field = c.field.n

	if c.cond != nil && c.hasValueExpr {
		// TODO(sgc): lazily evaluate valueCond
		c.row.valueCond = influxql.Reduce(c.cond, c)
		if isTrueBooleanLiteral(c.row.valueCond) {
			// we've reduced the expression to "true"
			c.row.valueCond = nil
		}
	}

	c.row.tags = copyTags(c.row.tags, c.tags)

	return &c.row
}

func (c *indexSeriesCursor) Value(key string) (interface{}, bool) {
	switch key {
	case "_name":
		return c.row.name, true
	case fieldKey:
		return c.row.field, true
	default:
		res := c.row.stags.Get([]byte(key))
		return res, res != nil
	}
}

func (c *indexSeriesCursor) Err() error {
	return c.err
}

type limitSeriesCursor struct {
	SeriesCursor
	n, o, c int64
}

func NewLimitSeriesCursor(ctx context.Context, cur SeriesCursor, n, o int64) SeriesCursor {
	return &limitSeriesCursor{SeriesCursor: cur, o: o, n: n}
}

func (c *limitSeriesCursor) Next() *SeriesRow {
	if c.o > 0 {
		for i := int64(0); i < c.o; i++ {
			if c.SeriesCursor.Next() == nil {
				break
			}
		}
		c.o = 0
	}

	if c.c >= c.n {
		return nil
	}
	c.c++
	return c.SeriesCursor.Next()
}

func isTrueBooleanLiteral(expr influxql.Expr) bool {
	b, ok := expr.(*influxql.BooleanLiteral)
	if ok {
		return b.Val
	}
	return false
}

func toFloatIterator(iter query.Iterator) (query.FloatIterator, error) {
	sitr, ok := iter.(query.FloatIterator)
	if !ok {
		return nil, errors.New("expected FloatIterator")
	}

	return sitr, nil
}

type measurementFields map[string][]field

type field struct {
	n  string
	nb []byte
}

func extractFields(itr query.FloatIterator) measurementFields {
	mf := make(measurementFields)

	for {
		p, err := itr.Next()
		if err != nil {
			return nil
		} else if p == nil {
			break
		}

		// Aux is populated by `fieldKeysIterator#Next`
		fields := append(mf[p.Name], field{
			n: p.Aux[0].(string),
		})

		mf[p.Name] = fields
	}

	if len(mf) == 0 {
		return nil
	}

	for k, fields := range mf {
		sort.Slice(fields, func(i, j int) bool {
			return fields[i].n < fields[j].n
		})

		// deduplicate
		i := 1
		fields[0].nb = []byte(fields[0].n)
		for j := 1; j < len(fields); j++ {
			if fields[j].n != fields[j-1].n {
				fields[i] = fields[j]
				fields[i].nb = []byte(fields[i].n)
				i++
			}
		}

		mf[k] = fields[:i]
	}

	return mf
}
