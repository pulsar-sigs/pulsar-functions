package prometheus

import (
	"testing"

	"github.com/prometheus/prometheus/prompb"
)

var writeRequestFixture = &prompb.WriteRequest{
	Timeseries: []prompb.TimeSeries{
		{
			Labels: []prompb.Label{
				{Name: "__name__", Value: "test_metric1"},
				{Name: "b", Value: "c"},
				{Name: "baz", Value: "qux"},
				{Name: "d", Value: "e"},
				{Name: "foo", Value: "bar"},
			},
			Samples:   []prompb.Sample{{Value: 1, Timestamp: 0}},
			Exemplars: []prompb.Exemplar{{Labels: []prompb.Label{{Name: "f", Value: "g"}}, Value: 1, Timestamp: 0}},
		},
		{
			Labels: []prompb.Label{
				{Name: "__name__", Value: "test_metric1"},
				{Name: "b", Value: "c"},
				{Name: "baz", Value: "qux"},
				{Name: "d", Value: "e"},
				{Name: "foo", Value: "bar"},
			},
			Samples:   []prompb.Sample{{Value: 2, Timestamp: 1}},
			Exemplars: []prompb.Exemplar{{Labels: []prompb.Label{{Name: "h", Value: "i"}}, Value: 2, Timestamp: 1}},
		},
	},
}

func Test_DoBytesPost(t *testing.T) {
	data, err := writeRequestFixture.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	p := PrometheusHandle{}
	_, err = p.DoBytesPost("localhost:9000/api/v1/prom/write", data)
	if err != nil {
		t.Fatal(err)
	}
}
