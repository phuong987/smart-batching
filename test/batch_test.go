package test

import (
	"reflect"
	"smartbatching"
	"sync"
	"testing"
	"time"
)

type processBench struct {
	mtx sync.Mutex
}

func (p *processBench) Do(key string, data []interface{}) []interface{} {
	_ = key
	// Simulate the resource contention, non-effect for this case because we merge requests to batch for each resource access
	p.mtx.Lock()
	time.Sleep(50 * time.Millisecond)
	p.mtx.Unlock()
	return data
}

// Although 50ms for each resource using, 10.2 ms/op is the average result of 110 calls when apply smart batching
func Benchmark_Add(b *testing.B) {
	s := smartbatching.NewSmartBatching(&processBench{mtx: sync.Mutex{}})
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			s.Add("A", int(100))
		}
	})
}

type processTest struct{}

func (p *processTest) Do(key string, data []interface{}) []interface{} {
	_ = key
	time.Sleep(100 * time.Millisecond)
	return data

}
func TestSmartBatching_Add(t *testing.T) {
	s := smartbatching.NewSmartBatching(&processTest{})

	type args struct {
		key  string
		data interface{}
	}
	tests := []struct {
		name string
		args args
		want interface{}
	}{
		{
			name: "Case add 10 return 10",
			args: args{"A", int(10)},
			want: int(10),
		},
		{
			name: "Case add 20 return 20",
			args: args{"A", int(20)},
			want: int(20),
		},
		{
			name: "Case add 10 return 10",
			args: args{"A", int(10)},
			want: int(10),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			if got := s.Add(tt.args.key, tt.args.data); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("SmartBatching.Add() = %v, want %v", got, tt.want)
			}
		})
	}
}
