package pipeline

import (
	"context"
	"testing"
	"fmt"
)

func TestEmptyPipeline(t *testing.T) {
	p := NewPipeline[struct{}](context.Background())
	c, err := p.Run(nil)
	if c != nil {
		t.Error("Running an empty pipeline should return nil")
	}
	if err == nil {
		t.Fatal("Running an empty pipeline should return nil")
	} else if err := <- err; err != EmptyPipelineError {
		t.Error("Should have gotten empty pipeline error")
	}
}

func TestMultiStepPipeline(t *testing.T) {
	type msg struct { 
		i, res, expected int
	}

	ctx := context.Background()
	var p = NewPipeline[*msg](ctx)
	p.AddStep(2, func(i *msg)(*msg, error) { 
		i.res = i.i * i.i
		return i, nil
	})
	p.AddStep(1, func(i *msg)(*msg, error) {
		i.res = i.res + i.res
		return i, nil
	})
	p.AddStep(10, func(i *msg)(*msg, error) {
		i.res = i.res + i.i
		return i, nil
	})
	var results = make([]msg, 0, 3)
	inputs, errc := p.Run( func(i *msg) { results = append(results, *i) } )
	inputs <- &msg{1,0,3}
	inputs <- &msg{2,0,10}
	inputs <- &msg{3,0,21}
	close(inputs)
	p.Wait()
	if l := len(results); l != 3 {
		t.Errorf("Expected 3 inputs, got %d", l)
	}
	for _, msg := range results {
		if msg.expected != msg.res { 
			t.Errorf("Message %+v result not expected", msg)
		}
	}
	if err := <- errc; err != nil {
		t.Error(err)
	}
}
func TestProducer(t *testing.T){ 
	type msg struct { 
		i, res, expected int
	}

	ctx := context.Background()
	var p = NewPipeline[*msg](ctx)
	p.AddProducerStep(2, func(i *msg, out chan<- *msg)(error) { 
		out <- &msg{i: i.i * i.i, res: 0, expected: (i.i * i.i * i.i * i.i) + i.i * i.i }
		out <- i
		return nil
	})
	p.AddStep(1, func(i *msg)(*msg, error) {
		i.res = i.i * i.i
		return i, nil
	})
	p.AddStep(10, func(i *msg)(*msg, error) {
		i.res = i.res + i.i
		return i, nil
	})
	var results = make([]msg, 0, 3)
	inputs, errc := p.Run( func(i *msg) { results = append(results, *i) } )
	inputs <- &msg{1,0,2}
	inputs <- &msg{2,0,6}
	inputs <- &msg{3,0,12}
	close(inputs)
	p.Wait()
	if l := len(results); l != 6 {
		t.Errorf("Expected 6 inputs, got %d", l)
	}
	for _, msg := range results {
		if msg.expected != msg.res { 
			t.Errorf("Message %+v result not expected", msg)
		}
	}
	if err := <- errc; err != nil {
		t.Error(err)
	}
}


func TestErrPipeline(t *testing.T) {

	ctx := context.Background()
	var p = NewPipeline[string](ctx)
	p.AddStep(2, func(i string)(string, error) { 
		return i, nil
	})
	p.AddStep(1, func(i string)(string, error) {
		if i == "" { return "", fmt.Errorf("empty string") } else { return i, nil }
	})
	var results = make([]string, 0, 3)
	inputs, errc := p.Run( func(i string) { results = append(results, i) } )
	inputs <- ""
	inputs <- "works"
	inputs <- "also works"
	close(inputs)
	p.Wait()
	if l := len(results); l != 2 {
		t.Errorf("Expected 2 inputs, got %d", l)
	}
	for _, msg := range results {
		if msg == ""{ 
			t.Errorf("Message '%s' not expected in results", msg)
		}
	}
	if err := <- errc; err == nil {
		t.Error("Didn't get  expected error back from pipeline")
	} else {
		t.Log(err.Error())
	}
}
