package pipeline

import (
	"context"
	"fmt"
	"sync"

	"github.com/hashicorp/go-multierror"
)

var EmptyPipelineError = fmt.Errorf("Empty pipeline cannot run")

type Pipeline[M any] struct {
	sync.WaitGroup
	ctx context.Context
	channels []chan M
	errors chan error
}

func NewPipeline[M any](ctx context.Context) *Pipeline[M] {
	return &Pipeline[M]{
		ctx: ctx,
		errors: make(chan error),
	}
}

// connect f to last channel in the pipeline
func (p *Pipeline[M])AddStep(n int, f func(M) (M, error) ) {

	in, out := p.nextChannels()

	var wg = new(sync.WaitGroup)
	p.Add(1) // step wait group
	wg.Add(n) // channel close wait group
	for i := 0; i < n; i++ { 
		go p.marshal(wg, in, out, f)
	}
	go func() { wg.Wait(); close(out); p.Done() }()
}

func(p *Pipeline[M])nextChannels() (in, out chan M) {
	if p.channels == nil {
		p.channels = make([]chan M, 1, 2)
		p.channels[0] = make(chan M)
	}
	in = p.channels[len(p.channels)-1]
	out = make(chan M)
	p.channels =append(p.channels, out)
	return in, out
}

func (p *Pipeline[M])AddProducerStep(n int, f func(M, chan<- M) error) {
	in, out := p.nextChannels()

	var wg = new(sync.WaitGroup)
	p.Add(1) // step wait group
	wg.Add(n) // channel close wait group
	for i := 0; i < n; i++ { 
		go p.marshalProducer(wg, in, out, f)
	}
	go func() { wg.Wait(); close(out); p.Done() }()
}



func (p *Pipeline[M])marshal(wg *sync.WaitGroup, in <-chan M, out chan<- M, f func(M) (M, error) ) {
	defer wg.Done()
	for m := range in {
		if r, e := f(m); e != nil {
			p.errors <- e
		} else {
			out <- r
		}
	}
}

func (p *Pipeline[M])marshalProducer(wg *sync.WaitGroup, in <-chan M, out chan<- M, f func(M, chan<- M) (error) ) {
	defer wg.Done()
	for m := range in {
		if e := f(m, out); e != nil {
			p.errors <- e
		}
	}
}

func (p *Pipeline[M])Run(collector func(M)) (chan<- M, <-chan error) {
	var errs = make(chan error)
	if p.channels == nil {
		go func() { errs <- EmptyPipelineError }()
		return nil, errs
	}
	p.Add(1)
	go func() {
		defer p.Done()
		for res := range p.channels[len(p.channels)-1] {
			collector(res)
		}
		close(p.errors)
	}()
	p.Add(1)
	go func() {
		var errors *multierror.Error
		defer p.Done()
		defer func() {
			go func() { errs <- errors.ErrorOrNil() }()
		}()
		for err := range p.errors {
			errors = multierror.Append(errors, err)
		}
	}()
	return p.channels[0], errs
}

