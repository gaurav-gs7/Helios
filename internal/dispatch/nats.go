package dispatch

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/gauravgs7/helios/internal/domain"
	"github.com/nats-io/nats.go"
)

type Dispatcher struct {
	conn *nats.Conn
}

func New(url string) (*Dispatcher, error) {
	conn, err := nats.Connect(url)
	if err != nil {
		return nil, fmt.Errorf("connect to nats: %w", err)
	}
	return &Dispatcher{conn: conn}, nil
}

func (d *Dispatcher) Close() {
	if d.conn != nil {
		d.conn.Close()
	}
}

func (d *Dispatcher) PublishAssignment(ctx context.Context, assignment domain.Assignment) error {
	body, err := json.Marshal(assignment)
	if err != nil {
		return fmt.Errorf("marshal assignment: %w", err)
	}
	if err := d.conn.Publish(subjectForWorker(assignment.WorkerID), body); err != nil {
		return err
	}
	flushCtx := ctx
	if _, ok := flushCtx.Deadline(); !ok {
		var cancel context.CancelFunc
		flushCtx, cancel = context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
	}
	return d.conn.FlushWithContext(flushCtx)
}

func (d *Dispatcher) Subscribe(workerID string, handler nats.MsgHandler) (*nats.Subscription, error) {
	return d.conn.Subscribe(subjectForWorker(workerID), handler)
}

func subjectForWorker(workerID string) string {
	return "helios.assign." + workerID
}
