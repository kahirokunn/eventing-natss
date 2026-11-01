/*
Copyright 2026 The Knative Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package filter

import (
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"go.uber.org/zap"

	brokerutils "knative.dev/eventing-natss/pkg/broker/utils"
)

// pendingDelivery keeps the message alive until processing ends. Progress must
// stop before settlement: sending it after a delayed NAK would reset that delay.
type pendingDelivery struct {
	*nats.Msg
	stop chan struct{}
	done chan struct{}
	once sync.Once
}

func newPendingDelivery(msg *nats.Msg, logger *zap.SugaredLogger) *pendingDelivery {
	d := &pendingDelivery{Msg: msg, stop: make(chan struct{}), done: make(chan struct{})}
	go func() {
		defer close(d.done)
		// Every consumer configuration produced by the Trigger controller has
		// at least AckWaitMargin. Renew more often than that minimum, including
		// when a new configuration shortens AckWait during an old request.
		ticker := time.NewTicker(brokerutils.AckWaitMargin / 2)
		defer ticker.Stop()
		for {
			select {
			case <-d.stop:
				return
			case <-ticker.C:
				if err := d.InProgress(); err != nil {
					logger.Warnw("failed to extend AckWait while processing message", zap.Error(err))
				}
			}
		}
	}()
	return d
}

// stopProgress is also deferred by HandleMessage, including for canceled or
// failed dispatches that leave the message pending. Waiting closes the race
// between a progress write and the following ACK, NAK or TERM.
func (d *pendingDelivery) stopProgress() {
	d.once.Do(func() { close(d.stop) })
	<-d.done
}

func (d *pendingDelivery) Ack(opts ...nats.AckOpt) error {
	d.stopProgress()
	return d.Msg.Ack(opts...)
}

func (d *pendingDelivery) NakWithDelay(delay time.Duration, opts ...nats.AckOpt) error {
	d.stopProgress()
	return d.Msg.NakWithDelay(delay, opts...)
}

func (d *pendingDelivery) Term(opts ...nats.AckOpt) error {
	d.stopProgress()
	return d.Msg.Term(opts...)
}
