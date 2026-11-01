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
	"bytes"
	"testing"
	"testing/synctest"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	brokerutils "knative.dev/eventing-natss/pkg/broker/utils"
)

func TestPendingDelivery_FastCompletionDoesNotSendProgress(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		logger, logs := progressTestLogger()
		delivery := newPendingDelivery(&nats.Msg{}, logger)
		delivery.stopProgress()
		time.Sleep(2 * brokerutils.AckWaitMargin)
		require.Zero(t, logs.Len())
	})
}

func TestPendingDelivery_StopsProgressBeforeSettlement(t *testing.T) {
	for _, action := range []string{"ack", "nak", "term", "canceled dispatch"} {
		t.Run(action, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				logger, logs := progressTestLogger()
				delivery := newPendingDelivery(&nats.Msg{}, logger)
				defer delivery.stopProgress()
				synctest.Wait()
				time.Sleep(brokerutils.AckWaitMargin)
				synctest.Wait()
				// This unbound message rejects progress. Both failures must be
				// observed: a failed write must not silently stop renewal.
				require.Equal(t, 2, bytes.Count(logs.Bytes(), []byte("\n")))

				var err error
				switch action {
				case "ack":
					err = delivery.Ack()
				case "nak":
					err = delivery.NakWithDelay(time.Second)
				case "term":
					err = delivery.Term()
				default:
					delivery.stopProgress()
				}
				if action != "canceled dispatch" {
					require.ErrorIs(t, err, nats.ErrMsgNotBound)
				}
				before := logs.Len()
				time.Sleep(2 * brokerutils.AckWaitMargin)
				synctest.Wait()
				require.Equal(t, before, logs.Len(), "progress continued after processing ended")
			})
		})
	}
}

func progressTestLogger() (*zap.SugaredLogger, *bytes.Buffer) {
	logs := &bytes.Buffer{}
	core := zapcore.NewCore(zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()),
		zapcore.Lock(zapcore.AddSync(logs)), zap.WarnLevel)
	return zap.New(core).Sugar(), logs
}
