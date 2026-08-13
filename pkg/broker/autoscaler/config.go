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

package autoscaler

import (
	"fmt"
	"math"
	"net"
	"strconv"
	"strings"
)

const (
	AnnotationDomain = "autoscaling.eventing.knative.dev"

	ClassAnnotation                  = AnnotationDomain + "/class"
	MinScaleAnnotation               = AnnotationDomain + "/min-scale"
	MaxScaleAnnotation               = AnnotationDomain + "/max-scale"
	PollingIntervalAnnotation        = AnnotationDomain + "/polling-interval"
	CooldownPeriodAnnotation         = AnnotationDomain + "/cooldown-period"
	LagThresholdAnnotation           = AnnotationDomain + "/lag-threshold"
	ActivationLagThresholdAnnotation = AnnotationDomain + "/activation-lag-threshold"

	KEDAClass = "keda.autoscaling.knative.dev"

	DefaultMinScale               int64 = 0
	DefaultMaxScale               int64 = 50
	DefaultPollingInterval        int64 = 10
	DefaultCooldownPeriod         int64 = 30
	DefaultLagThreshold           int64 = 100
	DefaultActivationLagThreshold int64 = 0
	DefaultAccount                      = "$G"
)

// Settings contains the Broker-wide KEDA settings resolved from annotations.
type Settings struct {
	Enabled         bool
	MinScale        int64
	MaxScale        int64
	PollingInterval int64
	CooldownPeriod  int64
}

// MonitoringConfig is the validated NATS monitoring configuration used in a
// KEDA nats-jetstream trigger.
type MonitoringConfig struct {
	Endpoint string
	Account  string
	UseHTTPS bool
}

// ResolveSettings parses Broker annotations. KEDA is enabled only by an exact
// class match; missing, disabled, or another class keeps the existing static
// replica behavior.
func ResolveSettings(annotations map[string]string) (Settings, error) {
	settings := Settings{
		Enabled:         annotations[ClassAnnotation] == KEDAClass,
		MinScale:        DefaultMinScale,
		MaxScale:        DefaultMaxScale,
		PollingInterval: DefaultPollingInterval,
		CooldownPeriod:  DefaultCooldownPeriod,
	}
	if !settings.Enabled {
		return settings, nil
	}

	var err error
	if settings.MinScale, err = annotationInt64(annotations, MinScaleAnnotation, DefaultMinScale); err != nil {
		return Settings{}, err
	}
	if settings.MaxScale, err = annotationInt64(annotations, MaxScaleAnnotation, DefaultMaxScale); err != nil {
		return Settings{}, err
	}
	if settings.PollingInterval, err = annotationInt64(annotations, PollingIntervalAnnotation, DefaultPollingInterval); err != nil {
		return Settings{}, err
	}
	if settings.CooldownPeriod, err = annotationInt64(annotations, CooldownPeriodAnnotation, DefaultCooldownPeriod); err != nil {
		return Settings{}, err
	}

	switch {
	case settings.MinScale < 0:
		return Settings{}, fmt.Errorf("%s must be at least 0", MinScaleAnnotation)
	case settings.MaxScale < 1:
		return Settings{}, fmt.Errorf("%s must be at least 1", MaxScaleAnnotation)
	case settings.MaxScale > math.MaxInt32:
		return Settings{}, fmt.Errorf("%s must be at most %d", MaxScaleAnnotation, math.MaxInt32)
	case settings.MaxScale < settings.MinScale:
		return Settings{}, fmt.Errorf("%s must be greater than or equal to %s", MaxScaleAnnotation, MinScaleAnnotation)
	case settings.PollingInterval < 1:
		return Settings{}, fmt.Errorf("%s must be at least 1", PollingIntervalAnnotation)
	case settings.CooldownPeriod < 0:
		return Settings{}, fmt.Errorf("%s must be at least 0", CooldownPeriodAnnotation)
	}

	return settings, nil
}

// FallbackReplicaCountFromAnnotations resolves the safety floor independently
// of the rest of the KEDA settings. A malformed unrelated annotation must not
// make a valid min-scale disappear during controller fallback.
func FallbackReplicaCountFromAnnotations(annotations map[string]string) int64 {
	minScale, err := annotationInt64(annotations, MinScaleAnnotation, DefaultMinScale)
	if err != nil || minScale < 0 || minScale > math.MaxInt32 {
		return 1
	}
	return FallbackReplicaCount(minScale)
}

// ResolveLagThresholds applies Trigger annotations before Broker annotations.
func ResolveLagThresholds(triggerAnnotations, brokerAnnotations map[string]string) (int64, int64, error) {
	lag, err := layeredAnnotationInt64(triggerAnnotations, brokerAnnotations, LagThresholdAnnotation, DefaultLagThreshold)
	if err != nil {
		return 0, 0, err
	}
	activationLag, err := layeredAnnotationInt64(triggerAnnotations, brokerAnnotations, ActivationLagThresholdAnnotation, DefaultActivationLagThreshold)
	if err != nil {
		return 0, 0, err
	}
	if lag < 1 {
		return 0, 0, fmt.Errorf("%s must be at least 1", LagThresholdAnnotation)
	}
	if activationLag < 0 {
		return 0, 0, fmt.Errorf("%s must be at least 0", ActivationLagThresholdAnnotation)
	}
	return lag, activationLag, nil
}

// ValidateMonitoringConfig validates and defaults the common NATS autoscaler
// settings. The endpoint is required only after a Broker opts in.
func ValidateMonitoringConfig(endpoint, account string, useHTTPS bool) (MonitoringConfig, error) {
	if endpoint == "" {
		return MonitoringConfig{}, fmt.Errorf("autoscaler.monitoringEndpoint is required")
	}
	if strings.Contains(endpoint, "://") {
		return MonitoringConfig{}, fmt.Errorf("autoscaler.monitoringEndpoint must be host:port without a URL scheme")
	}
	if _, _, err := net.SplitHostPort(endpoint); err != nil {
		return MonitoringConfig{}, fmt.Errorf("autoscaler.monitoringEndpoint must be a valid host:port: %w", err)
	}
	if account == "" {
		account = DefaultAccount
	}
	return MonitoringConfig{Endpoint: endpoint, Account: account, UseHTTPS: useHTTPS}, nil
}

func layeredAnnotationInt64(primary, secondary map[string]string, key string, defaultValue int64) (int64, error) {
	if value := primary[key]; value != "" {
		return parseAnnotationInt64(key, value)
	}
	return annotationInt64(secondary, key, defaultValue)
}

func annotationInt64(annotations map[string]string, key string, defaultValue int64) (int64, error) {
	if value := annotations[key]; value != "" {
		return parseAnnotationInt64(key, value)
	}
	return defaultValue, nil
}

func parseAnnotationInt64(key, value string) (int64, error) {
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%s must be an integer, got %q: %w", key, value, err)
	}
	return parsed, nil
}
