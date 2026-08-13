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

package ci

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestKindE2EFailureDiagnostics(t *testing.T) {
	repoRoot, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	scriptPath := filepath.Join(repoRoot, "hack", "gather-e2e-failure-data.sh")
	workflowPath := filepath.Join(repoRoot, ".github", "workflows", "kind-e2e.yaml")

	workflow, err := os.ReadFile(workflowPath)
	if err != nil {
		t.Fatal(err)
	}
	workflowText := string(workflow)
	if !strings.Contains(workflowText, "bash ./hack/gather-e2e-failure-data.sh") {
		t.Fatal("kind E2E failure step does not invoke the tested diagnostics script")
	}
	for _, want := range []string{
		"set -o pipefail",
		"${RUNNER_TEMP}/kind-e2e-diagnostics",
		"2>&1 | tee \"${diagnostics_dir}/e2e-test.log\"",
		"continue-on-error: true",
		"timeout-minutes: 5",
		"OUT_DIR: ${{ runner.temp }}/kind-e2e-diagnostics",
		"uses: actions/upload-artifact@v4",
		"if: ${{ always() && failure() }}",
		"name: kind-e2e-${{ matrix.k8s-version }}-${{ matrix.eventing-version }}",
		"path: ${{ runner.temp }}/kind-e2e-diagnostics",
		"retention-days: 7",
	} {
		if !strings.Contains(workflowText, want) {
			t.Errorf("kind E2E workflow lacks %q", want)
		}
	}
	if output, err := exec.Command("bash", "-n", scriptPath).CombinedOutput(); err != nil {
		t.Fatalf("diagnostics script has invalid shell syntax: %v\n%s", err, output)
	}

	tempDir := t.TempDir()
	callsPath := filepath.Join(tempDir, "kubectl-calls")
	outDir := filepath.Join(tempDir, "diagnostics")
	fakeKubectl := `#!/usr/bin/env bash
printf '%s\n' "$*" >> "$KUBECTL_CALLS"
request_timeout="$1"
shift
if [[ "$request_timeout" != --request-timeout=* ]]; then
  printf 'missing request timeout: %s\n' "$request_timeout" >&2
  exit 2
fi
if [[ "$1 $2" == "get namespaces" && "$*" == *"app.kubernetes.io/component=reconciler-test"* ]]; then
  printf 'test-one\ntest-two\n'
elif [[ "$1 $2" == "get pods" && "$*" == *"-o name"* ]]; then
  printf 'pod/pod-a\n'
elif [[ "$1 $2 $3" == "get pod pod-a" && "$*" == *"jsonpath="* ]]; then
  printf 'init-setup app sidecar debugger'
elif [[ "$*" == "get deployments -n keda -o wide" ]]; then
  exec sleep 10
else
  printf 'diagnostic-output\n'
fi
`
	if err := os.WriteFile(filepath.Join(tempDir, "kubectl"), []byte(fakeKubectl), 0o755); err != nil {
		t.Fatal(err)
	}

	command := exec.Command("bash", scriptPath)
	command.Dir = repoRoot
	command.Env = append(os.Environ(),
		"PATH="+tempDir+string(os.PathListSeparator)+os.Getenv("PATH"),
		"KUBECTL_CALLS="+callsPath,
		"OUT_DIR="+outDir,
		"KUBECTL_COMMAND_TIMEOUT=0.1s",
		"KUBECTL_KILL_AFTER=0.05s",
		"KUBECTL_REQUEST_TIMEOUT=50ms",
	)
	started := time.Now()
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("diagnostics script must be best-effort; error=%v\n%s", err, output)
	}
	if elapsed := time.Since(started); elapsed > 3*time.Second {
		t.Fatalf("one hung kubectl blocked best-effort collection for %v", elapsed)
	}
	callsBytes, err := os.ReadFile(callsPath)
	if err != nil {
		t.Fatal(err)
	}
	calls := string(callsBytes)
	for _, line := range strings.Split(strings.TrimSpace(calls), "\n") {
		if !strings.HasPrefix(line, "--request-timeout=50ms ") {
			t.Errorf("kubectl call lacks the common request timeout: %q", line)
		}
	}

	for _, namespace := range []string{"knative-eventing", "keda", "nats-io", "test-one", "test-two"} {
		for _, want := range []string{
			"get pods -n " + namespace + " -o wide",
			"get deployments -n " + namespace + " -o wide",
			"get jobs -n " + namespace + " -o wide",
		} {
			if !strings.Contains(calls, want) {
				t.Errorf("kubectl calls lack %q\n%s", want, calls)
			}
		}
		for _, container := range []string{"init-setup", "app", "sidecar", "debugger"} {
			for _, previous := range []string{"", " --previous"} {
				want := "logs -n " + namespace + " pod-a -c " + container + " --timestamps=true --tail=500 --limit-bytes=1048576" + previous
				if !strings.Contains(calls, want) {
					t.Errorf("kubectl calls lack %q\n%s", want, calls)
				}
			}
		}
	}
	if !strings.Contains(calls, "get namespaces -l app.kubernetes.io/component=reconciler-test") {
		t.Errorf("test namespaces were not selected by reconciler-test label\n%s", calls)
	}
	for _, line := range strings.Split(calls, "\n") {
		if strings.HasPrefix(strings.ToLower(line), "describe ") {
			t.Fatalf("diagnostics must not describe resources with literal Pod env values: %q", line)
		}
		fields := strings.Fields(strings.ToLower(line))
		for _, field := range fields {
			if field == "secret" || field == "secrets" || strings.HasPrefix(field, "secret/") {
				t.Fatalf("diagnostics must not query Secret resources: %q", line)
			}
		}
	}
	for _, relativePath := range []string{
		"brokers.yaml",
		"natsjetstreamchannels.yaml",
		"namespaces/keda/deployments-wide.txt",
		"namespaces/keda/jobs-wide.txt",
		"namespaces/test-one/logs/pod-a-app-current.log",
		"namespaces/test-one/logs/pod-a-app-previous.log",
	} {
		contents, err := os.ReadFile(filepath.Join(outDir, relativePath))
		if err != nil {
			t.Errorf("diagnostic artifact %q: %v", relativePath, err)
			continue
		}
		if relativePath != "namespaces/keda/deployments-wide.txt" && !strings.Contains(string(contents), "diagnostic-output") {
			t.Errorf("diagnostic artifact %q lacks command output: %s", relativePath, contents)
		}
	}
}

func TestE2EEnvironmentsAreManaged(t *testing.T) {
	var sources strings.Builder
	for _, path := range []string{"../e2e/main_test.go", "../e2e/natsbroker_test.go"} {
		contents, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		sources.Write(contents)
	}
	text := sources.String()
	if got := strings.Count(text, "global.Environment("); got != 5 {
		t.Fatalf("global.Environment calls = %d, want 5", got)
	}
	if got := strings.Count(text, "environment.Managed(t)"); got != 5 {
		t.Errorf("environment.Managed(t) calls = %d, want one for every Environment", got)
	}
	if strings.Contains(text, "env.Finish()") {
		t.Error("manual env.Finish() bypasses managed testing.T failure state")
	}
}
