/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package aisimulate

import (
	"strings"
	"testing"

	v1beta2 "github.com/ai-dynamo/dynamo/deploy/operator/api/v1beta2"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

func withTrace(spec *v1beta2.DynamoGraphDeploymentRequestSpec, speedup *float64) *v1beta2.DynamoGraphDeploymentRequestSpec {
	spec.Workload = &v1beta2.WorkloadSpec{Trace: &v1beta2.TraceWorkloadSpec{
		Format: v1beta2.TraceFormatDynamo,
		Source: v1beta2.TraceSourceSpec{PVC: &v1beta2.TracePVCSource{ClaimName: "request-traces", Path: "retune/w-1.jsonl.gz"}},
	}}
	if speedup != nil {
		spec.Workload.Trace.Replay = &v1beta2.TraceReplaySpec{ArrivalSpeedupRatio: speedup}
	}
	return spec
}

func TestLowerTraceWorkload(t *testing.T) {
	tests := []struct {
		name        string
		spec        *v1beta2.DynamoGraphDeploymentRequestSpec
		wantTraffic string
	}{
		{
			name: "recorded arrival times without a request stop",
			spec: withTrace(defaultedSpec(), nil),
			wantTraffic: `{"load":{"type":"trace_timestamps"},` +
				`"source":{"format":"dynamo","paths":["/var/run/aisimulate/trace/retune/w-1.jsonl.gz"],"type":"trace"}}`,
		},
		{
			name: "an arrival speedup lowers to traffic.load.speedup",
			spec: withTrace(defaultedSpec(), new(2.5)),
			wantTraffic: `{"load":{"speedup":2.5,"type":"trace_timestamps"},` +
				`"source":{"format":"dynamo","paths":["/var/run/aisimulate/trace/retune/w-1.jsonl.gz"],"type":"trace"}}`,
		},
		{
			name: "a stop override still applies",
			spec: withInputOverrides(withTrace(defaultedSpec(), nil), `{"traffic":{"stop":{"max_virtual_time_seconds":600}}}`),
			wantTraffic: `{"load":{"type":"trace_timestamps"},` +
				`"source":{"format":"dynamo","paths":["/var/run/aisimulate/trace/retune/w-1.jsonl.gz"],"type":"trace"},` +
				`"stop":{"max_virtual_time_seconds":600}}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			traffic := nested(t, lower(t, tt.spec), "traffic").(map[string]any)
			if got := encode(t, traffic); got != tt.wantTraffic {
				t.Fatalf("traffic\n got: %s\nwant: %s", got, tt.wantTraffic)
			}
		})
	}
}

func TestTracePathStaysUnderTheMount(t *testing.T) {
	for claimPath, want := range map[string]string{
		"retune/w-1.jsonl":    "/var/run/aisimulate/trace/retune/w-1.jsonl",
		"../../etc/passwd":    "/var/run/aisimulate/trace/etc/passwd",
		"/abs/./trace.jsonl/": "/var/run/aisimulate/trace/abs/trace.jsonl",
	} {
		if got := TracePath(claimPath); got != want {
			t.Errorf("TracePath(%q) = %q, want %q", claimPath, got, want)
		}
	}
}

func TestValidateTraceWorkload(t *testing.T) {
	type traceCase struct {
		name   string
		mutate func(*v1beta2.WorkloadSpec)
		want   []string
	}
	tests := []traceCase{
		{name: "a valid trace", mutate: func(*v1beta2.WorkloadSpec) {}},
		{name: "a hidden file and dotted names are ordinary segments", mutate: func(w *v1beta2.WorkloadSpec) {
			w.Trace.Source.PVC.Path = ".cache/a..b/.trace.jsonl"
		}},
		{name: "a workload without a trace", mutate: func(w *v1beta2.WorkloadSpec) {
			w.Trace = nil
			w.Static = &v1beta2.StaticWorkloadSpec{}
		}},
		{name: "static beside a trace", mutate: func(w *v1beta2.WorkloadSpec) {
			w.Static = &v1beta2.StaticWorkloadSpec{}
		}, want: []string{"spec.workload.static: Forbidden: static and trace are mutually exclusive"}},
		{name: "a missing format", mutate: func(w *v1beta2.WorkloadSpec) {
			w.Trace.Format = ""
		}, want: []string{"spec.workload.trace.format: Required value"}},
		{name: "an unsupported format", mutate: func(w *v1beta2.WorkloadSpec) {
			w.Trace.Format = "mooncake"
		}, want: []string{`spec.workload.trace.format: Unsupported value: "mooncake": supported values: "dynamo"`}},
		{name: "a missing pvc", mutate: func(w *v1beta2.WorkloadSpec) {
			w.Trace.Source.PVC = nil
		}, want: []string{"spec.workload.trace.source.pvc: Required value: pvc is required"}},
		{name: "a claim name that is not a DNS subdomain", mutate: func(w *v1beta2.WorkloadSpec) {
			w.Trace.Source.PVC.ClaimName = "Traces_1"
		}, want: []string{`spec.workload.trace.source.pvc.claimName: Invalid value: "Traces_1": a lowercase RFC 1123 subdomain`}},
		{name: "a non-positive speedup", mutate: func(w *v1beta2.WorkloadSpec) {
			w.Trace.Replay.ArrivalSpeedupRatio = new(0.0)
		}, want: []string{"spec.workload.trace.replay.arrivalSpeedupRatio: Invalid value: 0: must be greater than 0"}},
	}
	for _, claimPath := range []string{"", "/abs/trace.jsonl", "dir/", "a//b", ".", "..", "./a", "a/./b", "a/../b", "../a", "a/..", strings.Repeat("a", 1025)} {
		tests = append(tests, traceCase{
			name:   "path " + claimPath[:min(len(claimPath), 16)],
			mutate: func(w *v1beta2.WorkloadSpec) { w.Trace.Source.PVC.Path = claimPath },
			want:   []string{"spec.workload.trace.source.pvc.path: Invalid value: "},
		})
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			workload := withTrace(defaultedSpec(), new(1.0)).Workload
			tt.mutate(workload)
			errs := ValidateTraceWorkload(workload, field.NewPath("spec", "workload"))
			if len(errs) != len(tt.want) {
				t.Fatalf("errors = %v, want %d matching %v", errs, len(tt.want), tt.want)
			}
			for i, want := range tt.want {
				if !strings.HasPrefix(errs[i].Error(), want) {
					t.Errorf("error[%d] = %q, want prefix %q", i, errs[i].Error(), want)
				}
			}
		})
	}
}
