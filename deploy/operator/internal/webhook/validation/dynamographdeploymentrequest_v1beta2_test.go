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

package validation

import (
	"strings"
	"testing"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

func v1beta2Details(errs field.ErrorList) string {
	var out []string
	for _, err := range errs {
		out = append(out, err.Field+": "+err.Detail)
	}
	return strings.Join(out, "\n")
}

func v1beta2RequireError(t *testing.T, errs field.ErrorList, substr string) {
	t.Helper()
	if !strings.Contains(v1beta2Details(errs), substr) {
		t.Fatalf("want an error containing %q, got:\n%s", substr, v1beta2Details(errs))
	}
}

func v1beta2RequireNoError(t *testing.T, errs field.ErrorList) {
	t.Helper()
	if len(errs) > 0 {
		t.Fatalf("unexpected errors:\n%s", v1beta2Details(errs))
	}
}

func v1beta2Envelope(raw string) *apiextensionsv1.JSON {
	return &apiextensionsv1.JSON{Raw: []byte(raw)}
}

func TestValidateEnvelopeV1beta2(t *testing.T) {
	path := field.NewPath("spec", "v1beta2")

	v1beta2RequireNoError(t, validateDynamoGraphDeploymentRequestEnvelopeV1beta2(
		v1beta2Envelope(`{"model":{"name":"meta-llama/Llama-3.1-8B-Instruct"}}`), path))

	v1beta2RequireError(t, validateDynamoGraphDeploymentRequestEnvelopeV1beta2(v1beta2Envelope(`{"model": 3}`), path), "spec.v1beta2: must be a valid v1beta2 spec")
}

func TestValidateEnvelopeV1beta2Trace(t *testing.T) {
	const model = `"model":{"name":"Qwen/Qwen3-0.6B"}`
	const trace = `"trace":{"format":"dynamo","source":{"pvc":{"claimName":"request-traces","path":"retune/w-1.jsonl.gz"}}}`
	tests := []struct {
		name string
		spec string
		want []string
	}{
		{
			name: "a valid trace",
			spec: `{` + model + `,"workload":{` + trace + `}}`,
		},
		{
			name: "a trace allows load and stop overrides",
			spec: `{` + model + `,"workload":{` + trace + `},"aisimulate":{"inputOverrides":{"traffic":{"load":{"speedup":2},"stop":{"max_virtual_time_seconds":600}}}}}`,
		},
		{
			name: "source overrides stay allowed without a trace",
			spec: `{` + model + `,"aisimulate":{"inputOverrides":{"traffic":{"source":{"type":"trace","format":"dynamo","paths":["/x"]}}}}}`,
		},
		{
			name: "a claim name that is not a DNS subdomain",
			spec: `{` + model + `,"workload":{"trace":{"format":"dynamo","source":{"pvc":{"claimName":"Bad_Claim","path":"w.jsonl"}}}}}`,
			want: []string{`spec.workload.trace.source.pvc.claimName: Invalid value: "Bad_Claim": a lowercase RFC 1123 subdomain`},
		},
		{
			name: "a path that leaves the claim",
			spec: `{` + model + `,"workload":{"trace":{"format":"dynamo","source":{"pvc":{"claimName":"traces","path":"../w.jsonl"}}}}}`,
			want: []string{`spec.workload.trace.source.pvc.path: Invalid value: "../w.jsonl": path must be relative to the claim root`},
		},
		{
			name: "static beside a trace",
			spec: `{` + model + `,"workload":{"static":{},` + trace + `}}`,
			want: []string{"spec.workload.static: Forbidden: static and trace are mutually exclusive"},
		},
		{
			name: "overrides of the trace source",
			spec: `{` + model + `,"workload":{` + trace + `},"aisimulate":{"inputOverrides":{"traffic":{"source":{"type":"synthetic","format":"mooncake","paths":["/etc/passwd"]}}}}}`,
			want: []string{
				"spec.aisimulate.inputOverrides.traffic.source.format: Forbidden: is owned by spec.workload.trace",
				"spec.aisimulate.inputOverrides.traffic.source.paths: Forbidden: is owned by spec.workload.trace",
				"spec.aisimulate.inputOverrides.traffic.source.type: Forbidden: is owned by spec.workload.trace",
			},
		},
		{
			name: "a null traffic override would delete the trace source",
			spec: `{` + model + `,"workload":{` + trace + `},"aisimulate":{"inputOverrides":{"traffic":null}}}`,
			want: []string{"spec.aisimulate.inputOverrides.traffic: Forbidden: must be an object because spec.workload.trace owns traffic.source"},
		},
		{
			name: "a null source override would delete the trace source",
			spec: `{` + model + `,"workload":{` + trace + `},"aisimulate":{"inputOverrides":{"traffic":{"source":null}}}}`,
			want: []string{"spec.aisimulate.inputOverrides.traffic.source: Forbidden: is owned by spec.workload.trace"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			errs := validateDynamoGraphDeploymentRequestEnvelopeV1beta2(v1beta2Envelope(tt.spec), field.NewPath("spec", "v1beta2"))
			if len(errs) != len(tt.want) {
				t.Fatalf("errors:\n%s\nwant %d matching %v", v1beta2Details(errs), len(tt.want), tt.want)
			}
			for i, want := range tt.want {
				if !strings.HasPrefix(errs[i].Error(), want) {
					t.Errorf("error[%d] = %q, want prefix %q", i, errs[i].Error(), want)
				}
			}
		})
	}
}
