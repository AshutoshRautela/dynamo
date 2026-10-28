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
	"path"
	"strings"
	"unicode/utf8"

	v1beta2 "github.com/ai-dynamo/dynamo/deploy/operator/api/v1beta2"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

// TraceMountPath is where the Run Job mounts spec.workload.trace.source.pvc read-only.
const TraceMountPath = "/var/run/aisimulate/trace"

// maxTracePathLength is the CRD maxLength of spec.workload.trace.source.pvc.path.
const maxTracePathLength = 1024

// TracePath resolves a claim-relative trace path under TraceMountPath. Cleaning it as an
// absolute path first keeps ".." from leaving the mount even if validation was bypassed.
func TracePath(claimPath string) string {
	return path.Join(TraceMountPath, path.Clean("/"+claimPath))
}

// TraceSource returns the traffic.source object that trace owns: a trace of its format read from
// its one claim file under TraceMountPath. trace must not be nil. A trace without a pvc source has
// no paths; the CRD schema and ValidateTraceWorkload reject that shape.
func TraceSource(trace *v1beta2.TraceWorkloadSpec) map[string]any {
	source := map[string]any{"type": "trace", "format": string(trace.Format)}
	if pvc := trace.Source.PVC; pvc != nil {
		source["paths"] = []any{TracePath(pvc.Path)}
	}
	return source
}

// ValidateTraceWorkload reports every workload.trace value that the Request and Run CRD schemas
// reject, in the same terms. It covers callers the schema does not protect: the v1beta1 envelope
// in admission, and the Request controller before it creates a Run. workload must not be nil; a
// workload without a trace is valid here.
func ValidateTraceWorkload(workload *v1beta2.WorkloadSpec, fldPath *field.Path) field.ErrorList {
	trace := workload.Trace
	if trace == nil {
		return nil
	}
	allErrs := field.ErrorList{}
	if workload.Static != nil {
		allErrs = append(allErrs, field.Forbidden(fldPath.Child("static"), "static and trace are mutually exclusive"))
	}

	// The format has no default: AISimulate and the DEP default to mooncake, which is not admitted.
	tracePath := fldPath.Child("trace")
	switch trace.Format {
	case v1beta2.TraceFormatDynamo:
	case "":
		allErrs = append(allErrs, field.Required(tracePath.Child("format"), ""))
	default:
		allErrs = append(allErrs, field.NotSupported(tracePath.Child("format"), trace.Format,
			[]v1beta2.TraceFormat{v1beta2.TraceFormatDynamo}))
	}

	// The claim is a PersistentVolumeClaim name and the path one nameable file that stays inside it.
	pvcPath := tracePath.Child("source", "pvc")
	if pvc := trace.Source.PVC; pvc == nil {
		allErrs = append(allErrs, field.Required(pvcPath, "pvc is required"))
	} else {
		for _, msg := range validation.IsDNS1123Subdomain(pvc.ClaimName) {
			allErrs = append(allErrs, field.Invalid(pvcPath.Child("claimName"), pvc.ClaimName, msg))
		}
		if !validTraceClaimPath(pvc.Path) {
			allErrs = append(allErrs, field.Invalid(pvcPath.Child("path"), pvc.Path,
				"path must be relative to the claim root without empty, '.' or '..' segments"))
		}
		if strings.ContainsRune(pvc.Path, 0) {
			allErrs = append(allErrs, field.Invalid(pvcPath.Child("path"), pvc.Path, "path must not contain a NUL character"))
		}
	}

	if replay := trace.Replay; replay != nil && replay.ArrivalSpeedupRatio != nil && *replay.ArrivalSpeedupRatio <= 0 {
		allErrs = append(allErrs, field.Invalid(tracePath.Child("replay", "arrivalSpeedupRatio"),
			*replay.ArrivalSpeedupRatio, "must be greater than 0"))
	}
	return allErrs
}

// validTraceClaimPath mirrors the CRD rules on spec.workload.trace.source.pvc.path: 1 to 1024
// characters, not absolute, no trailing '/', and no empty, '.' or '..' segment.
func validTraceClaimPath(claimPath string) bool {
	if claimPath == "" || utf8.RuneCountInString(claimPath) > maxTracePathLength {
		return false
	}
	for segment := range strings.SplitSeq(claimPath, "/") {
		if segment == "" || segment == "." || segment == ".." {
			return false
		}
	}
	return true
}
