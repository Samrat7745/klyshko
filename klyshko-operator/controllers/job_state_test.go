/*
Copyright (c) 2026 - for information on the respective copyright owner
see the NOTICE file and/or the repository https://github.com/carbynestack/klyshko.

SPDX-License-Identifier: Apache-2.0
*/

package controllers

import (
	"testing"

	klyshkov1alpha1 "github.com/Samrat7745/klyshko/klyshko-operator/api/v1alpha1"
)

var allTaskStates = []klyshkov1alpha1.TupleGenerationTaskState{
	klyshkov1alpha1.TaskPreparing, klyshkov1alpha1.TaskLaunching, klyshkov1alpha1.TaskGenerating,
	klyshkov1alpha1.TaskProvisioning, klyshkov1alpha1.TaskCompleted, klyshkov1alpha1.TaskFailed,
}

// Each owned task is either missing (-1) or in one of the task states (including the empty initial one).
func TestComputeJobStateExhaustive(t *testing.T) {
	states := append([]klyshkov1alpha1.TupleGenerationTaskState{""}, allTaskStates...)
	currents := []klyshkov1alpha1.TupleGenerationJobState{"", klyshkov1alpha1.JobPending, klyshkov1alpha1.JobRunning,
		klyshkov1alpha1.JobCompleted, klyshkov1alpha1.JobFailed}
	for vcps := uint(2); vcps <= 4; vcps++ {
		// choices per player: len(states) states + missing
		choices := len(states) + 1
		total := 1
		for i := uint(0); i < vcps; i++ {
			total *= choices
		}
		for combo := 0; combo < total; combo++ {
			var tasks []klyshkov1alpha1.TupleGenerationTask
			c := combo
			for p := uint(0); p < vcps; p++ {
				ch := c % choices
				c /= choices
				if ch == len(states) {
					continue // missing
				}
				tasks = append(tasks, klyshkov1alpha1.TupleGenerationTask{Status: klyshkov1alpha1.TupleGenerationTaskStatus{State: states[ch]}})
			}
			for _, cur := range currents {
				var want klyshkov1alpha1.TupleGenerationJobState
				wantActivate := false
				terminated, failed := 0, false
				for _, tk := range tasks {
					if tk.Status.State == klyshkov1alpha1.TaskCompleted || tk.Status.State == klyshkov1alpha1.TaskFailed {
						terminated++
					}
					if tk.Status.State == klyshkov1alpha1.TaskFailed {
						failed = true
					}
				}
				switch {
				case uint(len(tasks)) < vcps:
					want = klyshkov1alpha1.JobPending
				case terminated < len(tasks):
					want = klyshkov1alpha1.JobRunning
				case failed:
					want = klyshkov1alpha1.JobFailed
				case cur == klyshkov1alpha1.JobCompleted:
					want = ""
				default:
					want, wantActivate = klyshkov1alpha1.JobCompleted, true
				}
				got, act := computeJobState(cur, tasks, vcps)
				if got != want || act != wantActivate {
					t.Fatalf("vcps=%d cur=%q tasks=%v: got (%q,%v) want (%q,%v)", vcps, cur, tasks, got, act, want, wantActivate)
				}
			}
		}
	}
}

func TestComputeJobStateNamedCases(t *testing.T) {
	mk := func(ss ...klyshkov1alpha1.TupleGenerationTaskState) []klyshkov1alpha1.TupleGenerationTask {
		var r []klyshkov1alpha1.TupleGenerationTask
		for _, s := range ss {
			r = append(r, klyshkov1alpha1.TupleGenerationTask{Status: klyshkov1alpha1.TupleGenerationTaskStatus{State: s}})
		}
		return r
	}
	C, F, G := klyshkov1alpha1.TaskCompleted, klyshkov1alpha1.TaskFailed, klyshkov1alpha1.TaskGenerating
	for i, c := range []struct {
		cur   klyshkov1alpha1.TupleGenerationJobState
		tasks []klyshkov1alpha1.TupleGenerationTask
		n     uint
		want  klyshkov1alpha1.TupleGenerationJobState
		act   bool
	}{
		{klyshkov1alpha1.JobPending, mk(C, C), 2, klyshkov1alpha1.JobCompleted, true},
		{klyshkov1alpha1.JobCompleted, mk(C, C), 2, "", false},
		{klyshkov1alpha1.JobRunning, mk(C, F), 2, klyshkov1alpha1.JobFailed, false},
		{klyshkov1alpha1.JobRunning, mk(C, G), 2, klyshkov1alpha1.JobRunning, false},
		{klyshkov1alpha1.JobRunning, mk(C), 2, klyshkov1alpha1.JobPending, false},
		{klyshkov1alpha1.JobRunning, nil, 2, klyshkov1alpha1.JobPending, false},
	} {
		got, act := computeJobState(c.cur, c.tasks, c.n)
		if got != c.want || act != c.act {
			t.Errorf("case %d: got (%q,%v)", i, got, act)
		}
	}
}
