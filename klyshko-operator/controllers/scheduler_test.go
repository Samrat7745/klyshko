/*
Copyright (c) 2026 - for information on the respective copyright owner
see the NOTICE file and/or the repository https://github.com/carbynestack/klyshko.

SPDX-License-Identifier: Apache-2.0
*/

package controllers

import (
	"context"
	"encoding/json"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	klyshkov1alpha1 "github.com/carbynestack/klyshko/api/v1alpha1"
	"github.com/carbynestack/klyshko/castor"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	tBit  = "BIT_GFP"
	tMul  = "MULTIPLICATION_TRIPLE_GFP"
	tInv  = "INVERSE_TUPLE_GFP"
	tMask = "INPUT_MASK_GFP"
)

func tel(m ...castor.TupleMetrics) castor.Telemetry { return castor.Telemetry{TupleMetrics: m} }
func met(t string, avail int) castor.TupleMetrics {
	return castor.TupleMetrics{TupleType: t, Available: avail}
}
func pol(t string, threshold, prio int) klyshkov1alpha1.TupleTypePolicy {
	return klyshkov1alpha1.TupleTypePolicy{Type: t, Threshold: threshold, Priority: prio}
}
func jobOf(t string, count int, state klyshkov1alpha1.TupleGenerationJobState) klyshkov1alpha1.TupleGenerationJob {
	return klyshkov1alpha1.TupleGenerationJob{
		Spec:   klyshkov1alpha1.TupleGenerationJobSpec{Type: t, Count: count},
		Status: klyshkov1alpha1.TupleGenerationJobStatus{State: state},
	}
}

func TestLotteryStrategyTable(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name     string
		telem    castor.Telemetry
		policies []klyshkov1alpha1.TupleTypePolicy
		jobs     []klyshkov1alpha1.TupleGenerationJob
		want     []string // acceptable results; empty means nil
	}{
		{"empty telemetry", tel(), []klyshkov1alpha1.TupleTypePolicy{pol(tBit, 10, 1)}, nil, nil},
		{"no policies", tel(met(tBit, 0)), nil, nil, nil},
		{"above threshold", tel(met(tBit, 10)), []klyshkov1alpha1.TupleTypePolicy{pol(tBit, 10, 1)}, nil, nil},
		{"below threshold", tel(met(tBit, 9)), []klyshkov1alpha1.TupleTypePolicy{pol(tBit, 10, 1)}, nil, []string{tBit}},
		{"in-flight lifts over threshold", tel(met(tBit, 5)), []klyshkov1alpha1.TupleTypePolicy{pol(tBit, 10, 1)},
			[]klyshkov1alpha1.TupleGenerationJob{jobOf(tBit, 5, klyshkov1alpha1.JobRunning)}, nil},
		{"in-flight of other type ignored", tel(met(tBit, 5), met(tMul, 100)), []klyshkov1alpha1.TupleTypePolicy{pol(tBit, 10, 1)},
			[]klyshkov1alpha1.TupleGenerationJob{jobOf(tMul, 500, klyshkov1alpha1.JobRunning)}, []string{tBit}},
		{"type without policy", tel(met(tMul, 0)), []klyshkov1alpha1.TupleTypePolicy{pol(tBit, 10, 1)}, nil, nil},
		{"policy without telemetry", tel(met(tMul, 0)), []klyshkov1alpha1.TupleTypePolicy{pol(tBit, 10, 1), pol(tMul, 1, 1)}, nil, []string{tMul}},
		{"only one below", tel(met(tBit, 0), met(tMul, 99)), []klyshkov1alpha1.TupleTypePolicy{pol(tBit, 10, 1), pol(tMul, 50, 100)}, nil, []string{tBit}},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			for i := 0; i < 50; i++ {
				got := (&LotterySchedulingStrategy{}).Schedule(ctx, c.telem, c.policies, c.jobs)
				if len(c.want) == 0 {
					if got != nil {
						t.Fatalf("expected nil, got %s", *got)
					}
					continue
				}
				if got == nil {
					t.Fatalf("expected one of %v, got nil", c.want)
				}
				ok := false
				for _, w := range c.want {
					ok = ok || w == *got
				}
				if !ok {
					t.Fatalf("expected one of %v, got %s", c.want, *got)
				}
			}
		})
	}
}

func TestLotteryStrategyDoesNotMutateInputs(t *testing.T) {
	telem := tel(met(tBit, 1), met(tMul, 2))
	jobs := []klyshkov1alpha1.TupleGenerationJob{jobOf(tBit, 5, klyshkov1alpha1.JobRunning)}
	(&LotterySchedulingStrategy{}).Schedule(context.Background(), telem, []klyshkov1alpha1.TupleTypePolicy{pol(tBit, 100, 1)}, jobs)
	if telem.TupleMetrics[0].Available != 1 {
		t.Fatal("telemetry mutated")
	}
}

// Priority ordering: over many draws the frequency is proportional to priority; ties are roughly uniform.
func TestLotteryStrategyWeights(t *testing.T) {
	ctx := context.Background()
	count := map[string]int{}
	telem := tel(met(tBit, 0), met(tMul, 0), met(tInv, 0))
	pols := []klyshkov1alpha1.TupleTypePolicy{pol(tBit, 10, 1), pol(tMul, 10, 8), pol(tInv, 10, 1)}
	const n = 5000
	for i := 0; i < n; i++ {
		count[*(&LotterySchedulingStrategy{}).Schedule(ctx, telem, pols, nil)]++
	}
	if count[tMul] < n*70/100 || count[tMul] > n*90/100 {
		t.Fatalf("priority 8/10 expected ~80%%, got %v", count)
	}
	if count[tBit] == 0 || count[tInv] == 0 {
		t.Fatalf("ties/lower priorities must still be drawn: %v", count)
	}
	eq := map[string]int{}
	pols = []klyshkov1alpha1.TupleTypePolicy{pol(tBit, 10, 1), pol(tMul, 10, 1)}
	for i := 0; i < n; i++ {
		eq[*(&LotterySchedulingStrategy{}).Schedule(ctx, telem, pols, nil)]++
	}
	if eq[tBit] < n*40/100 || eq[tBit] > n*60/100 {
		t.Fatalf("tie should be ~uniform: %v", eq)
	}
}

// Property: the returned type always has a policy, a telemetry entry, and aggregate availability below its threshold.
func TestLotteryStrategyProperty(t *testing.T) {
	rnd := rand.New(rand.NewSource(7))
	types := []string{tBit, tMul, tInv, tMask}
	for i := 0; i < 3000; i++ {
		var telem castor.Telemetry
		avail := map[string]int{}
		for _, ty := range types {
			if rnd.Intn(4) > 0 {
				a := rnd.Intn(100)
				telem.TupleMetrics = append(telem.TupleMetrics, met(ty, a))
				avail[ty] = a
			}
		}
		var pols []klyshkov1alpha1.TupleTypePolicy
		thr := map[string]int{}
		for _, ty := range types {
			if rnd.Intn(4) > 0 {
				p := pol(ty, 1+rnd.Intn(100), 1+rnd.Intn(10))
				pols = append(pols, p)
				thr[ty] = p.Threshold
			}
		}
		var jobs []klyshkov1alpha1.TupleGenerationJob
		for k := rnd.Intn(4); k > 0; k-- {
			ty := types[rnd.Intn(len(types))]
			c := 1 + rnd.Intn(50)
			jobs = append(jobs, jobOf(ty, c, klyshkov1alpha1.JobRunning))
			if _, ok := avail[ty]; ok {
				avail[ty] += c
			}
		}
		got := (&LotterySchedulingStrategy{}).Schedule(context.Background(), telem, pols, jobs)
		if got == nil {
			// then nothing may be eligible
			for ty, th := range thr {
				if a, ok := avail[ty]; ok && a < th {
					t.Fatalf("type %s eligible (%d<%d) but nothing scheduled", ty, a, th)
				}
			}
			continue
		}
		th, hasPolicy := thr[*got]
		a, hasTel := avail[*got]
		if !hasPolicy || !hasTel || a >= th {
			t.Fatalf("scheduled %s: policy=%v telemetry=%v avail=%d threshold=%d", *got, hasPolicy, hasTel, a, th)
		}
	}
}

// ---- Scheduler reconciler -------------------------------------------------------------------------------------

type fakeCastor struct {
	srv       *httptest.Server
	telemetry atomic.Value // castor.Telemetry
	status    atomic.Int32
	calls     atomic.Int32
}

func newFakeCastor(t *testing.T, telem castor.Telemetry) *fakeCastor {
	f := &fakeCastor{}
	f.telemetry.Store(telem)
	f.status.Store(200)
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.calls.Add(1)
		if r.URL.Path != "/intra-vcp/telemetry" {
			http.NotFound(w, r)
			return
		}
		if s := int(f.status.Load()); s != 200 {
			w.WriteHeader(s)
			return
		}
		_ = json.NewEncoder(w).Encode(f.telemetry.Load().(castor.Telemetry))
	}))
	t.Cleanup(f.srv.Close)
	return f
}

func generator(name string, supports ...klyshkov1alpha1.TupleTypeSpec) *klyshkov1alpha1.TupleGenerator {
	return &klyshkov1alpha1.TupleGenerator{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Spec:       klyshkov1alpha1.TupleGeneratorSpec{Supports: supports},
	}
}

func schedulerObj(concurrency int, pols ...klyshkov1alpha1.TupleTypePolicy) *klyshkov1alpha1.TupleGenerationScheduler {
	return &klyshkov1alpha1.TupleGenerationScheduler{
		ObjectMeta: metav1.ObjectMeta{Name: "sched", Namespace: "default", UID: "uid-1"},
		Spec: klyshkov1alpha1.TupleGenerationSchedulerSpec{
			Concurrency: concurrency, TTLSecondsAfterFinished: 600, TupleTypePolicies: pols,
		},
	}
}

func reconcileScheduler(t *testing.T, c *fakeCastor, objs ...client.Object) (*TupleGenerationSchedulerReconciler, []klyshkov1alpha1.TupleGenerationJob, error) {
	t.Helper()
	cl := newFakeClient(t, objs...)
	r := &TupleGenerationSchedulerReconciler{Client: cl, Scheme: testScheme(t), CastorClient: castor.NewClient(c.srv.URL)}
	_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "sched"}})
	return r, listJobs(t, r), err
}

func listJobs(t *testing.T, r *TupleGenerationSchedulerReconciler) []klyshkov1alpha1.TupleGenerationJob {
	l := &klyshkov1alpha1.TupleGenerationJobList{}
	if err := r.List(context.Background(), l); err != nil {
		t.Fatal(err)
	}
	return l.Items
}

func TestSchedulerReconcile(t *testing.T) {
	gen := generator("g", klyshkov1alpha1.TupleTypeSpec{Type: tBit, BatchSize: 1234}, klyshkov1alpha1.TupleTypeSpec{Type: tMul, BatchSize: 77})

	t.Run("creates job with generator batch size", func(t *testing.T) {
		f := newFakeCastor(t, tel(met(tBit, 0)))
		_, jobs, err := reconcileScheduler(t, f, schedulerObj(1, pol(tBit, 10, 1)), gen)
		if err != nil || len(jobs) != 1 {
			t.Fatalf("jobs=%d err=%v", len(jobs), err)
		}
		j := jobs[0]
		if j.Spec.Type != tBit || j.Spec.Count != 1234 || j.Spec.Generator != "g" || j.Spec.ID == "" || j.Status.State != klyshkov1alpha1.JobPending {
			t.Fatalf("unexpected job %+v", j)
		}
		if len(j.OwnerReferences) != 1 || j.OwnerReferences[0].Name != "sched" {
			t.Fatalf("owner reference missing")
		}
	})
	t.Run("generator filtering: only generator-supported type", func(t *testing.T) {
		f := newFakeCastor(t, tel(met(tInv, 0), met(tMul, 0)))
		for i := 0; i < 10; i++ {
			_, jobs, err := reconcileScheduler(t, f, schedulerObj(1, pol(tInv, 10, 100), pol(tMul, 10, 1)), gen)
			if err != nil || len(jobs) != 1 || jobs[0].Spec.Type != tMul || jobs[0].Spec.Count != 77 {
				t.Fatalf("jobs=%v err=%v", jobs, err)
			}
		}
	})
	t.Run("no generator for any type", func(t *testing.T) {
		f := newFakeCastor(t, tel(met(tInv, 0)))
		_, jobs, err := reconcileScheduler(t, f, schedulerObj(1, pol(tInv, 10, 1)), gen)
		if err != nil || len(jobs) != 0 {
			t.Fatalf("jobs=%v err=%v", jobs, err)
		}
	})
	t.Run("ambiguous generators are not scheduled", func(t *testing.T) {
		f := newFakeCastor(t, tel(met(tBit, 0)))
		_, jobs, err := reconcileScheduler(t, f, schedulerObj(1, pol(tBit, 10, 1)), gen, generator("g2", klyshkov1alpha1.TupleTypeSpec{Type: tBit, BatchSize: 1}))
		if err != nil || len(jobs) != 0 {
			t.Fatalf("jobs=%v err=%v", jobs, err)
		}
	})
	t.Run("castor telemetry failing", func(t *testing.T) {
		f := newFakeCastor(t, tel(met(tBit, 0)))
		f.status.Store(500)
		_, jobs, err := reconcileScheduler(t, f, schedulerObj(1, pol(tBit, 10, 1)), gen)
		if err == nil || len(jobs) != 0 {
			t.Fatalf("expected error and no job, got jobs=%v err=%v", jobs, err)
		}
	})
	t.Run("castor unreachable", func(t *testing.T) {
		f := newFakeCastor(t, tel())
		f.srv.Close()
		_, jobs, err := reconcileScheduler(t, f, schedulerObj(1, pol(tBit, 10, 1)), gen)
		if err == nil || len(jobs) != 0 {
			t.Fatalf("expected error, got jobs=%v err=%v", jobs, err)
		}
	})
	t.Run("castor telemetry empty", func(t *testing.T) {
		f := newFakeCastor(t, tel())
		_, jobs, err := reconcileScheduler(t, f, schedulerObj(1, pol(tBit, 10, 1)), gen)
		if err != nil || len(jobs) != 0 {
			t.Fatalf("jobs=%v err=%v", jobs, err)
		}
	})
	t.Run("above threshold", func(t *testing.T) {
		f := newFakeCastor(t, tel(met(tBit, 10)))
		_, jobs, err := reconcileScheduler(t, f, schedulerObj(1, pol(tBit, 10, 1)), gen)
		if err != nil || len(jobs) != 0 {
			t.Fatalf("jobs=%v err=%v", jobs, err)
		}
	})
	t.Run("concurrency zero never schedules and does not call castor", func(t *testing.T) {
		f := newFakeCastor(t, tel(met(tBit, 0)))
		_, jobs, err := reconcileScheduler(t, f, schedulerObj(0, pol(tBit, 10, 1)), gen)
		if err != nil || len(jobs) != 0 || f.calls.Load() != 0 {
			t.Fatalf("jobs=%v err=%v calls=%d", jobs, err, f.calls.Load())
		}
	})
	t.Run("missing scheduler is a no-op", func(t *testing.T) {
		f := newFakeCastor(t, tel(met(tBit, 0)))
		cl := newFakeClient(t, gen)
		r := &TupleGenerationSchedulerReconciler{Client: cl, Scheme: testScheme(t), CastorClient: castor.NewClient(f.srv.URL)}
		if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "nope"}}); err != nil {
			t.Fatal(err)
		}
	})
}

// Property: repeated reconciliation never exceeds the concurrency limit and never schedules a type whose available
// plus in-flight tuples reach the threshold.
func TestSchedulerReconcileProperty(t *testing.T) {
	gen := generator("g", klyshkov1alpha1.TupleTypeSpec{Type: tBit, BatchSize: 10}, klyshkov1alpha1.TupleTypeSpec{Type: tMul, BatchSize: 10})
	for conc := 0; conc <= 4; conc++ {
		f := newFakeCastor(t, tel(met(tBit, 0), met(tMul, 0)))
		s := schedulerObj(conc, pol(tBit, 35, 1), pol(tMul, 15, 3))
		r, _, err := reconcileScheduler(t, f, s, gen)
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 20; i++ {
			if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "sched"}}); err != nil {
				t.Fatal(err)
			}
			jobs := listJobs(t, r)
			if len(jobs) > conc {
				t.Fatalf("concurrency %d exceeded: %d jobs", conc, len(jobs))
			}
			per := map[string]int{}
			for _, j := range jobs {
				per[j.Spec.Type] += j.Spec.Count
			}
			if per[tBit] > 35+9 || per[tMul] > 15+9 { // last job may straddle the threshold, but never start above it
				t.Fatalf("threshold overshoot: %v", per)
			}
		}
		jobs := listJobs(t, r)
		per := map[string]int{}
		for _, j := range jobs {
			per[j.Spec.Type]++
		}
		if per[tMul] > 2 || per[tBit] > 4 { // thresholds 15/35 with batch 10 -> at most 2 / 4 jobs
			t.Fatalf("conc=%d: too many jobs per type %v", conc, per)
		}
	}
}
