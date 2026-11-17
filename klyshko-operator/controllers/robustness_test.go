/*
Copyright (c) 2026 - for information on the respective copyright owner
see the NOTICE file and/or the repository https://github.com/carbynestack/klyshko.

SPDX-License-Identifier: Apache-2.0
*/

package controllers

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	klyshkov1alpha1 "github.com/carbynestack/klyshko/api/v1alpha1"
	v1 "k8s.io/api/core/v1"
)

func FuzzUnmarshalTaskStatus(f *testing.F) {
	for _, s := range []string{
		`{"state":"Completed","endpoint":"h:1"}`, `{}`, `null`, `[]`, `"x"`, `{"state":1}`, `{"state":"Bogus"}`,
		`{"state":"Failed","endpoint":null}`, "\xff\xfe", ``, `{"state":"Completed"}{"x":1}`,
	} {
		f.Add([]byte(s))
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		st, err := klyshkov1alpha1.Unmarshal(data)
		if st == nil {
			t.Fatal("Unmarshal must never return nil status")
		}
		if err != nil {
			return
		}
		enc, err := json.Marshal(st)
		if err != nil {
			t.Fatal(err)
		}
		again, err := klyshkov1alpha1.Unmarshal(enc)
		if err != nil || !reflect.DeepEqual(again, st) {
			t.Fatalf("re-encoding changes status: %v vs %v (%v)", st, again, err)
		}
		_ = st.State.IsValid()
	})
}

func TestUnmarshalTaskStatusErrors(t *testing.T) {
	for _, bad := range []string{"", "{", "garbage", `{"state":}`, `[1]`, `{"state":5}`, `"str"`, `{"state":"a"}x`} {
		if _, err := klyshkov1alpha1.Unmarshal([]byte(bad)); err == nil {
			t.Errorf("expected error for %q", bad)
		}
	}
}

// Status stored in etcd that is garbage / invalid must yield a defined error from getStatus.
func TestGetStatusFromEtcdGarbage(t *testing.T) {
	etcd := newTestEtcd(t)
	c := etcd.newClient()
	r := &TupleGenerationTaskReconciler{EtcdClient: c}
	key := RosterEntryKey{RosterKey: rk("default", "job"), PlayerID: 0}
	ctx := context.Background()

	if _, err := r.getStatus(ctx, key); err == nil {
		t.Error("missing entry must error")
	}
	for _, bad := range []string{"", "{", "\x00\xff", `{"state":""}`, `{"state":"Bogus"}`, `null`, `[]`, strings.Repeat("{", 10000)} {
		put(t, c, key.ToEtcdKey(), bad)
		if st, err := r.getStatus(ctx, key); err == nil {
			t.Errorf("expected error for %q, got %v", bad, st)
		}
	}
	put(t, c, key.ToEtcdKey(), `{"state":"Generating","endpoint":"1.2.3.4:5"}`)
	st, err := r.getStatus(ctx, key)
	if err != nil || st.State != klyshkov1alpha1.TaskGenerating || st.Endpoint != "1.2.3.4:5" {
		t.Fatalf("valid status: %v %v", st, err)
	}
}

func TestEndpointFromService(t *testing.T) {
	lb := func(ing ...v1.LoadBalancerIngress) *v1.Service {
		return &v1.Service{Spec: v1.ServiceSpec{Type: v1.ServiceTypeLoadBalancer}, Status: v1.ServiceStatus{LoadBalancer: v1.LoadBalancerStatus{Ingress: ing}}}
	}
	if _, err := endpoint(&v1.Service{Spec: v1.ServiceSpec{Type: v1.ServiceTypeClusterIP}}); err == nil {
		t.Error("non-LB must error")
	}
	if e, err := endpoint(lb()); e != nil || err != nil {
		t.Errorf("no ingress: %v %v", e, err)
	}
	if e, err := endpoint(lb(v1.LoadBalancerIngress{})); e != nil || err == nil {
		t.Errorf("empty ingress must error: %v %v", e, err)
	}
	if e, _ := endpoint(lb(v1.LoadBalancerIngress{IP: "1.2.3.4", Hostname: "h"})); *e != "1.2.3.4" {
		t.Error("IP preferred")
	}
	if e, _ := endpoint(lb(v1.LoadBalancerIngress{Hostname: "h"})); *e != "h" {
		t.Error("hostname fallback")
	}
}

func TestEndpointsSkipsEmpty(t *testing.T) {
	r := &TupleGenerationTaskReconciler{}
	got := r.endpoints(map[uint]*klyshkov1alpha1.TupleGenerationTask{
		0: {Status: klyshkov1alpha1.TupleGenerationTaskStatus{Endpoint: "a:1"}},
		1: {},
	})
	if len(got) != 1 || got[0] != "a:1" {
		t.Fatalf("got %v", got)
	}
}

func FuzzTaskNames(f *testing.F) {
	for _, s := range []string{"job-1", "nodash", "", "-", "a-b-c-9", "é-é"} {
		f.Add(s, uint32(3))
	}
	f.Fuzz(func(t *testing.T, name string, pid uint32) {
		n := taskName(name, uint(pid))
		if got := jobNameFromTaskName(n); got != name {
			t.Fatalf("jobNameFromTaskName(taskName(%q)) = %q", name, got)
		}
	})
}
