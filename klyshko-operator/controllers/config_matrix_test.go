/*
Copyright (c) 2026 - for information on the respective copyright owner
see the NOTICE file and/or the repository https://github.com/carbynestack/klyshko.

SPDX-License-Identifier: Apache-2.0
*/

package controllers

import (
	"context"
	"fmt"
	"math"
	"testing"

	klyshkov1alpha1 "github.com/carbynestack/klyshko/api/v1alpha1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func testScheme(t testing.TB) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	if err := klyshkov1alpha1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	return s
}

func newFakeClient(t testing.TB, objs ...client.Object) client.Client {
	t.Helper()
	return fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(objs...).
		WithStatusSubresource(&klyshkov1alpha1.TupleGenerationJob{}, &klyshkov1alpha1.TupleGenerationTask{}).Build()
}

func vcpConfigMap(ns string, data map[string]string) *v1.ConfigMap {
	return &v1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: vcpConfigMapName, Namespace: ns}, Data: data}
}

func TestParseVCPConfigMatrix(t *testing.T) {
	big := fmt.Sprint(uint64(math.MaxUint32))
	over := fmt.Sprint(uint64(math.MaxUint32) + 1)
	cases := []struct {
		name         string
		data         map[string]string // nil: no ConfigMap at all
		wantID, want uint
		wantErr      bool
	}{
		{name: "missing configmap", data: nil, wantErr: true},
		{name: "empty data", data: map[string]string{}, wantErr: true},
		{name: "missing playerCount", data: map[string]string{"playerId": "0"}, wantErr: true},
		{name: "missing playerId", data: map[string]string{"playerCount": "2"}, wantErr: true},
		{name: "non numeric count", data: map[string]string{"playerCount": "two", "playerId": "0"}, wantErr: true},
		{name: "non numeric id", data: map[string]string{"playerCount": "2", "playerId": "x"}, wantErr: true},
		{name: "empty strings", data: map[string]string{"playerCount": "", "playerId": ""}, wantErr: true},
		{name: "whitespace", data: map[string]string{"playerCount": " 2", "playerId": "0"}, wantErr: true},
		{name: "float", data: map[string]string{"playerCount": "2.0", "playerId": "0"}, wantErr: true},
		{name: "negative count", data: map[string]string{"playerCount": "-1", "playerId": "0"}, wantErr: true},
		{name: "negative id", data: map[string]string{"playerCount": "2", "playerId": "-1"}, wantErr: true},
		{name: "id equals count", data: map[string]string{"playerCount": "2", "playerId": "2"}, wantErr: true},
		{name: "id above count", data: map[string]string{"playerCount": "2", "playerId": "7"}, wantErr: true},
		{name: "count zero", data: map[string]string{"playerCount": "0", "playerId": "0"}, wantErr: true},
		{name: "count one id zero", data: map[string]string{"playerCount": "1", "playerId": "0"}, wantID: 0, want: 1},
		{name: "count one id one", data: map[string]string{"playerCount": "1", "playerId": "1"}, wantErr: true},
		{name: "two players id one", data: map[string]string{"playerCount": "2", "playerId": "1"}, wantID: 1, want: 2},
		{name: "huge count", data: map[string]string{"playerCount": big, "playerId": big}, wantErr: true},
		{name: "huge count last id", data: map[string]string{"playerCount": big, "playerId": fmt.Sprint(uint64(math.MaxUint32) - 1)}, wantID: math.MaxUint32 - 1, want: math.MaxUint32},
		{name: "count overflow uint32", data: map[string]string{"playerCount": over, "playerId": "0"}, wantErr: true},
		{name: "count overflow int64", data: map[string]string{"playerCount": "99999999999999999999", "playerId": "0"}, wantErr: true},
		{name: "extra keys ignored", data: map[string]string{"playerCount": "3", "playerId": "2", "other": "x"}, wantID: 2, want: 3},
	}
	ctx := context.Background()
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			var cl client.Client
			if c.data == nil {
				cl = newFakeClient(t)
			} else {
				cl = newFakeClient(t, vcpConfigMap("ns", c.data))
			}
			id, n, err := parseVCPConfig(ctx, &cl, "ns")
			lid, lerr := localPlayerID(ctx, &cl, "ns")
			cnt, cerr := numberOfVCPs(ctx, &cl, "ns")
			if c.wantErr {
				if err == nil || lerr == nil || cerr == nil {
					t.Fatalf("expected errors, got %v %v %v", err, lerr, cerr)
				}
				return
			}
			if err != nil || lerr != nil || cerr != nil {
				t.Fatalf("unexpected errors %v %v %v", err, lerr, cerr)
			}
			if id != c.wantID || n != c.want || lid != c.wantID || cnt != c.want {
				t.Fatalf("got id=%d n=%d lid=%d cnt=%d", id, n, lid, cnt)
			}
		})
	}
}

func TestVCPConfigIsNamespaced(t *testing.T) {
	cl := newFakeClient(t, vcpConfigMap("other", map[string]string{"playerCount": "2", "playerId": "1"}))
	if _, err := localPlayerID(context.Background(), &cl, "ns"); err == nil {
		t.Fatal("config of other namespace must not be used")
	}
}
