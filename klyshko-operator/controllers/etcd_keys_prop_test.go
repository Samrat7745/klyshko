/*
Copyright (c) 2026 - for information on the respective copyright owner
see the NOTICE file and/or the repository https://github.com/carbynestack/klyshko.

SPDX-License-Identifier: Apache-2.0
*/

package controllers

import (
	"fmt"
	"math"
	"math/rand"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/types"
)

func rk(ns, name string) RosterKey {
	return RosterKey{NamespacedName: types.NamespacedName{Namespace: ns, Name: name}}
}

func TestKeyRoundTripTable(t *testing.T) {
	long := strings.Repeat("a", 253)
	cases := []struct{ ns, name string }{
		{"default", "job"},
		{"ns-with-dash", "scheduler-3f2b1c4e-aaaa-bbbb-cccc-000000000001"},
		{"under_score", "NAME_UP"},
		{"1", "2"},
		{long, long},
		{"a", "1"}, // numeric job name must not be confused with a player id
	}
	for _, c := range cases {
		c := c
		t.Run(fmt.Sprintf("%.10s/%.10s", c.ns, c.name), func(t *testing.T) {
			key := rk(c.ns, c.name)
			got, err := ParseKey(key.ToEtcdKey())
			if err != nil || got != Key(key) {
				t.Fatalf("roster key round trip failed: %v %v", got, err)
			}
			for _, pid := range []uint{0, 1, 42, math.MaxUint32} {
				ek := RosterEntryKey{RosterKey: key, PlayerID: pid}
				got, err := ParseKey(ek.ToEtcdKey())
				if err != nil || got != Key(ek) {
					t.Fatalf("entry key round trip failed for pid %d: %v %v", pid, got, err)
				}
				if ek.String() != ek.ToEtcdKey() {
					t.Fatalf("String must equal ToEtcdKey")
				}
			}
		})
	}
}

func TestParseKeyRejectsInvalid(t *testing.T) {
	invalid := map[string]string{
		"empty":               "",
		"prefix only":         "/klyshko/roster",
		"prefix slash":        "/klyshko/roster/",
		"no job":              "/klyshko/roster/ns",
		"no job slash":        "/klyshko/roster/ns/",
		"empty namespace":     "/klyshko/roster//job",
		"wrong prefix":        "/klyshko/heads/ns/job",
		"no leading slash":    "klyshko/roster/ns/job",
		"trailing slash":      "/klyshko/roster/ns/job/",
		"trailing slash pid":  "/klyshko/roster/ns/job/1/",
		"extra component":     "/klyshko/roster/ns/job/1/2",
		"non numeric pid":     "/klyshko/roster/ns/job/x",
		"negative pid":        "/klyshko/roster/ns/job/-1",
		"pid overflow":        "/klyshko/roster/ns/job/4294967296",
		"pid huge":            "/klyshko/roster/ns/job/99999999999999999999999999",
		"unicode namespace":   "/klyshko/roster/nä/job",
		"unicode name":        "/klyshko/roster/ns/jöb",
		"dot in name":         "/klyshko/roster/ns/job.name",
		"space":               "/klyshko/roster/ns/job name",
		"newline":             "/klyshko/roster/ns/job\n",
		"nul":                 "/klyshko/roster/ns/job\x00",
		"slash in namespace":  "/klyshko/roster/a/b/c/d",
		"unicode digits pid":  "/klyshko/roster/ns/job/١٢",
		"prefix of other key": "/klyshko/rosterx/ns/job",
	}
	for name, k := range invalid {
		k := k
		t.Run(name, func(t *testing.T) {
			got, err := ParseKey(k)
			if err == nil {
				t.Fatalf("expected error for %q, got %v", k, got)
			}
			if got != nil {
				t.Fatalf("expected nil key on error, got %v", got)
			}
		})
	}
}

func TestParseKeyPlayerIDBounds(t *testing.T) {
	for _, c := range []struct {
		in string
		ok bool
		id uint
	}{
		{"0", true, 0}, {"1", true, 1}, {"007", true, 7},
		{"4294967295", true, math.MaxUint32}, {"4294967296", false, 0},
		{"9223372036854775807", false, 0}, {"9223372036854775808", false, 0},
	} {
		got, err := ParseKey("/klyshko/roster/ns/job/" + c.in)
		if c.ok {
			e, isEntry := got.(RosterEntryKey)
			if err != nil || !isEntry || e.PlayerID != c.id {
				t.Errorf("%s: got %v, %v", c.in, got, err)
			}
		} else if err == nil {
			t.Errorf("%s: expected error, got %v", c.in, got)
		}
	}
}

// Slashes in names cannot be represented unambiguously; the parser must never return the original key for them.
func TestParseKeyOddNamesNeverAliasRoundTrip(t *testing.T) {
	for _, k := range []RosterKey{rk("a/b", "c"), rk("a", "b/c"), rk("ns", "ü"), rk("ns", ""), rk("", "job")} {
		got, err := ParseKey(k.ToEtcdKey())
		if err == nil && got == Key(k) {
			t.Errorf("odd key %q unexpectedly round-trips", k.ToEtcdKey())
		}
	}
}

// Property: random keys composed of valid characters always round trip.
func TestKeyRoundTripProperty(t *testing.T) {
	rnd := rand.New(rand.NewSource(1))
	const alphabet = "abcXYZ019_-"
	gen := func() string {
		n := 1 + rnd.Intn(60)
		b := make([]byte, n)
		for i := range b {
			b[i] = alphabet[rnd.Intn(len(alphabet))]
		}
		return string(b)
	}
	for i := 0; i < 2000; i++ {
		key := rk(gen(), gen())
		if got, err := ParseKey(key.ToEtcdKey()); err != nil || got != Key(key) {
			t.Fatalf("roster %q: %v %v", key, got, err)
		}
		ek := RosterEntryKey{RosterKey: key, PlayerID: uint(rnd.Uint32())}
		if got, err := ParseKey(ek.ToEtcdKey()); err != nil || got != Key(ek) {
			t.Fatalf("entry %q: %v %v", ek, got, err)
		}
	}
}

func FuzzParseKey(f *testing.F) {
	for _, s := range []string{
		"/klyshko/roster/ns/job", "/klyshko/roster/ns/job/0", "/klyshko/roster/ns/job/4294967295",
		"/klyshko/roster/ns/job/4294967296", "", "/klyshko/roster//", "/klyshko/roster/ä/ö",
	} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		k, err := ParseKey(s)
		if err != nil {
			if k != nil {
				t.Fatalf("non-nil key with error")
			}
			return
		}
		again, err := ParseKey(k.ToEtcdKey())
		if err != nil || again != k {
			t.Fatalf("round trip violated for %q: %v -> %v (%v)", s, k, again, err)
		}
	})
}
