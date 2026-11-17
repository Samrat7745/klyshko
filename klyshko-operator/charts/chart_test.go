/*
Copyright (c) 2026 - for information on the respective copyright owner
see the NOTICE file and/or the repository https://github.com/carbynestack/klyshko.

SPDX-License-Identifier: Apache-2.0
*/

// Package charts contains render tests for the Klyshko operator Helm chart. They are skipped if helm is unavailable.
package charts

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"sigs.k8s.io/yaml"
)

const chartDir = "klyshko"

func helmTemplate(t *testing.T, args ...string) (string, error) {
	t.Helper()
	helm, err := exec.LookPath("helm")
	if err != nil {
		t.Skip("helm not installed")
	}
	cmd := exec.Command(helm, append([]string{"template", "klyshko", chartDir, "--namespace", "klyshko-ns"}, args...)...)
	var out, errb bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errb
	err = cmd.Run()
	if err != nil {
		return "", &helmError{err, errb.String()}
	}
	return out.String(), nil
}

type helmError struct {
	err    error
	stderr string
}

func (e *helmError) Error() string { return e.err.Error() + ": " + e.stderr }

type rendered struct {
	raw         string
	deployment  appsv1.Deployment
	clusterRole rbacv1.ClusterRole
	docs        []map[string]interface{}
}

func render(t *testing.T, args ...string) rendered {
	t.Helper()
	out, err := helmTemplate(t, args...)
	if err != nil {
		t.Fatalf("helm template failed: %v", err)
	}
	r := rendered{raw: out}
	var foundDep, foundRole bool
	for _, doc := range strings.Split(out, "\n---\n") {
		if strings.TrimSpace(doc) == "" {
			continue
		}
		var generic map[string]interface{}
		if err := yaml.Unmarshal([]byte(doc), &generic); err != nil {
			t.Fatalf("rendered document is not valid YAML: %v\n%s", err, doc)
		}
		if generic == nil {
			continue
		}
		r.docs = append(r.docs, generic)
		switch generic["kind"] {
		case "Deployment":
			if err := yaml.UnmarshalStrict([]byte(doc), &r.deployment); err != nil {
				t.Fatalf("deployment: %v", err)
			}
			foundDep = true
		case "ClusterRole":
			if generic["metadata"].(map[string]interface{})["name"] == "klyshko-manager-role" {
				if err := yaml.UnmarshalStrict([]byte(doc), &r.clusterRole); err != nil {
					t.Fatalf("clusterrole: %v", err)
				}
				foundRole = true
			}
		}
	}
	if !foundDep || !foundRole {
		t.Fatalf("deployment (%v) or manager cluster role (%v) missing", foundDep, foundRole)
	}
	return r
}

func (r rendered) container(t *testing.T, name string) (args []string, image string, _ struct{}) {
	t.Helper()
	for _, c := range r.deployment.Spec.Template.Spec.Containers {
		if c.Name == name {
			return c.Args, c.Image, struct{}{}
		}
	}
	t.Fatalf("container %s not found", name)
	return
}

func hasArg(args []string, a string) bool {
	for _, x := range args {
		if x == a {
			return true
		}
	}
	return false
}

func argValue(args []string, name string) (string, bool) {
	for _, x := range args {
		if strings.HasPrefix(x, name+"=") {
			return strings.TrimPrefix(x, name+"="), true
		}
	}
	return "", false
}

func TestDefaultRender(t *testing.T) {
	r := render(t)
	args, image, _ := r.container(t, "manager")
	if v, ok := argValue(args, "--etcd-endpoint"); !ok || v != "172.18.1.129:2379" {
		t.Errorf("etcd endpoint: %q %v", v, ok)
	}
	if v, ok := argValue(args, "--provisioner-image"); !ok || v != "ghcr.io/carbynestack/klyshko-provisioner:latest" {
		t.Errorf("provisioner image: %q", v)
	}
	if !hasArg(args, "--leader-elect") {
		t.Error("--leader-elect missing")
	}
	if hasArg(args, "--sgx-enabled") {
		t.Error("--sgx-enabled must be off by default")
	}
	if image != "ghcr.io/carbynestack/klyshko-operator-controller:latest" {
		t.Errorf("image %q", image)
	}
	if r.deployment.Namespace != "klyshko-ns" {
		t.Errorf("namespace %q", r.deployment.Namespace)
	}
	if len(r.deployment.Spec.Template.Spec.ImagePullSecrets) != 0 {
		t.Error("no pull secrets by default")
	}
}

func TestOverrides(t *testing.T) {
	r := render(t,
		"--set", "controller.etcdEndpoint=etcd.example:2379",
		"--set", "controller.sgx.enabled=true",
		"--set", "controller.image.registry=registry.example",
		"--set", "controller.image.repository=team/ctl",
		"--set", "controller.image.tag=1.2.3",
		"--set", "controller.image.pullPolicy=Always",
		"--set", "provisioner.image.registry=prov.example",
		"--set", "provisioner.image.repository=p/prov",
		"--set", "provisioner.image.tag=9.9",
		"--set", "imagePullSecrets[0].name=regcred",
	)
	args, image, _ := r.container(t, "manager")
	if v, _ := argValue(args, "--etcd-endpoint"); v != "etcd.example:2379" {
		t.Errorf("etcd endpoint %q", v)
	}
	if v, _ := argValue(args, "--provisioner-image"); v != "prov.example/p/prov:9.9" {
		t.Errorf("provisioner image %q", v)
	}
	if !hasArg(args, "--sgx-enabled") || !hasArg(args, "--leader-elect") {
		t.Errorf("args: %v", args)
	}
	if image != "registry.example/team/ctl:1.2.3" {
		t.Errorf("image %q", image)
	}
	for _, c := range r.deployment.Spec.Template.Spec.Containers {
		if c.Name == "manager" && string(c.ImagePullPolicy) != "Always" {
			t.Errorf("pull policy %q", c.ImagePullPolicy)
		}
	}
	if s := r.deployment.Spec.Template.Spec.ImagePullSecrets; len(s) != 1 || s[0].Name != "regcred" {
		t.Errorf("pull secrets %v", s)
	}
}

// Every flag the chart passes must be defined by the manager binary.
func TestManagerFlagsExistInMain(t *testing.T) {
	src, err := os.ReadFile(filepath.Join("..", "main.go"))
	if err != nil {
		t.Fatal(err)
	}
	r := render(t, "--set", "controller.sgx.enabled=true")
	args, _, _ := r.container(t, "manager")
	for _, a := range args {
		name := strings.SplitN(strings.TrimPrefix(a, "--"), "=", 2)[0]
		if name == "zap-log-level" { // registered by the zap options, not by main.go itself
			continue
		}
		if !regexp.MustCompile(`"` + regexp.QuoteMeta(name) + `"`).Match(src) {
			t.Errorf("flag --%s is not defined in main.go", name)
		}
	}
}

func TestSecurityContext(t *testing.T) {
	r := render(t)
	// Original (upstream) chart values: runAsNonRoot on the pod, allowPrivilegeEscalation=false on the manager.
	podSC := r.deployment.Spec.Template.Spec.SecurityContext
	if podSC == nil || podSC.RunAsNonRoot == nil || !*podSC.RunAsNonRoot {
		t.Errorf("pod security context: %+v", podSC)
	}
	if len(r.deployment.Spec.Template.Spec.Containers) != 2 {
		t.Fatalf("expected two containers")
	}
	for _, c := range r.deployment.Spec.Template.Spec.Containers {
		sc := c.SecurityContext
		if c.Name == "manager" && (sc == nil || sc.AllowPrivilegeEscalation == nil || *sc.AllowPrivilegeEscalation) {
			t.Errorf("container %s security context: %+v", c.Name, sc)
		}
		if sc != nil && sc.Privileged != nil && *sc.Privileged {
			t.Errorf("container %s privileged", c.Name)
		}
	}
	if r.deployment.Spec.Template.Spec.ServiceAccountName != "klyshko-controller-manager" {
		t.Error("service account")
	}
}

func TestRBACCoversCRDs(t *testing.T) {
	r := render(t)
	type grant struct{ group, resource string }
	verbsBy := map[grant]map[string]bool{}
	for _, rule := range r.clusterRole.Rules {
		for _, g := range rule.APIGroups {
			for _, res := range rule.Resources {
				if verbsBy[grant{g, res}] == nil {
					verbsBy[grant{g, res}] = map[string]bool{}
				}
				for _, v := range rule.Verbs {
					verbsBy[grant{g, res}][v] = true
				}
			}
		}
	}
	const group = "klyshko.carbnyestack.io"
	for _, res := range []string{"tuplegenerationjobs", "tuplegenerationtasks", "tuplegenerationschedulers", "tuplegenerators"} {
		for _, v := range []string{"get", "list", "watch"} {
			if !verbsBy[grant{group, res}][v] {
				t.Errorf("missing %s on %s", v, res)
			}
		}
	}
	for _, res := range []string{"tuplegenerationjobs", "tuplegenerationtasks", "tuplegenerationschedulers"} {
		for _, v := range []string{"create", "update", "delete"} {
			if !verbsBy[grant{group, res}][v] {
				t.Errorf("missing %s on %s", v, res)
			}
		}
		if !verbsBy[grant{group, res + "/status"}]["update"] {
			t.Errorf("missing status update on %s", res)
		}
	}
	for _, res := range []string{"configmaps"} {
		if !verbsBy[grant{"", res}]["get"] {
			t.Errorf("missing get on %s", res)
		}
	}
	for _, res := range []string{"pods", "services", "persistentvolumeclaims"} {
		for _, v := range []string{"create", "delete", "get", "list", "watch"} {
			if !verbsBy[grant{"", res}][v] {
				t.Errorf("missing %s on %s", v, res)
			}
		}
	}
	// No wildcards, least privilege
	for _, rule := range r.clusterRole.Rules {
		for _, s := range append(append(append([]string{}, rule.APIGroups...), rule.Resources...), rule.Verbs...) {
			if s == "*" {
				t.Errorf("wildcard in rule %+v", rule)
			}
		}
	}
	// Every CRD shipped by the chart is covered
	for _, d := range r.docs {
		if d["kind"] != "CustomResourceDefinition" {
			continue
		}
		spec := d["spec"].(map[string]interface{})
		plural := spec["names"].(map[string]interface{})["plural"].(string)
		if verbsBy[grant{spec["group"].(string), plural}] == nil {
			t.Errorf("CRD %s not covered by manager role", plural)
		}
	}
}

// The original chart declares no kubeVersion constraint, so every Kubernetes version is accepted.
func TestNoKubeVersionConstraint(t *testing.T) {
	if _, err := helmTemplate(t, "--kube-version", "1.25.0"); err != nil {
		t.Errorf("1.25.0 must be accepted: %v", err)
	}
	if _, err := helmTemplate(t, "--kube-version", "1.24.9"); err != nil {
		t.Errorf("1.24.9 must be accepted (no kubeVersion constraint): %v", err)
	}
	if _, err := helmTemplate(t, "--kube-version", "1.31.2-gke.100"); err != nil {
		t.Errorf("provider suffixed versions must be accepted: %v", err)
	}
}

func TestKubeconform(t *testing.T) {
	kc, err := exec.LookPath("kubeconform")
	if err != nil {
		t.Skip("kubeconform not installed")
	}
	out, err := helmTemplate(t)
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(kc, "-strict", "-summary", "-ignore-missing-schemas", "-kubernetes-version", "1.30.0", "-")
	cmd.Stdin = strings.NewReader(out)
	if b, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("kubeconform: %v\n%s", err, b)
	}
}
