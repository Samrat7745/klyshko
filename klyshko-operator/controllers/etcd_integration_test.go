/*
Copyright (c) 2026 - for information on the respective copyright owner
see the NOTICE file and/or the repository https://github.com/carbynestack/klyshko.

SPDX-License-Identifier: Apache-2.0
*/

package controllers

import (
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/google/uuid"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	klyshkov1alpha1 "github.com/carbynestack/klyshko/api/v1alpha1"
	"github.com/carbynestack/klyshko/castor"
)

// testEtcd is an embedded etcd server that can be stopped and restarted on the same address and data directory.
type testEtcd struct {
	t      *testing.T
	dir    string
	client *url.URL
	peer   *url.URL
	server *embed.Etcd
}

func freeURL(t *testing.T) *url.URL {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	u, _ := url.Parse("http://" + l.Addr().String())
	return u
}

func newTestEtcd(t *testing.T) *testEtcd {
	e := &testEtcd{t: t, dir: t.TempDir()}
	// The free ports found may be taken by someone else before etcd binds them; retry with new ones.
	var err error
	for attempt := 0; attempt < 5; attempt++ {
		e.client, e.peer = freeURL(t), freeURL(t)
		if err = e.tryStart(); err == nil {
			t.Cleanup(e.stop)
			return e
		}
	}
	t.Fatalf("starting embedded etcd: %v", err)
	return nil
}

func (e *testEtcd) start() {
	if err := e.tryStart(); err != nil {
		e.t.Fatalf("starting embedded etcd: %v", err)
	}
}

func (e *testEtcd) tryStart() error {
	cfg := embed.NewConfig()
	cfg.Dir = e.dir
	cfg.LogLevel = "error"
	cfg.ListenClientUrls = []url.URL{*e.client}
	cfg.AdvertiseClientUrls = []url.URL{*e.client}
	cfg.ListenPeerUrls = []url.URL{*e.peer}
	cfg.AdvertisePeerUrls = []url.URL{*e.peer}
	cfg.InitialCluster = cfg.InitialClusterFromName(cfg.Name)
	cfg.Metrics = "basic"
	srv, err := embed.StartEtcd(cfg)
	if err != nil {
		return err
	}
	select {
	case <-srv.Server.ReadyNotify():
	case <-time.After(20 * time.Second):
		srv.Close()
		return fmt.Errorf("embedded etcd not ready")
	}
	e.server = srv
	return nil
}

func (e *testEtcd) stop() {
	if e.server != nil {
		e.server.Close()
		e.server = nil
	}
}

func (e *testEtcd) newClient() *clientv3.Client {
	c, err := clientv3.New(clientv3.Config{Endpoints: []string{e.client.Host}, DialTimeout: 2 * time.Second})
	if err != nil {
		e.t.Fatal(err)
	}
	e.t.Cleanup(func() { _ = c.Close() })
	return c
}

func shortRetries(t *testing.T) {
	oldHead, oldWatch := headRevisionOpRetryPeriod, watchRetryPeriod
	headRevisionOpRetryPeriod, watchRetryPeriod = 50*time.Millisecond, 20*time.Millisecond
	t.Cleanup(func() { headRevisionOpRetryPeriod, watchRetryPeriod = oldHead, oldWatch })
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func never(t *testing.T, what string, d time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			t.Fatalf("unexpected: %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// vcpEnv bundles a reconciler for the local VCP (player 1 of 2), a fake Kubernetes API, and a fake Castor.
type vcpEnv struct {
	t       *testing.T
	k8s     client.Client
	r       *TupleGenerationJobReconciler
	castor  *httptest.Server
	chunks  sync.Map // chunk id -> *atomic.Int32 activation count
	failAct atomic.Bool
}

func newVCPEnv(t *testing.T, etcd *clientv3.Client, playerID, playerCount int, objs ...client.Object) *vcpEnv {
	e := &vcpEnv{t: t}
	e.castor = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		const prefix = "/intra-vcp/tuple-chunks/activate/"
		if r.Method != http.MethodPut || len(r.URL.Path) <= len(prefix) || r.URL.Path[:len(prefix)] != prefix {
			http.NotFound(w, r)
			return
		}
		ctr, _ := e.chunks.LoadOrStore(r.URL.Path[len(prefix):], new(atomic.Int32))
		ctr.(*atomic.Int32).Add(1)
		if e.failAct.Load() {
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	t.Cleanup(e.castor.Close)
	cm := vcpConfigMap("default", map[string]string{"playerId": fmt.Sprint(playerID), "playerCount": fmt.Sprint(playerCount)})
	e.k8s = newFakeClient(t, append(objs, cm)...)
	e.r = &TupleGenerationJobReconciler{
		Client: e.k8s, Scheme: testScheme(t), EtcdClient: etcd, CastorClient: castor.NewClient(e.castor.URL), Logger: logr.Discard(),
	}
	return e
}

func (e *vcpEnv) activations(chunk string) int32 {
	if c, ok := e.chunks.Load(chunk); ok {
		return c.(*atomic.Int32).Load()
	}
	return 0
}

// runWatch starts the watch loop and returns a stop function that waits for the loop to terminate.
func (e *vcpEnv) runWatch() (stop func()) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); e.r.runWatchLoop(ctx, "default") }()
	var once sync.Once
	stop = func() {
		once.Do(func() {
			cancel()
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				e.t.Error("watch loop did not terminate")
			}
		})
	}
	e.t.Cleanup(stop)
	return stop
}

func (e *vcpEnv) task(name string) (*klyshkov1alpha1.TupleGenerationTask, error) {
	task := &klyshkov1alpha1.TupleGenerationTask{}
	err := e.k8s.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: name}, task)
	return task, err
}

func (e *vcpEnv) hasTask(name string) bool { _, err := e.task(name); return err == nil }

func newJob(name string) *klyshkov1alpha1.TupleGenerationJob {
	return &klyshkov1alpha1.TupleGenerationJob{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", UID: types.UID("uid-" + name)},
		Spec:       klyshkov1alpha1.TupleGenerationJobSpec{ID: uuid.New().String(), Type: tBit, Count: 5, Generator: "g"},
	}
}

func put(t *testing.T, c *clientv3.Client, k, v string) int64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	r, err := c.Put(ctx, k, v)
	if err != nil {
		t.Fatal(err)
	}
	return r.Header.Revision
}

func entryKey(job string, pid uint) string {
	return RosterEntryKey{RosterKey: rk("default", job), PlayerID: pid}.ToEtcdKey()
}

func storedHead(t *testing.T, c *clientv3.Client, pid int) (int64, bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	r, err := c.Get(ctx, fmt.Sprintf("/klyshko/heads/%d", pid))
	if err != nil {
		t.Fatal(err)
	}
	if r.Count == 0 {
		return 0, false
	}
	v, _ := binary.Varint(r.Kvs[0].Value)
	return v, true
}

// ---- Tests ------------------------------------------------------------------------------------------------------

func TestHeadRevisionKeyAndRoundTrip(t *testing.T) {
	etcd := newTestEtcd(t)
	c := etcd.newClient()
	for _, pid := range []int{0, 1, 3} {
		e := newVCPEnv(t, c, pid, 4)
		key, err := e.r.getHeadRevisionKey(context.Background(), "default")
		if err != nil || key != fmt.Sprintf("/klyshko/heads/%d", pid) {
			t.Fatalf("key=%q err=%v", key, err)
		}
		if rev, err := e.r.getHeadRevision(context.Background(), "default"); err != nil || rev != 0 {
			t.Fatalf("unset head must be 0, got %d %v", rev, err)
		}
		for _, rev := range []int64{1, 127, 128, 1 << 40} {
			if err := e.r.setHeadRevision(context.Background(), "default", rev); err != nil {
				t.Fatal(err)
			}
			if got, err := e.r.getHeadRevision(context.Background(), "default"); err != nil || got != rev {
				t.Fatalf("head round trip: got %d want %d (%v)", got, rev, err)
			}
		}
	}
	// no VCP config -> error
	e := newVCPEnv(t, c, 0, 1)
	e.r.Client = newFakeClient(t)
	if _, err := e.r.getHeadRevisionKey(context.Background(), "default"); err == nil {
		t.Fatal("expected error without VCP config")
	}
}

func TestRosterFlowsOnMaster(t *testing.T) {
	etcd := newTestEtcd(t)
	c := etcd.newClient()
	job := newJob("job-a")
	e := newVCPEnv(t, c, 0, 2, job)
	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "job-a"}}
	ctx := context.Background()
	key := RosterKey{req.NamespacedName}.ToEtcdKey()

	res, err := e.r.Reconcile(ctx, req)
	if err != nil || !res.Requeue {
		t.Fatalf("first reconcile should create the local task and requeue: %v %v", res, err)
	}
	resp, err := c.Get(ctx, key)
	if err != nil || resp.Count != 1 {
		t.Fatalf("roster not created: %v %v", resp, err)
	}
	if !e.hasTask("job-a-0") {
		t.Fatal("local task not created")
	}
	rev := resp.Kvs[0].ModRevision
	if _, err := e.r.Reconcile(ctx, req); err != nil {
		t.Fatal(err)
	}
	if resp, _ = c.Get(ctx, key); resp.Kvs[0].ModRevision != rev {
		t.Fatal("existing roster must not be rewritten")
	}

	// Deletion of job resource deletes roster on master
	if err := e.k8s.Delete(ctx, job); err != nil {
		t.Fatal(err)
	}
	if _, err := e.r.Reconcile(ctx, req); err != nil {
		t.Fatal(err)
	}
	if resp, _ = c.Get(ctx, key); resp.Count != 0 {
		t.Fatal("roster not deleted")
	}
}

func TestRosterFlowsOnFollower(t *testing.T) {
	etcd := newTestEtcd(t)
	c := etcd.newClient()
	e := newVCPEnv(t, c, 1, 2, newJob("job-a"))
	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "job-a"}}
	ctx := context.Background()
	if _, err := e.r.Reconcile(ctx, req); err != nil {
		t.Fatal(err)
	}
	if resp, _ := c.Get(ctx, RosterKey{req.NamespacedName}.ToEtcdKey()); resp.Count != 0 {
		t.Fatal("follower must not create roster")
	}
	if e.hasTask("job-a-1") {
		t.Fatal("follower must wait for the roster before creating its task")
	}
	// Follower does not delete a roster when the job disappears
	put(t, c, RosterKey{req.NamespacedName}.ToEtcdKey(), "{}")
	_ = e.k8s.Delete(ctx, newJob("job-a"))
	if _, err := e.r.Reconcile(ctx, req); err != nil {
		t.Fatal(err)
	}
	if resp, _ := c.Get(ctx, RosterKey{req.NamespacedName}.ToEtcdKey()); resp.Count != 1 {
		t.Fatal("follower must not delete roster")
	}
}

func TestReconcileEtcdUnavailable(t *testing.T) {
	etcd := newTestEtcd(t)
	c := etcd.newClient()
	e := newVCPEnv(t, c, 0, 2, newJob("job-a"))
	etcd.stop()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := e.r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "job-a"}})
	if err == nil {
		t.Fatal("expected error while etcd is down")
	}
	if e.hasTask("job-a-0") {
		t.Fatal("no task must be created while etcd is down")
	}
	etcd.start()
	if _, err := e.r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "job-a"}}); err != nil {
		t.Fatalf("reconcile must recover once etcd is back: %v", err)
	}
	if !e.hasTask("job-a-0") {
		t.Fatal("task not created after recovery")
	}
}

func ownedTask(job *klyshkov1alpha1.TupleGenerationJob, pid uint, state klyshkov1alpha1.TupleGenerationTaskState) *klyshkov1alpha1.TupleGenerationTask {
	return &klyshkov1alpha1.TupleGenerationTask{
		ObjectMeta: metav1.ObjectMeta{
			Name: taskNameForJob(job, pid), Namespace: "default",
			OwnerReferences: []metav1.OwnerReference{{APIVersion: "klyshko.carbnyestack.io/v1alpha1", Kind: "TupleGenerationJob", Name: job.Name, UID: job.UID}},
		},
		Spec:   klyshkov1alpha1.TupleGenerationTaskSpec{PlayerID: pid},
		Status: klyshkov1alpha1.TupleGenerationTaskStatus{State: state},
	}
}

func jobState(t *testing.T, e *vcpEnv, name string) klyshkov1alpha1.TupleGenerationJobState {
	j := &klyshkov1alpha1.TupleGenerationJob{}
	if err := e.k8s.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: name}, j); err != nil {
		t.Fatal(err)
	}
	return j.Status.State
}

func TestReconcileActivatesCastorExactlyOnceOnCompletion(t *testing.T) {
	etcd := newTestEtcd(t)
	c := etcd.newClient()
	job := newJob("job-a")
	e := newVCPEnv(t, c, 0, 2, job, ownedTask(job, 0, klyshkov1alpha1.TaskGenerating), ownedTask(job, 1, klyshkov1alpha1.TaskProvisioning))
	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "job-a"}}
	ctx := context.Background()

	reconcile := func() {
		t.Helper()
		if _, err := e.r.Reconcile(ctx, req); err != nil {
			t.Fatal(err)
		}
	}
	reconcile()
	if s := jobState(t, e, "job-a"); s != klyshkov1alpha1.JobRunning || e.activations(job.Spec.ID) != 0 {
		t.Fatalf("running expected without activation, got %q / %d", s, e.activations(job.Spec.ID))
	}
	setTask := func(name string, st klyshkov1alpha1.TupleGenerationTaskState) {
		task, err := e.task(name)
		if err != nil {
			t.Fatal(err)
		}
		task.Status.State = st
		if err := e.k8s.Status().Update(ctx, task); err != nil {
			t.Fatal(err)
		}
	}
	setTask("job-a-0", klyshkov1alpha1.TaskCompleted)
	reconcile()
	if e.activations(job.Spec.ID) != 0 {
		t.Fatal("must not activate while a task is still running")
	}

	// Activation failure: error, state unchanged, retried
	e.failAct.Store(true)
	setTask("job-a-1", klyshkov1alpha1.TaskCompleted)
	if _, err := e.r.Reconcile(ctx, req); err == nil {
		t.Fatal("expected activation error")
	}
	if s := jobState(t, e, "job-a"); s == klyshkov1alpha1.JobCompleted {
		t.Fatal("job must not be Completed when activation failed")
	}
	e.failAct.Store(false)
	reconcile()
	if s := jobState(t, e, "job-a"); s != klyshkov1alpha1.JobCompleted {
		t.Fatalf("expected Completed, got %q", s)
	}
	before := e.activations(job.Spec.ID)
	for i := 0; i < 3; i++ {
		reconcile()
	}
	if got := e.activations(job.Spec.ID); got != before || before != 2 {
		t.Fatalf("activation count after completion: before=%d after=%d (expected 2 = failed attempt + success, no more)", before, got)
	}
}

func TestReconcileNoActivationOnFailureOrInvalidID(t *testing.T) {
	etcd := newTestEtcd(t)
	c := etcd.newClient()
	job := newJob("job-f")
	e := newVCPEnv(t, c, 0, 2, job, ownedTask(job, 0, klyshkov1alpha1.TaskCompleted), ownedTask(job, 1, klyshkov1alpha1.TaskFailed))
	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "job-f"}}
	if _, err := e.r.Reconcile(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if jobState(t, e, "job-f") != klyshkov1alpha1.JobFailed || e.activations(job.Spec.ID) != 0 {
		t.Fatal("failed job must not be activated")
	}

	bad := newJob("job-bad")
	bad.Spec.ID = "not-a-uuid"
	e = newVCPEnv(t, c, 0, 2, bad, ownedTask(bad, 0, klyshkov1alpha1.TaskCompleted), ownedTask(bad, 1, klyshkov1alpha1.TaskCompleted))
	req.Name = "job-bad"
	if _, err := e.r.Reconcile(context.Background(), req); err == nil {
		t.Fatal("expected error for invalid job id")
	}
	if jobState(t, e, "job-bad") == klyshkov1alpha1.JobCompleted {
		t.Fatal("must not complete with invalid id")
	}
}

func statusJSON(state string) string {
	return fmt.Sprintf(`{"state":%q,"endpoint":"10.0.0.1:5000"}`, state)
}

func TestWatchRemoteTaskLifecycle(t *testing.T) {
	shortRetries(t)
	etcd := newTestEtcd(t)
	c := etcd.newClient()
	e := newVCPEnv(t, c, 1, 2, newJob("job-a"))
	e.runWatch()

	put(t, c, entryKey("job-a", 0), statusJSON("Generating"))
	eventually(t, "proxy task created in Preparing", func() bool {
		tk, err := e.task("job-a-0")
		return err == nil && tk.Status.State == klyshkov1alpha1.TaskPreparing
	})
	task, _ := e.task("job-a-0")
	if len(task.OwnerReferences) != 1 {
		t.Fatalf("new proxy: %+v", task)
	}

	put(t, c, entryKey("job-a", 0), statusJSON("Completed"))
	eventually(t, "proxy task updated", func() bool {
		tk, _ := e.task("job-a-0")
		return tk.Status.State == klyshkov1alpha1.TaskCompleted && tk.Status.Endpoint == "10.0.0.1:5000"
	})

	// Garbage status must not crash the loop nor change the proxy
	put(t, c, entryKey("job-a", 0), "{not json")
	put(t, c, entryKey("job-a", 0), statusJSON("Failed"))
	eventually(t, "task failed after garbage", func() bool {
		tk, _ := e.task("job-a-0")
		return tk.Status.State == klyshkov1alpha1.TaskFailed
	})

	// Event for the local player is ignored
	put(t, c, entryKey("job-a", 1), statusJSON("Generating"))
	// Unparsable key below the roster prefix is ignored
	put(t, c, "/klyshko/roster/default/job-a/x/y", "1")
	never(t, "local task proxied", 200*time.Millisecond, func() bool { return e.hasTask("job-a-1") })

	ctx := context.Background()
	if _, err := c.Delete(ctx, entryKey("job-a", 0)); err != nil {
		t.Fatal(err)
	}
	eventually(t, "proxy deleted", func() bool { return !e.hasTask("job-a-0") })
}

func TestWatchRemoteJobLifecycle(t *testing.T) {
	shortRetries(t)
	etcd := newTestEtcd(t)
	c := etcd.newClient()
	e := newVCPEnv(t, c, 1, 2)
	e.runWatch()

	spec := `{"id":"` + uuid.New().String() + `","type":"BIT_GFP","count":7,"generator":"g"}`
	rosterKey := RosterKey{types.NamespacedName{Namespace: "default", Name: "remote-job"}}.ToEtcdKey()
	put(t, c, rosterKey, spec)
	getJob := func() (*klyshkov1alpha1.TupleGenerationJob, error) {
		j := &klyshkov1alpha1.TupleGenerationJob{}
		return j, e.k8s.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "remote-job"}, j)
	}
	eventually(t, "job created", func() bool { j, err := getJob(); return err == nil && j.Spec.Count == 7 })

	put(t, c, rosterKey, "garbage") // must not delete or crash
	if _, err := c.Delete(context.Background(), rosterKey); err != nil {
		t.Fatal(err)
	}
	eventually(t, "job deleted", func() bool { _, err := getJob(); return apierrors.IsNotFound(err) })
}

func TestWatchResumesFromHeadRevisionWithoutLossOrReplay(t *testing.T) {
	shortRetries(t)
	etcd := newTestEtcd(t)
	c := etcd.newClient()
	e := newVCPEnv(t, c, 1, 2, newJob("job-a"), newJob("job-b"))
	stop := e.runWatch()

	rev := put(t, c, entryKey("job-a", 0), statusJSON("Generating"))
	eventually(t, "A proxied", func() bool { return e.hasTask("job-a-0") })
	eventually(t, "head stored", func() bool { h, ok := storedHead(t, c, 1); return ok && h >= rev })
	stop()
	head, _ := storedHead(t, c, 1)

	// Remove A's proxy: a replay of A's event would bring it back.
	task, _ := e.task("job-a-0")
	if err := e.k8s.Delete(context.Background(), task); err != nil {
		t.Fatal(err)
	}
	// Event produced while the operator is down
	put(t, c, entryKey("job-b", 0), statusJSON("Generating"))

	// "Restart": a fresh reconciler instance resumes from the stored head
	e2 := &vcpEnv{t: t, k8s: e.k8s}
	e2.r = &TupleGenerationJobReconciler{Client: e.k8s, Scheme: testScheme(t), EtcdClient: c, Logger: logr.Discard()}
	e2.runWatch()
	eventually(t, "B (missed while down) proxied", func() bool { return e.hasTask("job-b-0") })
	never(t, "A replayed after restart", 300*time.Millisecond, func() bool { return e.hasTask("job-a-0") })
	if h, _ := storedHead(t, c, 1); h <= head {
		t.Fatalf("head did not advance: %d <= %d", h, head)
	}
}

func TestWatchRecoversFromCompactedHeadRevision(t *testing.T) {
	shortRetries(t)
	etcd := newTestEtcd(t)
	c := etcd.newClient()
	e := newVCPEnv(t, c, 1, 2, newJob("job-a"), newJob("job-c"))
	stop := e.runWatch()
	put(t, c, entryKey("job-a", 0), statusJSON("Generating"))
	eventually(t, "A proxied", func() bool { return e.hasTask("job-a-0") })
	stop()

	var last int64
	for i := 0; i < 5; i++ {
		last = put(t, c, fmt.Sprintf("/unrelated/%d", i), "x")
	}
	if _, err := c.Compact(context.Background(), last); err != nil {
		t.Fatal(err)
	}

	e.runWatch()
	put(t, c, entryKey("job-c", 0), statusJSON("Generating"))
	eventually(t, "watch recovered from compaction", func() bool { return e.hasTask("job-c-0") })
}

func TestWatchClosedChannelIsReestablished(t *testing.T) {
	shortRetries(t)
	etcd := newTestEtcd(t)
	c := etcd.newClient()
	e := newVCPEnv(t, c, 1, 2, newJob("job-a"))
	// Wrap the watcher so that the first watch channel is closed immediately
	var calls atomic.Int32
	e.r.EtcdClient = c
	realWatcher := c.Watcher
	c.Watcher = &closingWatcher{Watcher: realWatcher, calls: &calls}
	e.runWatch()
	put(t, c, entryKey("job-a", 0), statusJSON("Generating"))
	eventually(t, "proxy created after closed channel", func() bool { return e.hasTask("job-a-0") })
	if calls.Load() < 2 {
		t.Fatalf("watch must have been re-registered, calls=%d", calls.Load())
	}
}

type closingWatcher struct {
	clientv3.Watcher
	calls *atomic.Int32
}

func (w *closingWatcher) Watch(ctx context.Context, key string, opts ...clientv3.OpOption) clientv3.WatchChan {
	if w.calls.Add(1) == 1 {
		ch := make(chan clientv3.WatchResponse)
		close(ch)
		return ch
	}
	return w.Watcher.Watch(ctx, key, opts...)
}

func TestWatchLoopSurvivesEtcdUnavailableAtStartAndMidRun(t *testing.T) {
	shortRetries(t)
	etcd := newTestEtcd(t)
	c := etcd.newClient()
	e := newVCPEnv(t, c, 1, 2, newJob("job-a"), newJob("job-b"))

	etcd.stop() // unavailable at startup
	e.runWatch()
	time.Sleep(300 * time.Millisecond)
	etcd.start()
	put(t, c, entryKey("job-a", 0), statusJSON("Generating"))
	eventually(t, "A processed after etcd came up", func() bool { return e.hasTask("job-a-0") })

	etcd.stop() // mid-run outage
	time.Sleep(300 * time.Millisecond)
	etcd.start()
	put(t, c, entryKey("job-b", 0), statusJSON("Generating"))
	eventually(t, "B processed after mid-run outage", func() bool { return e.hasTask("job-b-0") })
}

func TestHandleWatchEventUnexpectedKeys(t *testing.T) {
	etcd := newTestEtcd(t)
	e := newVCPEnv(t, etcd.newClient(), 1, 2)
	for _, k := range []string{"", "/klyshko/roster", "/other/x", "/klyshko/roster/a/b/c/d"} {
		e.r.handleWatchEvent(context.Background(), &clientv3.Event{Type: mvccpb.PUT, Kv: &mvccpb.KeyValue{Key: []byte(k), Value: []byte("x")}})
	}
}
