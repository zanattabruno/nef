package processor

import (
	"encoding/json"
	"github.com/free5gc/openapi/models"
	"github.com/gin-gonic/gin"
	"net/http/httptest"
	"testing"
	"time"
)

func qosRequest() QoSSubscriptionRequest {
	return QoSSubscriptionRequest{RequestID: "stable-request", UeIpv4Addr: "10.60.0.1", Dnn: "internet", Snssai: &models.Snssai{Sst: 1, Sd: "010203"}, QosReference: "LAB_GBR_DL", Flow: QoSFlow{ServerIP: "192.0.2.2", ServerPort: 5200, UEPort: 6000, Protocol: "UDP"}, NotificationDestination: "http://127.0.0.1:9090/api/qos/callback"}
}
func TestQoSOperatorProfileAndDecimalUnits(t *testing.T) {
	r := qosRequest()
	if e := validQoSRequest(&r); e != nil {
		t.Fatal(e)
	}
	if r.Duration != 120 || r.RequestedQos.FiveQI != 2 || pcfKbps(r.RequestedQos.GbrDl) != "20000 Kbps" {
		t.Fatal(r)
	}
	for _, bad := range []string{"NaN Mbps", "+Inf Mbps", "-Inf Mbps", "20 Mbps junk", "-1 Mbps", "1000 Mbps"} {
		if _, e := qosMbps(bad); e == nil {
			t.Fatal("accepted", bad)
		}
	}
	r.RequestedQos.FiveQI = 9
	if validQoSRequest(&r) == nil {
		t.Fatal("arbitrary 5QI accepted")
	}
}
func TestQoSEventsIdempotentAndTerminal(t *testing.T) {
	m := &qosManager{}
	req := qosRequest()
	_ = validQoSRequest(&req)
	r := &qosRecord{QoSSubscriptionRequest: req, Status: "AUTHORIZED"}
	n := &models.PcfPolicyAuthorizationEventsNotification{EvNotifs: []models.PcfPolicyAuthorizationAfEventNotification{{Event: models.PcfPolicyAuthorizationAfEvent_SUCCESSFUL_RESOURCES_ALLOCATION}}}
	m.applyEvents(r, n)
	if !r.AllocationConfirmed || r.Status != "ACTIVE" {
		t.Fatal("no activation")
	}
	expiry := *r.ExpiresAt
	time.Sleep(time.Millisecond)
	m.applyEvents(r, n)
	if !expiry.Equal(*r.ExpiresAt) || len(r.Events) != 1 {
		t.Fatal("duplicate extended lifetime")
	}
	m.transition(r, "RELEASED", "")
	m.applyEvents(r, n)
	if r.Status != "RELEASED" || r.AllocationConfirmed {
		t.Fatal("late callback revived subscription")
	}
}
func TestQoSStableRequestAndFlowConflict(t *testing.T) {
	gin.SetMode(gin.TestMode)
	m := &qosManager{records: map[string]*qosRecord{}, busy: map[string]bool{}}
	p := &Processor{qos: m}
	req := qosRequest()
	_ = validQoSRequest(&req)
	// Pre-populate durable identity without requiring a configured SBI server.
	m.records["sub"] = &qosRecord{QoSSubscriptionRequest: req, AF: "af", ID: "sub", Status: "REQUESTED"}
	send := func(r QoSSubscriptionRequest) int {
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		b, _ := json.Marshal(r)
		p.PostQoSSubscription(c, "af", b)
		return w.Code
	}
	if code := send(req); code != 200 {
		t.Fatal(code)
	}
	req.Duration = 60
	if send(req) != 409 {
		t.Fatal("same identity changed parameters")
	}
	req.RequestID = "another"
	if send(req) != 409 {
		t.Fatal("duplicate outstanding flow")
	}
}

func TestQoSDeadlineAndDurableRestart(t *testing.T) {
	t.Setenv("NEF_QOS_STATE_FILE", t.TempDir()+"/state.json")
	m, e := newQoSManager(nil)
	if e != nil {
		t.Fatal(e)
	}
	now := time.Now().UTC()
	r := &qosRecord{ID: "sub", Status: "AUTHORIZED", CreatedAt: now.Add(-31 * time.Second)}
	m.checkDeadline(r, now)
	if r.Status != "FAILED" || !r.CleanupPending {
		t.Fatal("activation timeout not held for cleanup")
	}
	expiry := now.Add(-time.Second)
	r = &qosRecord{ID: "active", Status: "ACTIVE", ExpiresAt: &expiry, AllocationConfirmed: true}
	m.records[r.ID] = r
	if e = m.saveLocked(); e != nil {
		t.Fatal(e)
	}
	loaded, e := newQoSManager(nil)
	if e != nil {
		t.Fatal(e)
	}
	r = loaded.records[r.ID]
	loaded.checkDeadline(r, now)
	if r.Status != "EXPIRED" || r.AllocationConfirmed || !r.CleanupPending {
		t.Fatal("restart renewed expired allocation")
	}
}
