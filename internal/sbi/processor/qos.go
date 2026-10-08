package processor

// The northbound subscription is durable and distinct from the PCF policy and
// the allocation result. HTTP creation alone never means QoS is active.
import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/free5gc/nef/pkg/factory"
	"github.com/free5gc/openapi/models"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

type QoSFlow struct {
	ServerIP   string `json:"serverIp"`
	ServerPort int    `json:"serverPort"`
	UEPort     int    `json:"uePort"`
	Protocol   string `json:"protocol"`
}
type RequestedQos struct {
	FiveQI int    `json:"5qi,omitempty"`
	GbrDl  string `json:"gbrDl"`
	GbrUl  string `json:"gbrUl,omitempty"`
	MbrDl  string `json:"mbrDl"`
	MbrUl  string `json:"mbrUl,omitempty"`
}
type QoSSubscriptionRequest struct {
	UeIpv4Addr              string         `json:"ueIpv4Addr"`
	QosReference            string         `json:"qosReference"`
	Snssai                  *models.Snssai `json:"snssai"`
	Dnn                     string         `json:"dnn"`
	UeId                    string         `json:"ueId,omitempty"`
	NotificationDestination string         `json:"notificationDestination"`
	RequestedQos            *RequestedQos  `json:"requestedQos,omitempty"`
	Flow                    QoSFlow        `json:"flow"`
	Duration                int            `json:"duration"`
	RequestID               string         `json:"requestId"`
}
type QoSSubscriptionPatch struct {
	Duration int `json:"duration"`
}
type qosEvent struct {
	At     time.Time `json:"at"`
	Status string    `json:"status"`
	Detail string    `json:"detail,omitempty"`
}
type qosRecord struct {
	QoSSubscriptionRequest
	ID                  string        `json:"subscriptionId"`
	AF                  string        `json:"afId"`
	AppSessionID        string        `json:"appSessionId,omitempty"`
	Status              string        `json:"status"`
	PolicyAuthorized    bool          `json:"policyAuthorized"`
	AllocationConfirmed bool          `json:"allocationConfirmed"`
	AuthorizedQos       *RequestedQos `json:"authorizedQos,omitempty"`
	CreatedAt           time.Time     `json:"createdAt"`
	ActivatedAt         *time.Time    `json:"activatedAt,omitempty"`
	ExpiresAt           *time.Time    `json:"expiresAt,omitempty"`
	LastError           string        `json:"lastError,omitempty"`
	CleanupPending      bool          `json:"cleanupPending"`
	Events              []qosEvent    `json:"events"`
}
type qosManager struct {
	mu      sync.Mutex
	records map[string]*qosRecord
	busy    map[string]bool
	path    string
	err     error
	p       *Processor
}

func newQoSManager(p *Processor) (*qosManager, error) {
	m := &qosManager{records: map[string]*qosRecord{}, busy: map[string]bool{}, path: os.Getenv("NEF_QOS_STATE_FILE"), p: p}
	if m.path != "" {
		b, e := os.ReadFile(m.path)
		if e == nil {
			if e = json.Unmarshal(b, &m.records); e != nil {
				return nil, e
			}
		} else if !os.IsNotExist(e) {
			return nil, e
		}
	}
	return m, nil
}
func (m *qosManager) saveLocked() (result error) {
	defer func() { m.err = result }()
	if m.path == "" {
		return nil
	}
	b, e := json.MarshalIndent(m.records, "", "  ")
	if e != nil {
		return e
	}
	if e = os.MkdirAll(filepath.Dir(m.path), 0700); e != nil {
		return e
	}
	f, e := os.OpenFile(m.path+".tmp", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if e != nil {
		return e
	}
	if _, e = f.Write(b); e == nil {
		e = f.Sync()
	}
	ce := f.Close()
	if e == nil {
		e = ce
	}
	if e == nil {
		e = os.Rename(m.path+".tmp", m.path)
	}
	m.err = e
	return e
}
func (m *qosManager) transition(r *qosRecord, status, detail string) {
	if r.Status == status && r.LastError == detail {
		return
	}
	r.Status = status
	r.LastError = detail
	r.AllocationConfirmed = status == "ACTIVE"
	r.Events = append(r.Events, qosEvent{time.Now().UTC(), status, detail})
	if len(r.Events) > 100 {
		r.Events = r.Events[len(r.Events)-100:]
	}
}
func cloneQoS(r *qosRecord) *qosRecord {
	b, _ := json.Marshal(r)
	var c qosRecord
	_ = json.Unmarshal(b, &c)
	return &c
}
func terminalQoS(s string) bool {
	return s == "RELEASED" || s == "EXPIRED" || s == "REJECTED" || s == "FAILED"
}
func validQoSRequest(r *QoSSubscriptionRequest) error {
	if r.RequestID == "" || len(r.RequestID) > 128 {
		return fmt.Errorf("requestId is required (maximum 128 characters)")
	}
	if r.QosReference != "LAB_GBR_DL" {
		return fmt.Errorf("supported qosReference: LAB_GBR_DL")
	}
	if net.ParseIP(r.UeIpv4Addr).To4() == nil || net.ParseIP(r.Flow.ServerIP).To4() == nil {
		return fmt.Errorf("valid UE and server IPv4 addresses required")
	}
	if r.Dnn == "" || r.Snssai == nil {
		return fmt.Errorf("DNN and S-NSSAI from an established PDU session required")
	}
	if r.Flow.Protocol != "UDP" || r.Flow.ServerPort < 1 || r.Flow.ServerPort > 65535 || r.Flow.UEPort < 1 || r.Flow.UEPort > 65535 {
		return fmt.Errorf("an exact IPv4 UDP flow is required")
	}
	u, e := url.Parse(r.NotificationDestination)
	if e != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return fmt.Errorf("notificationDestination must be an HTTP(S) URL")
	}
	if r.Duration == 0 {
		r.Duration = 120
	}
	if r.Duration < 1 || r.Duration > 3600 {
		return fmt.Errorf("duration must be 1..3600 seconds")
	}
	if r.RequestedQos == nil {
		r.RequestedQos = &RequestedQos{FiveQI: 2, GbrDl: "20 Mbps", MbrDl: "25 Mbps", GbrUl: "1 Kbps", MbrUl: "1 Mbps"}
	}
	q := r.RequestedQos
	if q.FiveQI != 0 && q.FiveQI != 2 {
		return fmt.Errorf("operator profile fixes 5QI to 2")
	}
	q.FiveQI = 2
	g, e := qosMbps(q.GbrDl)
	if e != nil {
		return e
	}
	b, e := qosMbps(q.MbrDl)
	if e != nil {
		return e
	}
	if g < 0.001 || b < g || b > 100 {
		return fmt.Errorf("require 0 < GBR <= MBR <= 100 Mbps")
	}
	q.GbrUl = "1 Kbps"
	q.MbrUl = "1 Mbps"
	return nil
}
func qosMbps(s string) (float64, error) {
	fields := strings.Fields(s)
	if len(fields) != 2 || fields[1] != "Mbps" {
		return 0, fmt.Errorf("downlink rates must use Mbps")
	}
	n, e := strconv.ParseFloat(fields[0], 64)
	if e != nil || math.IsNaN(n) || math.IsInf(n, 0) || n <= 0 || n > 100 {
		return 0, fmt.Errorf("require a finite rate in (0,100] Mbps")
	}
	return n, nil
}

func (p *Processor) GetQoSSubscriptions(c *gin.Context, af string) {
	m := p.qos
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		c.JSON(503, gin.H{"detail": "reservation state is not durable: " + m.err.Error()})
		return
	}
	out := []*qosRecord{}
	for _, r := range m.records {
		if r.AF == af {
			out = append(out, cloneQoS(r))
		}
	}
	c.JSON(200, out)
}
func (p *Processor) GetIndividualQoSSubscription(c *gin.Context, af, id string) {
	m := p.qos
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		c.JSON(503, gin.H{"detail": "reservation state is not durable: " + m.err.Error()})
		return
	}
	r := m.records[id]
	if r == nil || r.AF != af {
		c.JSON(404, gin.H{"detail": "subscription not found"})
		return
	}
	c.JSON(200, cloneQoS(r))
}
func (p *Processor) PostQoSSubscription(c *gin.Context, af string, body []byte) {
	var req QoSSubscriptionRequest
	if e := json.Unmarshal(body, &req); e != nil {
		c.JSON(400, gin.H{"detail": e.Error()})
		return
	}
	if e := validQoSRequest(&req); e != nil {
		c.JSON(400, gin.H{"detail": e.Error()})
		return
	}
	m := p.qos
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, r := range m.records {
		if r.AF == af && r.RequestID == req.RequestID {
			a, _ := json.Marshal(r.QoSSubscriptionRequest)
			b, _ := json.Marshal(req)
			if !bytes.Equal(a, b) {
				c.JSON(409, gin.H{"detail": "requestId already used with different parameters"})
				return
			}
			c.JSON(200, cloneQoS(r))
			return
		}
		if (r.CleanupPending || !terminalQoS(r.Status)) && r.UeIpv4Addr == req.UeIpv4Addr && r.Flow == req.Flow {
			c.JSON(409, gin.H{"detail": "flow already has an outstanding subscription"})
			return
		}
	}
	r := &qosRecord{QoSSubscriptionRequest: req, ID: uuid.NewString(), AF: af, CreatedAt: time.Now().UTC()}
	m.transition(r, "REQUESTED", "")
	m.records[r.ID] = r
	if e := m.saveLocked(); e != nil {
		delete(m.records, r.ID)
		c.JSON(503, gin.H{"detail": "cannot persist request: " + e.Error()})
		return
	}
	c.Header("Location", p.genQoSSubURI(af, r.ID))
	c.JSON(201, cloneQoS(r))
}
func (p *Processor) PatchQoSSubscription(c *gin.Context, af, id string, b []byte) {
	var patch QoSSubscriptionPatch
	if e := json.Unmarshal(b, &patch); e != nil || patch.Duration < 1 || patch.Duration > 3600 {
		c.JSON(400, gin.H{"detail": "duration must be 1..3600 seconds; flow and rates are immutable"})
		return
	}
	m := p.qos
	m.mu.Lock()
	defer m.mu.Unlock()
	r := m.records[id]
	if r == nil || r.AF != af {
		c.Status(404)
		return
	}
	m.checkDeadline(r, time.Now().UTC())
	if r.Status != "ACTIVE" || r.CleanupPending {
		_ = m.saveLocked()
		c.JSON(409, gin.H{"detail": "only active reservations can be renewed"})
		return
	}
	old := cloneQoS(r)
	t := time.Now().UTC().Add(time.Duration(patch.Duration) * time.Second)
	r.ExpiresAt = &t
	r.Events = append(r.Events, qosEvent{time.Now().UTC(), "RENEWED", ""})
	if e := m.saveLocked(); e != nil {
		m.records[id] = old
		c.JSON(503, gin.H{"detail": e.Error()})
		return
	}
	c.JSON(200, cloneQoS(r))
}
func (p *Processor) DeleteQoSSubscription(c *gin.Context, af, id string) {
	m := p.qos
	m.mu.Lock()
	defer m.mu.Unlock()
	r := m.records[id]
	if r == nil || r.AF != af {
		c.Status(404)
		return
	}
	if !terminalQoS(r.Status) {
		m.transition(r, "RELEASED", "")
		r.CleanupPending = true
	}
	if e := m.saveLocked(); e != nil {
		c.JSON(503, gin.H{"detail": e.Error()})
		return
	}
	c.JSON(202, cloneQoS(r))
}
func (p *Processor) genQoSSubURI(af, id string) string {
	return p.Config().ServiceUri(factory.ServiceAsSessionWithQoS) + "/" + af + "/subscriptions/" + id
}
func (p *Processor) convertQoSRequestToAppSessionContext(req *QoSSubscriptionRequest, id string) *models.AppSessionContext {
	callback := p.Config().ServiceUri(factory.ServiceAsSessionWithQoS) + "/callbacks/" + id
	q := req.RequestedQos
	// Direction is downlink. The small UL allowance is for application probes.
	flow := fmt.Sprintf("permit out 17 from %s %d to %s %d", req.Flow.ServerIP, req.Flow.ServerPort, req.UeIpv4Addr, req.Flow.UEPort)
	raw := map[string]interface{}{"ascReqData": map[string]interface{}{
		"afAppId": "qoslab-" + id, "ueIpv4": req.UeIpv4Addr, "dnn": req.Dnn, "sliceInfo": req.Snssai, "notifUri": callback, "suppFeat": "3",
		"evSubsc":       map[string]interface{}{"notifUri": callback, "events": []map[string]string{{"event": "SUCCESSFUL_RESOURCES_ALLOCATION"}, {"event": "FAILED_RESOURCES_ALLOCATION"}, {"event": "QOS_NOTIF"}}},
		"medComponents": map[string]interface{}{"1": map[string]interface{}{"medCompN": 1, "medType": "VIDEO", "fStatus": "ENABLED", "mirBwDl": pcfKbps(q.GbrDl), "marBwDl": pcfKbps(q.MbrDl), "mirBwUl": q.GbrUl, "marBwUl": q.MbrUl, "medSubComps": map[string]interface{}{"1": map[string]interface{}{"fNum": 1, "fDescs": []string{flow}, "fStatus": "ENABLED"}}}},
	}}
	b, _ := json.Marshal(raw)
	var asc models.AppSessionContext
	_ = json.Unmarshal(b, &asc)
	return &asc
}
func (p *Processor) QoSNotification(c *gin.Context) {
	var n models.PcfPolicyAuthorizationEventsNotification
	if e := c.ShouldBindJSON(&n); e != nil {
		c.Status(400)
		return
	}
	m := p.qos
	m.mu.Lock()
	defer m.mu.Unlock()
	r := m.records[c.Param("subID")]
	if r == nil {
		c.Status(404)
		return
	}
	if terminalQoS(r.Status) || r.CleanupPending {
		c.Status(204)
		return
	}
	old := cloneQoS(r)
	m.applyEvents(r, &n)
	if e := m.saveLocked(); e != nil {
		m.records[r.ID] = old
		c.Status(503)
		return
	}
	c.Status(204)
}
func (m *qosManager) applyEvents(r *qosRecord, n *models.PcfPolicyAuthorizationEventsNotification) {
	if n == nil || terminalQoS(r.Status) || r.CleanupPending {
		return
	}
	for _, e := range n.EvNotifs {
		switch e.Event {
		case models.PcfPolicyAuthorizationAfEvent_SUCCESSFUL_RESOURCES_ALLOCATION:
			if r.ActivatedAt == nil {
				now := time.Now().UTC()
				expiry := now.Add(time.Duration(r.Duration) * time.Second)
				r.ActivatedAt = &now
				r.ExpiresAt = &expiry
			}
			r.PolicyAuthorized = true
			r.AuthorizedQos = r.RequestedQos
			m.transition(r, "ACTIVE", "")
		case models.PcfPolicyAuthorizationAfEvent_FAILED_RESOURCES_ALLOCATION:
			m.transition(r, "FAILED", "network resource allocation failed")
			r.CleanupPending = true
		case models.PcfPolicyAuthorizationAfEvent_QOS_NOTIF:
			for _, report := range n.QncReports {
				if string(report.NotifType) == "NOT_GUARANTEED" {
					m.transition(r, "FAILED", "network reports QoS unavailable")
					r.CleanupPending = true
				}
			}
		}
	}
}
func (m *qosManager) run() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for range ticker.C {
		m.mu.Lock()
		for id, r := range m.records {
			// A slow PCF request must not block the subscription's deadline.
			before := r.Status
			m.checkDeadline(r, time.Now().UTC())
			if before != r.Status {
				_ = m.saveLocked()
			}
			if m.busy[id] {
				continue
			}
			if terminalQoS(r.Status) && !r.CleanupPending {
				continue
			}
			m.busy[id] = true
			go m.work(id)
		}
		m.mu.Unlock()
	}
}
func (m *qosManager) work(id string) {
	defer func() { m.mu.Lock(); delete(m.busy, id); m.mu.Unlock() }()
	m.mu.Lock()
	r := cloneQoS(m.records[id])
	m.mu.Unlock()
	// Replaying a create uses a stable AF application identity; the PCF handles
	// it idempotently, including after a response was lost or the NEF restarted.
	if r.AppSessionID == "" {
		app, pd, e := m.p.Consumer().PostAppSessions(m.p.convertQoSRequestToAppSessionContext(&r.QoSSubscriptionRequest, r.ID))
		m.mu.Lock()
		live := m.records[id]
		if pd != nil {
			if !terminalQoS(live.Status) {
				m.transition(live, "REJECTED", pd.Detail)
			}
			live.CleanupPending = false
		} else if e != nil {
			live.LastError = e.Error()
		} else if app != "" {
			live.AppSessionID = app
			live.PolicyAuthorized = true
			live.AuthorizedQos = live.RequestedQos
			if live.Status == "REQUESTED" {
				m.transition(live, "AUTHORIZED", "")
			}
		}
		_ = m.saveLocked()
		r = cloneQoS(live)
		m.mu.Unlock()
	}
	if r.AppSessionID != "" && !terminalQoS(r.Status) {
		asc, pd, e := m.p.Consumer().GetAppSession(r.AppSessionID)
		m.mu.Lock()
		live := m.records[id]
		if !terminalQoS(live.Status) {
			if pd != nil && pd.Status == 404 {
				m.transition(live, "FAILED", "PCF session no longer exists")
				live.CleanupPending = false
			} else if e == nil && pd == nil && asc != nil {
				m.applyEvents(live, asc.EvsNotif)
			}
		}
		_ = m.saveLocked()
		r = cloneQoS(live)
		m.mu.Unlock()
	}
	m.mu.Lock()
	live := m.records[id]
	m.checkDeadline(live, time.Now().UTC())
	_ = m.saveLocked()
	r = cloneQoS(live)
	m.mu.Unlock()
	if r.CleanupPending && r.AppSessionID != "" {
		_, pd, e := m.p.Consumer().DeleteAppSession(r.AppSessionID)
		m.mu.Lock()
		live = m.records[id]
		if e == nil && (pd == nil || pd.Status == 404) {
			live.CleanupPending = false
		} else if e != nil {
			live.LastError = "cleanup: " + e.Error()
		} else {
			live.LastError = "cleanup: " + pd.Detail
		}
		_ = m.saveLocked()
		r = cloneQoS(live)
		m.mu.Unlock()
	}
	// Callbacks are repeatable snapshots; the AF also polls for recovery.
	if r.NotificationDestination != "" {
		b, _ := json.Marshal(r)
		client := http.Client{Timeout: 2 * time.Second}
		resp, e := client.Post(r.NotificationDestination, "application/json", bytes.NewReader(b))
		if e == nil {
			resp.Body.Close()
		}
	}
}

// The core bitrate converter treats Mbps as binary multiples. Send decimal
// Kbps explicitly so 20 Mbps corresponds to 20,000,000 bits/s in Linux HTB.
func pcfKbps(rate string) string { n, _ := qosMbps(rate); return fmt.Sprintf("%.0f Kbps", n*1000) }

func (m *qosManager) checkDeadline(r *qosRecord, now time.Time) {

	if !terminalQoS(r.Status) {
		if r.ExpiresAt != nil && !now.Before(*r.ExpiresAt) {
			m.transition(r, "EXPIRED", "")
			r.CleanupPending = true
		} else if r.ActivatedAt == nil && now.Sub(r.CreatedAt) > 30*time.Second {
			m.transition(r, "FAILED", "activation timeout")
			r.CleanupPending = true
		}
	}
}
