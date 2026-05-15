package processor

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/free5gc/nef/internal/logger"
	"github.com/free5gc/nef/pkg/factory"
	"github.com/free5gc/openapi"
	"github.com/free5gc/openapi/models"
	"github.com/free5gc/util/metrics/sbi"
	"github.com/gin-gonic/gin"
)

// ── QoS Subscription Request/Patch (3GPP TS 29.122 §5.6) ──────────────────
// These types represent the AF-facing API. The NEF translates them to the
// PCF's AppSessionContext (Npcf_PolicyAuthorization, TS 29.514).

type QoSSubscriptionRequest struct {
	UeIpv4Addr              string         `json:"ueIpv4Addr"`
	QosReference            string         `json:"qosReference"`
	Snssai                  *models.Snssai `json:"snssai,omitempty"`
	Dnn                     string         `json:"dnn,omitempty"`
	UeId                    string         `json:"ueId,omitempty"`
	NotificationDestination string         `json:"notificationDestination,omitempty"`
	RequestedQos            *RequestedQos  `json:"requestedQos,omitempty"`
}

type RequestedQos struct {
	FiveQI int    `json:"5qi,omitempty"`
	GbrDl  string `json:"gbrDl,omitempty"`
	GbrUl  string `json:"gbrUl,omitempty"`
	MbrDl  string `json:"mbrDl,omitempty"`
	MbrUl  string `json:"mbrUl,omitempty"`
}

type QoSSubscriptionPatch struct {
	QosReference string        `json:"qosReference,omitempty"`
	UeIpv4Addr   string        `json:"ueIpv4Addr,omitempty"`
	RequestedQos *RequestedQos `json:"requestedQos,omitempty"`
}

// ── Processor Methods ──────────────────────────────────────────────────────

// GetQoSSubscriptions lists all QoS subscriptions for an AF.
func (p *Processor) GetQoSSubscriptions(c *gin.Context, afID string) {
	logger.TrafInfluLog.Infof("GetQoSSubscriptions - afID[%s]", afID)

	af := p.Context().GetAf(afID)
	if af == nil {
		c.JSON(http.StatusOK, []interface{}{})
		return
	}

	af.Mu.RLock()
	defer af.Mu.RUnlock()

	var subs []map[string]interface{}
	for _, sub := range af.Subs {
		if sub.QosSub == nil {
			continue
		}
		subs = append(subs, sub.QosSub)
	}
	if subs == nil {
		subs = []map[string]interface{}{}
	}
	c.JSON(http.StatusOK, subs)
}

// PostQoSSubscription creates a new QoS subscription.
// AF → NEF (this) → PCF (Npcf_PolicyAuthorization PostAppSessions).
func (p *Processor) PostQoSSubscription(c *gin.Context, afID string, reqBody []byte) {
	logger.TrafInfluLog.Infof("PostQoSSubscription - afID[%s]", afID)

	var req QoSSubscriptionRequest
	if err := json.Unmarshal(reqBody, &req); err != nil {
		pd := openapi.ProblemDetailsMalformedReqSyntax(err.Error())
		c.Set(sbi.IN_PB_DETAILS_CTX_STR, pd.Cause)
		c.JSON(http.StatusBadRequest, pd)
		return
	}

	if req.QosReference == "" {
		pd := openapi.ProblemDetailsMalformedReqSyntax(
			"Missing required field: qosReference")
		c.Set(sbi.IN_PB_DETAILS_CTX_STR, pd.Cause)
		c.JSON(http.StatusBadRequest, pd)
		return
	}

	nefCtx := p.Context()
	af := nefCtx.GetAf(afID)
	if af == nil {
		af = nefCtx.NewAf(afID)
		if af == nil {
			pd := openapi.ProblemDetailsSystemFailure("No resource can be allocated")
			c.Set(sbi.IN_PB_DETAILS_CTX_STR, pd.Cause)
			c.JSON(int(pd.Status), pd)
			return
		}
	}

	af.Mu.Lock()
	defer af.Mu.Unlock()

	correID := nefCtx.NewCorreID()
	afSub := af.NewQoSSub(correID)
	if afSub == nil {
		pd := openapi.ProblemDetailsSystemFailure("No resource can be allocated")
		c.Set(sbi.IN_PB_DETAILS_CTX_STR, pd.Cause)
		c.JSON(int(pd.Status), pd)
		return
	}

	// Convert to PCF AppSessionContext and attempt to forward
	asc := p.convertQoSRequestToAppSessionContext(&req, afSub.NotifCorreID)
	pcfStatus := "FORWARDED"

	appSessId, pd, err := p.Consumer().PostAppSessions(asc)
	switch {
	case pd != nil:
		logger.TrafInfluLog.Warnf("PCF rejected QoS session (status=%d): %s — storing locally",
			pd.Status, pd.Detail)
		pcfStatus = fmt.Sprintf("PCF_REJECTED(%d)", pd.Status)
	case err != nil:
		logger.TrafInfluLog.Warnf("PCF unreachable: %v — storing locally", err)
		pcfStatus = "PCF_UNREACHABLE"
	default:
		afSub.AppSessID = appSessId
		pcfStatus = "ACTIVE"
		logger.TrafInfluLog.Infof("PCF AppSession created: %s", appSessId)
	}

	// Build response as map to avoid import cycle with context package
	subURI := p.genQoSSubURI(afID, afSub.SubID)
	resp := map[string]interface{}{
		"self":           subURI,
		"subscriptionId": afSub.SubID,
		"afId":           afID,
		"ueIpv4Addr":     req.UeIpv4Addr,
		"qosReference":   req.QosReference,
		"dnn":            req.Dnn,
		"appSessionId":   afSub.AppSessID,
		"pcfStatus":      pcfStatus,
		"status":         "ACTIVE",
	}
	if req.Snssai != nil {
		resp["snssai"] = req.Snssai
	}

	afSub.QosSub = resp
	af.Subs[afSub.SubID] = afSub
	nefCtx.AddAf(af)

	logger.TrafInfluLog.Infof("QoS subscription created: subID=%s, profile=%s, UE=%s, pcf=%s",
		afSub.SubID, req.QosReference, req.UeIpv4Addr, pcfStatus)

	c.Header("Location", subURI)
	c.JSON(http.StatusCreated, resp)
}

// GetIndividualQoSSubscription reads a single QoS subscription.
func (p *Processor) GetIndividualQoSSubscription(c *gin.Context, afID, subID string) {
	logger.TrafInfluLog.Infof("GetIndividualQoSSubscription - afID[%s], subID[%s]", afID, subID)

	af := p.Context().GetAf(afID)
	if af == nil {
		pd := openapi.ProblemDetailsDataNotFound("AF not found")
		c.Set(sbi.IN_PB_DETAILS_CTX_STR, pd.Cause)
		c.JSON(int(pd.Status), pd)
		return
	}

	af.Mu.RLock()
	defer af.Mu.RUnlock()

	afSub, ok := af.Subs[subID]
	if !ok || afSub.QosSub == nil {
		pd := openapi.ProblemDetailsDataNotFound("Subscription not found")
		c.Set(sbi.IN_PB_DETAILS_CTX_STR, pd.Cause)
		c.JSON(int(pd.Status), pd)
		return
	}

	c.JSON(http.StatusOK, afSub.QosSub)
}

// PatchQoSSubscription updates a QoS subscription (e.g., profile change).
// Translates to Npcf_PolicyAuthorization PatchAppSession.
func (p *Processor) PatchQoSSubscription(c *gin.Context, afID, subID string, reqBody []byte) {
	logger.TrafInfluLog.Infof("PatchQoSSubscription - afID[%s], subID[%s]", afID, subID)

	var patch QoSSubscriptionPatch
	if err := json.Unmarshal(reqBody, &patch); err != nil {
		pd := openapi.ProblemDetailsMalformedReqSyntax(err.Error())
		c.Set(sbi.IN_PB_DETAILS_CTX_STR, pd.Cause)
		c.JSON(http.StatusBadRequest, pd)
		return
	}

	af := p.Context().GetAf(afID)
	if af == nil {
		pd := openapi.ProblemDetailsDataNotFound("AF not found")
		c.Set(sbi.IN_PB_DETAILS_CTX_STR, pd.Cause)
		c.JSON(int(pd.Status), pd)
		return
	}

	af.Mu.Lock()
	defer af.Mu.Unlock()

	afSub, ok := af.Subs[subID]
	if !ok || afSub.QosSub == nil {
		pd := openapi.ProblemDetailsDataNotFound("Subscription not found")
		c.Set(sbi.IN_PB_DETAILS_CTX_STR, pd.Cause)
		c.JSON(int(pd.Status), pd)
		return
	}

	// If we have a PCF AppSession, update it
	if afSub.AppSessID != "" {
		ascUpdate := p.convertQoSPatchToAppSessionUpdate(&patch)
		_, pd, err := p.Consumer().PatchAppSession(afSub.AppSessID, ascUpdate)
		if pd != nil {
			logger.TrafInfluLog.Warnf("PCF patch rejected (status=%d): %s", pd.Status, pd.Detail)
		} else if err != nil {
			logger.TrafInfluLog.Warnf("PCF patch failed: %v", err)
		} else {
			logger.TrafInfluLog.Infof("PCF AppSession updated: %s", afSub.AppSessID)
		}
	}

	// Update local state
	oldProfile := ""
	if v, ok := afSub.QosSub["qosReference"].(string); ok {
		oldProfile = v
	}
	if patch.QosReference != "" {
		afSub.QosSub["qosReference"] = patch.QosReference
	}
	if patch.UeIpv4Addr != "" {
		afSub.QosSub["ueIpv4Addr"] = patch.UeIpv4Addr
	}

	newProfile := ""
	if v, ok := afSub.QosSub["qosReference"].(string); ok {
		newProfile = v
	}

	logger.TrafInfluLog.Infof("QoS subscription updated: subID=%s, %s → %s",
		subID, oldProfile, newProfile)

	c.JSON(http.StatusOK, afSub.QosSub)
}

// DeleteQoSSubscription deletes a QoS subscription.
// Translates to Npcf_PolicyAuthorization DeleteAppSession.
func (p *Processor) DeleteQoSSubscription(c *gin.Context, afID, subID string) {
	logger.TrafInfluLog.Infof("DeleteQoSSubscription - afID[%s], subID[%s]", afID, subID)

	af := p.Context().GetAf(afID)
	if af == nil {
		pd := openapi.ProblemDetailsDataNotFound("AF not found")
		c.Set(sbi.IN_PB_DETAILS_CTX_STR, pd.Cause)
		c.JSON(int(pd.Status), pd)
		return
	}

	af.Mu.Lock()
	defer af.Mu.Unlock()

	sub, ok := af.Subs[subID]
	if !ok {
		pd := openapi.ProblemDetailsDataNotFound("Subscription not found")
		c.Set(sbi.IN_PB_DETAILS_CTX_STR, pd.Cause)
		c.JSON(int(pd.Status), pd)
		return
	}

	if sub.AppSessID != "" {
		_, pd, err := p.Consumer().DeleteAppSession(sub.AppSessID)
		if err != nil {
			logger.TrafInfluLog.Warnf("PCF delete failed: %v", err)
		} else if pd != nil {
			logger.TrafInfluLog.Warnf("PCF delete rejected (status=%d): %s", pd.Status, pd.Detail)
		} else {
			logger.TrafInfluLog.Infof("PCF AppSession deleted: %s", sub.AppSessID)
		}
	}

	delete(af.Subs, subID)
	logger.TrafInfluLog.Infof("QoS subscription deleted: subID=%s", subID)

	c.Status(http.StatusNoContent)
}

// ── Conversion Helpers ─────────────────────────────────────────────────────

func (p *Processor) genQoSSubURI(afID, subID string) string {
	return p.Config().ServiceUri(factory.ServiceAsSessionWithQoS) +
		"/" + afID + "/subscriptions/" + subID
}

// convertQoSRequestToAppSessionContext translates AF QoS request to PCF AppSessionContext.
// TS 29.122 → TS 29.514 mapping.
func (p *Processor) convertQoSRequestToAppSessionContext(
	req *QoSSubscriptionRequest,
	notifCorreID string,
) *models.AppSessionContext {
	asc := &models.AppSessionContext{
		AscReqData: &models.AppSessionContextReqData{
			AfAppId:  req.QosReference,
			UeIpv4:   req.UeIpv4Addr,
			Dnn:      req.Dnn,
			NotifUri: req.NotificationDestination,
			SuppFeat: "1",
		},
	}

	if req.Snssai != nil {
		asc.AscReqData.SliceInfo = req.Snssai
	}

	if asc.AscReqData.NotifUri == "" {
		asc.AscReqData.NotifUri = p.genNotificationUri()
	}

	return asc
}

// convertQoSPatchToAppSessionUpdate translates AF QoS patch to PCF update data.
func (p *Processor) convertQoSPatchToAppSessionUpdate(
	patch *QoSSubscriptionPatch,
) *models.AppSessionContextUpdateData {
	update := &models.AppSessionContextUpdateData{}

	if patch.UeIpv4Addr != "" {
		logger.TrafInfluLog.Infof("QoS patch includes UE IP change: %s", patch.UeIpv4Addr)
	}

	return update
}
