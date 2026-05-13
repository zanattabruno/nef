package sbi

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"

	"github.com/free5gc/nef/internal/logger"
	"github.com/gin-gonic/gin"
)

const NwdafAnalyticsURI = "http://127.0.0.15:8000"

func (s *Server) getEventExposureRoutes() []Route {
	return []Route{
		{
			Method:  http.MethodPost,
			Pattern: "/subscriptions",
			APIFunc: s.apiPostEventExposureSubscription,
		},
		{
			Method:  http.MethodGet,
			Pattern: "/subscriptions/:subId",
			APIFunc: s.apiGetEventExposureSubscription,
		},
		{
			Method:  http.MethodDelete,
			Pattern: "/subscriptions/:subId",
			APIFunc: s.apiDeleteEventExposureSubscription,
		},
		{
			Method:  http.MethodGet,
			Pattern: "/analytics",
			APIFunc: s.apiGetAnalyticsExposure,
		},
	}
}

// POST /nnef-eventexposure/v1/subscriptions
// AF subscribes to analytics events via NEF → NEF forwards to NWDAF
func (s *Server) apiPostEventExposureSubscription(gc *gin.Context) {
	logger.SBILog.Info("PostEventExposureSubscription")

	var subReq map[string]interface{}
	if err := gc.ShouldBindJSON(&subReq); err != nil {
		gc.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// Forward subscription to NWDAF
	body, _ := json.Marshal(subReq)
	resp, err := http.Post(
		NwdafAnalyticsURI+"/nnwdaf-eventssubscription/v1/subscriptions",
		"application/json",
		bytes.NewReader(body),
	)
	if err != nil {
		logger.SBILog.Errorf("Failed to subscribe to NWDAF: %v", err)
		gc.JSON(http.StatusBadGateway, gin.H{"error": "NWDAF unreachable"})
		return
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	gc.Data(resp.StatusCode, "application/json", respBody)
}

// GET /nnef-eventexposure/v1/subscriptions/:subId
func (s *Server) apiGetEventExposureSubscription(gc *gin.Context) {
	subId := gc.Param("subId")
	logger.SBILog.Infof("GetEventExposureSubscription - subId[%s]", subId)
	gc.JSON(http.StatusOK, gin.H{"subscriptionId": subId, "status": "active"})
}

// DELETE /nnef-eventexposure/v1/subscriptions/:subId
func (s *Server) apiDeleteEventExposureSubscription(gc *gin.Context) {
	subId := gc.Param("subId")
	logger.SBILog.Infof("DeleteEventExposureSubscription - subId[%s]", subId)

	// Forward delete to NWDAF
	req, _ := http.NewRequest("DELETE",
		NwdafAnalyticsURI+"/nnwdaf-eventssubscription/v1/subscriptions/"+subId, nil)
	http.DefaultClient.Do(req)

	gc.Status(http.StatusNoContent)
}

// GET /nnef-eventexposure/v1/analytics?event-type=...
// Query NWDAF analytics via NEF (Nnef_EventExposure → Nnwdaf_AnalyticsInfo)
func (s *Server) apiGetAnalyticsExposure(gc *gin.Context) {
	eventType := gc.DefaultQuery("event-type", "SERVICE_EXPERIENCE")
	logger.SBILog.Infof("GetAnalyticsExposure - eventType[%s]", eventType)

	// Forward to NWDAF AnalyticsInfo
	resp, err := http.Get(NwdafAnalyticsURI + "/nnwdaf-analyticsinfo/v1/analytics?event-type=" + eventType)
	if err != nil {
		logger.SBILog.Errorf("Failed to query NWDAF analytics: %v", err)
		gc.JSON(http.StatusBadGateway, gin.H{"error": "NWDAF unreachable"})
		return
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	gc.Data(resp.StatusCode, "application/json", respBody)
}
