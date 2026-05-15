package sbi

import (
	"net/http"

	"github.com/free5gc/nef/internal/logger"
	"github.com/free5gc/openapi"
	"github.com/free5gc/util/metrics/sbi"
	"github.com/gin-gonic/gin"
)

// getAsSessionWithQoSRoutes returns routes for the 3GPP AsSessionWithQoS API.
// TS 29.122 §5.6 / TS 29.522 — AF-facing northbound API for QoS session management.
// The NEF translates these into Npcf_PolicyAuthorization requests towards the PCF.
func (s *Server) getAsSessionWithQoSRoutes() []Route {
	return []Route{
		{
			Method:  http.MethodGet,
			Pattern: "/:afID/subscriptions",
			APIFunc: s.apiGetQoSSubscriptions,
		},
		{
			Method:  http.MethodPost,
			Pattern: "/:afID/subscriptions",
			APIFunc: s.apiPostQoSSubscription,
		},
		{
			Method:  http.MethodGet,
			Pattern: "/:afID/subscriptions/:subID",
			APIFunc: s.apiGetIndividualQoSSubscription,
		},
		{
			Method:  http.MethodPatch,
			Pattern: "/:afID/subscriptions/:subID",
			APIFunc: s.apiPatchQoSSubscription,
		},
		{
			Method:  http.MethodDelete,
			Pattern: "/:afID/subscriptions/:subID",
			APIFunc: s.apiDeleteQoSSubscription,
		},
	}
}

func (s *Server) apiGetQoSSubscriptions(gc *gin.Context) {
	s.Processor().GetQoSSubscriptions(gc, gc.Param("afID"))
}

func (s *Server) apiPostQoSSubscription(gc *gin.Context) {
	reqBody, err := gc.GetRawData()
	if err != nil {
		logger.SBILog.Errorf("Get Request Body error: %+v", err)
		pd := openapi.ProblemDetailsSystemFailure(err.Error())
		gc.Set(sbi.IN_PB_DETAILS_CTX_STR, pd.Cause)
		gc.JSON(http.StatusInternalServerError, pd)
		return
	}

	s.Processor().PostQoSSubscription(gc, gc.Param("afID"), reqBody)
}

func (s *Server) apiGetIndividualQoSSubscription(gc *gin.Context) {
	s.Processor().GetIndividualQoSSubscription(gc, gc.Param("afID"), gc.Param("subID"))
}

func (s *Server) apiPatchQoSSubscription(gc *gin.Context) {
	reqBody, err := gc.GetRawData()
	if err != nil {
		logger.SBILog.Errorf("Get Request Body error: %+v", err)
		pd := openapi.ProblemDetailsSystemFailure(err.Error())
		gc.Set(sbi.IN_PB_DETAILS_CTX_STR, pd.Cause)
		gc.JSON(http.StatusInternalServerError, pd)
		return
	}

	s.Processor().PatchQoSSubscription(gc, gc.Param("afID"), gc.Param("subID"), reqBody)
}

func (s *Server) apiDeleteQoSSubscription(gc *gin.Context) {
	s.Processor().DeleteQoSSubscription(gc, gc.Param("afID"), gc.Param("subID"))
}
