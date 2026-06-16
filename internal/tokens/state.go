package tokens

type StateTransition string

const (
	TransitionActiveToCooling  StateTransition = "active_to_cooling"
	TransitionActiveToDisabled StateTransition = "active_to_disabled"
	TransitionCoolingToActive  StateTransition = "cooling_to_active"
	TransitionDisabledToActive StateTransition = "disabled_to_active"
)

type CooldownReason string

const (
	CooldownReasonRateLimit CooldownReason = "rate_limit"
	CooldownReasonUpstream  CooldownReason = "upstream_error"
	CooldownReasonManual    CooldownReason = "manual"
)

type DisabledReason string

const (
	DisabledReasonUnauthorized DisabledReason = "unauthorized"
	DisabledReasonForbidden    DisabledReason = "forbidden"
	DisabledReasonManual       DisabledReason = "manual"
)
