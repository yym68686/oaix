package store

import _ "embed"

//go:embed usage_rollups.sql
var usageRollupsSQL string

//go:embed current_costs.sql
var currentCostsSQL string
