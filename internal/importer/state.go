package importer

type JobStatus string

const (
	JobQueued    JobStatus = "queued"
	JobRunning   JobStatus = "running"
	JobCompleted JobStatus = "completed"
	JobFailed    JobStatus = "failed"
	JobCanceled  JobStatus = "canceled"
)

type ItemStatus string

const (
	ItemQueued     ItemStatus = "queued"
	ItemValidating ItemStatus = "validating"
	ItemValidated  ItemStatus = "validated"
	ItemPublishing ItemStatus = "publishing"
	ItemPublished  ItemStatus = "published"
	ItemSkipped    ItemStatus = "skipped"
	ItemFailed     ItemStatus = "failed"
	ItemCanceled   ItemStatus = "canceled"
)

func (s JobStatus) Terminal() bool {
	return s == JobCompleted || s == JobFailed || s == JobCanceled
}

func (s ItemStatus) Terminal() bool {
	return s == ItemPublished || s == ItemSkipped || s == ItemFailed || s == ItemCanceled
}

func CanTransitionJob(from, to JobStatus) bool {
	if from == to {
		return true
	}
	if from.Terminal() {
		return false
	}
	switch from {
	case JobQueued:
		return to == JobRunning || to == JobCanceled || to == JobFailed
	case JobRunning:
		return to == JobCompleted || to == JobFailed || to == JobCanceled || to == JobQueued
	default:
		return false
	}
}

func CanTransitionItem(from, to ItemStatus) bool {
	if from == to {
		return true
	}
	if from.Terminal() {
		return false
	}
	switch from {
	case ItemQueued:
		return to == ItemValidating || to == ItemSkipped || to == ItemFailed || to == ItemCanceled
	case ItemValidating:
		return to == ItemValidated || to == ItemFailed || to == ItemQueued || to == ItemCanceled
	case ItemValidated:
		return to == ItemPublishing || to == ItemSkipped || to == ItemFailed || to == ItemCanceled
	case ItemPublishing:
		return to == ItemPublished || to == ItemFailed || to == ItemQueued || to == ItemCanceled
	default:
		return false
	}
}
