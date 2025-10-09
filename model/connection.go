package model

// Connection represents a database connection with its details.
// It is used to monitor and manage active connections to the database.
type Connection struct {
	PID             int    `json:"pid"`
	Database        string `json:"database"`
	Username        string `json:"username"`
	ApplicationName string `json:"application_name"`
	Query           string `json:"query"`
	State           string `json:"state"`
}
