package model

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
)

type Options struct {
	OnError  *OnError
	Schedule *Schedule
}

func (c *Options) IsValid() error {
	if c.OnError != nil {
		if err := c.OnError.IsValid(); err != nil {
			return err
		}
	}
	if c.Schedule != nil {
		if err := c.Schedule.IsValid(); err != nil {
			return err
		}
	}
	return nil
}

func (c Options) Value() (driver.Value, error) {
	return c.Marshal()
}

func (c *Options) Scan(value interface{}) error {
	return c.Unmarshal(value)
}

func (r Options) Marshal() ([]byte, error) {
	return json.Marshal(r)
}

func (r *Options) Unmarshal(value interface{}) error {
	if o, ok := value.(Options); ok {
		*r = o
	} else {
		b, ok := value.([]byte)
		if !ok {
			return errors.New("type assertion to []byte failed")
		}
		return json.Unmarshal(b, r)
	}
	return nil
}
