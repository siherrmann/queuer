package model

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewTask(t *testing.T) {
	tests := []struct {
		name         string
		task         interface{}
		expName      string
		expInParams  int
		expOutParams int
		wantErr      bool
	}{
		{
			name:    "Valid task with no parameters",
			task:    func() {},
			expName: "github.com/siherrmann/queuer/model.TestNewTask.func1",
			wantErr: false,
		},
		{
			name:         "Task with input parameters",
			task:         func(a int, b string) {},
			expName:      "github.com/siherrmann/queuer/model.TestNewTask.func2",
			expInParams:  2,
			expOutParams: 0,
			wantErr:      false,
		},
		{
			name:         "Task with output parameters",
			task:         func() (int, string) { return 1, "test" },
			expName:      "github.com/siherrmann/queuer/model.TestNewTask.func3",
			expInParams:  0,
			expOutParams: 2,
			wantErr:      false,
		},
		{
			name:         "Task with multiple input and output parameters",
			task:         func(a int, b string, c float64) (bool, error) { return true, nil },
			expName:      "github.com/siherrmann/queuer/model.TestNewTask.func4",
			expInParams:  3,
			expOutParams: 2,
			wantErr:      false,
		},
		{
			name:    "Invalid task type",
			task:    "not a function",
			wantErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			task, err := NewTask(test.task)
			if test.wantErr {
				assert.Error(t, err, "NewTask should return an error for invalid options")
				assert.Nil(t, task, "Task should be nil for invalid options")
			} else {
				assert.NoError(t, err, "NewTask should not return an error for valid options")
				require.NotNil(t, task, "Task should not be nil for valid options")
				assert.Equal(t, test.expName, task.Name, "Task name should match the expected name")
				assert.Equal(t, test.expInParams, len(task.InputParameters), "Input parameters count should match expected")
				assert.Equal(t, test.expOutParams, len(task.OutputParameters), "Output parameters count should match expected")
			}
		})
	}
}

func TestNewTaskWithName(t *testing.T) {
	tests := []struct {
		name     string
		task     interface{}
		taskName string
		wantErr  bool
	}{
		{
			name:     "Valid task with valid name",
			task:     func() {},
			taskName: "ValidTask",
			wantErr:  false,
		},
		{
			name:     "Task with empty name",
			task:     func() {},
			taskName: "",
			wantErr:  true,
		},
		{
			name:     "Task with too long name",
			task:     func() {},
			taskName: "TaskMockWithNameLonger100_283032343638404244464850525456586062646668707274767880828486889092949698100",
			wantErr:  true,
		},
		{
			name:     "Invalid task type",
			task:     "not a function",
			taskName: "InvalidTask",
			wantErr:  true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			task, err := NewTaskWithName(test.task, test.taskName)
			if test.wantErr {
				assert.Error(t, err, "NewTask should return an error for invalid options")
				assert.Nil(t, task, "Task should be nil for invalid options")
			} else {
				assert.NoError(t, err, "NewTask should not return an error for valid options")
				require.NotNil(t, task, "Task should not be nil for valid options")
				assert.Equal(t, test.taskName, task.Name, "Task name should match the provided name")
			}
		})
	}
}
