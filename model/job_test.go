package model

import (
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TaskMock is a mock function that simulates a task.
// It takes an integer duration and a string parameter, simulates some work,
// and returns an integer result and an error if the string cannot be converted to an integer.
func TaskMock(duration int, param2 string) (int, error) {
	// Simulate some work
	time.Sleep(time.Duration(duration) * time.Second)

	// Example for some error handling
	param2Int, err := strconv.Atoi(param2)
	if err != nil {
		return 0, err
	}

	return duration + param2Int, nil
}

// Tast with a name longer than 100 characters to test the validation logic.
func TaskMockWithNameLonger100_283032343638404244464850525456586062646668707274767880828486889092949698100() string {
	return "Uff"
}

func TestNewJob(t *testing.T) {
	tests := []struct {
		name       string
		task       interface{}
		options    *Options
		parameters []interface{}
		wantErr    bool
	}{
		{
			name:       "Valid nil options",
			task:       TaskMock,
			options:    nil,
			parameters: []interface{}{1, "2"},
			wantErr:    false,
		},
		{
			name: "Valid options",
			task: TaskMock,
			options: &Options{
				OnError: &OnError{
					MaxRetries:   3,
					RetryDelay:   1,
					RetryBackoff: RETRY_BACKOFF_NONE,
				},
				Schedule: &Schedule{
					Start:    time.Now().Add(1 * time.Minute),
					Interval: 2 * time.Minute,
					MaxCount: 5,
				},
			},
			parameters: []interface{}{1, "2"},
			wantErr:    false,
		},
		{
			name: "Valid OnError options",
			task: TaskMock,
			options: &Options{
				OnError: &OnError{
					MaxRetries:   3,
					RetryDelay:   1,
					RetryBackoff: RETRY_BACKOFF_NONE,
				},
			},
			parameters: []interface{}{1, "2"},
			wantErr:    false,
		},
		{
			name: "Invalid OnError options",
			task: TaskMock,
			options: &Options{
				OnError: &OnError{
					MaxRetries:   -3,
					RetryDelay:   1,
					RetryBackoff: RETRY_BACKOFF_NONE,
				},
			},
			parameters: []interface{}{1, "2"},
			wantErr:    true,
		},
		{
			name: "Valid Schedule options",
			task: TaskMock,
			options: &Options{
				Schedule: &Schedule{
					Start:    time.Now().Add(1 * time.Minute),
					Interval: 2 * time.Minute,
					MaxCount: 5,
				},
			},
			parameters: []interface{}{1, "2"},
			wantErr:    false,
		},
		{
			name: "Invalid Schedule options",
			task: TaskMock,
			options: &Options{
				Schedule: &Schedule{
					Start:    time.Now().Add(1 * time.Minute),
					Interval: -2 * time.Minute,
					MaxCount: 5,
				},
			},
			parameters: []interface{}{1, "2"},
			wantErr:    true,
		},
		{
			name:       "Invalid task with long name",
			task:       TaskMockWithNameLonger100_283032343638404244464850525456586062646668707274767880828486889092949698100,
			options:    nil,
			parameters: []interface{}{},
			wantErr:    true, // Expecting an error due to task name length
		},
		{
			name:       "Invalid task type",
			task:       123, // Invalid task type
			options:    nil,
			parameters: []interface{}{1, "2"},
			wantErr:    true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			job, err := NewJob(test.task, test.options, nil, test.parameters...)
			if test.wantErr {
				assert.Error(t, err, "NewJob should return an error for invalid options")
				assert.Nil(t, job, "Job should be nil for invalid options")
			} else {
				assert.NoError(t, err, "NewJob should not return an error for valid options")
				require.NotNil(t, job, "Job should not be nil for valid options")
				assert.Equal(t, test.options, job.Options, "Job options should match the provided options")
				assert.Equal(t, test.parameters, job.Parameters.ToInterfaceSlice(), "Job parameters should match the provided parameters")
			}
		})
	}
}

func TestJobFromNotificationToJob(t *testing.T) {
	now := time.Now()
	// Create a sample job
	jobFromNotification := &JobFromNotification{
		ID:        1,
		RID:       uuid.New(),
		WorkerID:  1,
		WorkerRID: uuid.New(),
		Options: &Options{
			OnError: &OnError{
				MaxRetries:   3,
				RetryDelay:   1,
				RetryBackoff: RETRY_BACKOFF_NONE,
			},
		},
		TaskName:    "TaskMock",
		Parameters:  Parameters{1, "2"},
		Status:      JobStatusQueued,
		ScheduledAt: DBTime{now},
		StartedAt:   DBTime{now},
		Attempts:    0,
		Results:     Parameters{3},
		Error:       "Error message",
		CreatedAt:   DBTime{now},
		UpdatedAt:   DBTime{now},
	}

	job := jobFromNotification.ToJob()
	assert.NotNil(t, job, "Converted job should not be nil")
	assert.Equal(t, jobFromNotification.ID, job.ID, "Job ID should match")
	assert.Equal(t, jobFromNotification.RID, job.RID, "Job RID should match")
	assert.Equal(t, jobFromNotification.WorkerID, job.WorkerID, "Job WorkerID should match")
	assert.Equal(t, jobFromNotification.WorkerRID, job.WorkerRID, "Job WorkerRID should match")
	assert.EqualValues(t, jobFromNotification.Options, job.Options, "Job Options should match")
	assert.Equal(t, jobFromNotification.TaskName, job.TaskName, "Job TaskName should match")
	assert.EqualValues(t, jobFromNotification.Parameters, job.Parameters, "Job Parameters should match")
	assert.Equal(t, jobFromNotification.Status, job.Status, "Job Status should match")
	assert.Equal(t, &jobFromNotification.ScheduledAt.Time, job.ScheduledAt, "Job ScheduledAt should match")
	assert.Equal(t, &jobFromNotification.StartedAt.Time, job.StartedAt, "Job StartedAt should match")
	assert.Equal(t, jobFromNotification.Attempts, job.Attempts, "Job Attempts should match")
	assert.Equal(t, jobFromNotification.Results, job.Results, "Job Result should match")
	assert.Equal(t, jobFromNotification.Error, job.Error, "Job Error should match")
	assert.Equal(t, jobFromNotification.CreatedAt.Time, job.CreatedAt, "Job CreatedAt should match")
	assert.Equal(t, jobFromNotification.UpdatedAt.Time, job.UpdatedAt, "Job UpdatedAt should match")
}

func TestMarshalAndUnmarshalParameters(t *testing.T) {
	params := &Parameters{1, "test", 3.14, true}

	bytes, err := params.Value()
	require.NoError(t, err, "Marshalling Parameters should not return an error")

	unmarshalledParams := &Parameters{}
	err = unmarshalledParams.Scan(bytes)
	require.NoError(t, err, "Unmarshaling Parameters should not return an error")
	assert.Equal(t, float64((*params)[0].(int)), (*unmarshalledParams)[0], "Unmarshaled Parameter int should match")
	assert.Equal(t, (*params)[1], (*unmarshalledParams)[1], "Unmarshaled Parameter string should match")
	assert.Equal(t, (*params)[2], (*unmarshalledParams)[2], "Unmarshaled Parameter float should match")
	assert.Equal(t, (*params)[3], (*unmarshalledParams)[3], "Unmarshaled Parameter bool should match")
}

func TestParametersToReflectValues(t *testing.T) {
	params := &Parameters{1, "test", 3.14, true}

	values := params.ToReflectValues()
	require.Len(t, values, 4, "ToReflectValues should return the correct number of values")
	assert.Equal(t, int64(1), values[0].Int(), "First value should be an int")
	assert.Equal(t, "test", values[1].String(), "Second value should be a string")
	assert.Equal(t, 3.14, values[2].Float(), "Third value should be a float64")
	assert.Equal(t, true, values[3].Bool(), "Fourth value should be a bool")
}

func TestMarshalAndUnmarshalDBTime(t *testing.T) {
	t.Run("Successfully marshal and unmarshal DBTime", func(t *testing.T) {
		// The time gets formatted to the database layout
		now, err := time.Parse(dbTimeLayout, time.Now().Format(dbTimeLayout))
		require.NoError(t, err, "Parsing current time should not return an error")
		dbTime := &DBTime{now}

		marshaled, err := dbTime.MarshalJSON()
		require.NoError(t, err, "Marshalling DBTime should not return an error")
		assert.NotEmpty(t, marshaled, "Marshaled DBTime should not be empty")

		var unmarshaled DBTime
		err = unmarshaled.UnmarshalJSON(marshaled)
		require.NoError(t, err, "Unmarshalling DBTime should not return an error")
		assert.Equal(t, dbTime.Time.Unix(), unmarshaled.Time.Unix(), "Unmarshaled DBTime should match the original time")
	})

	t.Run("Unmarshal null string as zero time", func(t *testing.T) {
		var dbTime DBTime
		err := dbTime.UnmarshalJSON([]byte("null"))
		require.NoError(t, err, "Unmarshalling empty string should not return an error")
		assert.True(t, dbTime.Time.IsZero(), "Unmarshaled DBTime should be zero time")
	})

	t.Run("Unmarshal invalid time format", func(t *testing.T) {
		var dbTime DBTime
		err := dbTime.UnmarshalJSON([]byte("\"invalid-time-format\""))
		assert.Error(t, err, "Unmarshalling invalid time format should return an error")
	})

	t.Run("Check if DBTime is set", func(t *testing.T) {
		dbTime := DBTime{time.Time{}}
		assert.False(t, dbTime.IsSet(), "DBTime should not be set initially")

		now := time.Now()
		dbTime.Time = now
		assert.True(t, dbTime.IsSet(), "DBTime should be set after assigning a time")
	})
}
