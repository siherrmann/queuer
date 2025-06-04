package helper

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A simple function with no parameters and no return values
func testFuncNoParamsNoReturn() {
	fmt.Println("No params, no return")
}

// A function with one string parameter
func testFuncOneParam(s string) {
	fmt.Println("One param:", s)
}

// A function with multiple parameters and a return value
func testFuncMultiParamsReturn(ctx context.Context, x int, y float64) (string, error) {
	_ = ctx // avoid unused warning
	return fmt.Sprintf("Result: %d, %f", x, y), nil
}

// A function with an unexported name (for GetTaskNameFromFunction)
func unexportedTestFunc() {
	fmt.Println("Unexported")
}

func TestGetTaskNameFromFunction(t *testing.T) {
	t.Run("Valid function", func(t *testing.T) {
		name, err := GetTaskNameFromFunction(testFuncNoParamsNoReturn)
		assert.NoError(t, err)
		assert.Contains(t, name, "helper.testFuncNoParamsNoReturn", "should return full function name")
	})

	t.Run("Valid unexported function", func(t *testing.T) {
		name, err := GetTaskNameFromFunction(unexportedTestFunc)
		assert.NoError(t, err)
		assert.Contains(t, name, "helper.unexportedTestFunc", "should return full unexported function name")
	})

	t.Run("Not a function (string)", func(t *testing.T) {
		name, err := GetTaskNameFromFunction("not_a_func")
		require.Error(t, err)
		assert.Empty(t, name, "should return empty string for non-function")
		assert.Contains(t, err.Error(), "task must be a function, got string", "error message should indicate wrong type")
	})

	t.Run("Not a function (int)", func(t *testing.T) {
		name, err := GetTaskNameFromFunction(123)
		require.Error(t, err)
		assert.Empty(t, name, "should return empty string for non-function")
		assert.Contains(t, err.Error(), "task must be a function, got int", "error message should indicate wrong type")
	})

	t.Run("Not a function (struct)", func(t *testing.T) {
		type MyStruct struct{}
		name, err := GetTaskNameFromFunction(MyStruct{})
		require.Error(t, err)
		assert.Empty(t, name, "should return empty string for non-function")
		assert.Contains(t, err.Error(), "task must be a function, got struct", "error message should indicate wrong type")
	})
}

func TestGetTaskNameFromInterface(t *testing.T) {
	t.Run("Input is a string", func(t *testing.T) {
		expectedName := "MyCustomTaskName"
		name, err := GetTaskNameFromInterface(expectedName)
		assert.NoError(t, err)
		assert.Equal(t, expectedName, name, "should return the string directly if input is string")
	})

	t.Run("Input is a function", func(t *testing.T) {
		name, err := GetTaskNameFromInterface(testFuncOneParam)
		assert.NoError(t, err)
		assert.Contains(t, name, "helper.testFuncOneParam", "should return function name if input is a function")
	})

	t.Run("Input is neither string nor function (int)", func(t *testing.T) {
		name, err := GetTaskNameFromInterface(123)
		require.Error(t, err)
		assert.Empty(t, name, "should return empty string for non-string, non-function")
		assert.Contains(t, err.Error(), "task must be a function, got int", "error message should indicate wrong type")
	})

	t.Run("Input is neither string nor function (struct)", func(t *testing.T) {
		type MyStruct struct{}
		name, err := GetTaskNameFromInterface(MyStruct{})
		require.Error(t, err)
		assert.Empty(t, name, "should return empty string for non-string, non-function")
		assert.Contains(t, err.Error(), "task must be a function, got struct", "error message should indicate wrong type")
	})
}

func TestCheckValidTask(t *testing.T) {
	t.Run("Valid function", func(t *testing.T) {
		err := CheckValidTask(testFuncNoParamsNoReturn)
		assert.NoError(t, err, "should not return error for a valid function")
	})

	t.Run("Not a function (string)", func(t *testing.T) {
		err := CheckValidTask("not_a_func")
		require.Error(t, err, "should return error for non-function")
		assert.Contains(t, err.Error(), "task must be a function, got string", "error message should indicate wrong type")
	})

	t.Run("Not a function (nil)", func(t *testing.T) {
		var f func()
		err := CheckValidTask(f)
		require.Error(t, err, "should return error for nil function")
		assert.Contains(t, err.Error(), "task value must not be nil", "error message should indicate invalid kind for nil func")
	})
}

func TestCheckValidTaskWithParameters(t *testing.T) {
	t.Run("Valid function with matching parameters", func(t *testing.T) {
		err := CheckValidTaskWithParameters(testFuncOneParam, "hello")
		assert.NoError(t, err, "should not return error for matching params")

		err = CheckValidTaskWithParameters(testFuncMultiParamsReturn, context.Background(), 10, 3.14)
		assert.NoError(t, err, "should not return error for multiple matching params")
	})

	t.Run("Not a function", func(t *testing.T) {
		err := CheckValidTaskWithParameters("not_a_func", "param1")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "task must be a function, got string", "should fail if task is not a function")
	})

	t.Run("Function with too few parameters provided", func(t *testing.T) {
		err := CheckValidTaskWithParameters(testFuncOneParam)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "task expects 1 parameters, got 0", "should fail if too few parameters")
	})

	t.Run("Function with too many parameters provided", func(t *testing.T) {
		err := CheckValidTaskWithParameters(testFuncOneParam, "hello", "world")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "task expects 1 parameters, got 2", "should fail if too many parameters")
	})

	t.Run("Function with wrong parameter type (single param)", func(t *testing.T) {
		err := CheckValidTaskWithParameters(testFuncOneParam, 123)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "parameter 0 of task must be of type string, got int", "should fail if wrong parameter type")
	})

	t.Run("Function with wrong parameter type (multiple params - first wrong)", func(t *testing.T) {
		err := CheckValidTaskWithParameters(testFuncMultiParamsReturn, "wrong", 10, 3.14)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "parameter 0 of task must be of type interface, got string", "should fail on first wrong type")
	})

	t.Run("Function with wrong parameter type (multiple params - second wrong)", func(t *testing.T) {
		err := CheckValidTaskWithParameters(testFuncMultiParamsReturn, context.Background(), "wrong", 3.14)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "parameter 1 of task must be of type int, got string", "should fail on second wrong type")
	})

	t.Run("Function with different parameter type (time.Duration vs int)", func(t *testing.T) {
		f := func(d time.Duration) {}
		err := CheckValidTaskWithParameters(f, 10)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "parameter 0 of task must be of type int64, got int", "should distinguish similar underlying types")
	})

	t.Run("Function with interface parameter (expecting specific type)", func(t *testing.T) {
		f := func(i interface{}) {}
		err := CheckValidTaskWithParameters(f, "hello")
		assert.NoError(t, err, "should allow any type for interface{} parameter")
	})
}

func TestGetInputParametersFromTask(t *testing.T) {
	t.Run("Function with no parameters", func(t *testing.T) {
		params, err := GetInputParametersFromTask(testFuncNoParamsNoReturn)
		assert.NoError(t, err)
		assert.Empty(t, params, "should return empty slice for no parameters")
		assert.Len(t, params, 0, "slice length should be 0")
	})

	t.Run("Function with one parameter", func(t *testing.T) {
		params, err := GetInputParametersFromTask(testFuncOneParam)
		assert.NoError(t, err)
		assert.Len(t, params, 1, "slice length should be 1")
		assert.Equal(t, reflect.TypeOf(""), params[0], "first parameter should be string type")
	})

	t.Run("Function with multiple parameters", func(t *testing.T) {
		params, err := GetInputParametersFromTask(testFuncMultiParamsReturn)
		assert.NoError(t, err)
		assert.Len(t, params, 3, "slice length should be 3")
		assert.Equal(t, reflect.TypeOf((*context.Context)(nil)).Elem(), params[0], "first parameter should be context.Context type")
		assert.Equal(t, reflect.TypeOf(0), params[1], "second parameter should be int type")
		assert.Equal(t, reflect.TypeOf(0.0), params[2], "third parameter should be float64 type")
	})
}

func TestGetOutputParametersFromTask(t *testing.T) {
	t.Run("Function with no return values", func(t *testing.T) {
		params, err := GetOutputParametersFromTask(testFuncNoParamsNoReturn)
		assert.NoError(t, err)
		assert.Empty(t, params, "should return empty slice for no return values")
		assert.Len(t, params, 0, "slice length should be 0")
	})

	t.Run("Function with one return value (string)", func(t *testing.T) {
		f := func() string { return "" }
		params, err := GetOutputParametersFromTask(f)
		assert.NoError(t, err)
		assert.Len(t, params, 1, "slice length should be 1")
		assert.Equal(t, reflect.TypeOf(""), params[0], "first return should be string type")
	})

	t.Run("Function with multiple return values (string, error)", func(t *testing.T) {
		params, err := GetOutputParametersFromTask(testFuncMultiParamsReturn)
		assert.NoError(t, err)
		assert.Len(t, params, 2, "slice length should be 2")
		assert.Equal(t, reflect.TypeOf(""), params[0], "first return should be string type")
		assert.Equal(t, reflect.TypeOf((*error)(nil)).Elem(), params[1], "second return should be error type")
	})
}
