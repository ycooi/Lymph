package main

import "testing"

func TestEnvBool(t *testing.T) {
	t.Setenv("LYMPH_TEST_BOOL", "true")
	value, err := envBool("LYMPH_TEST_BOOL", false)
	if err != nil || !value {
		t.Fatalf("true value: got %v, %v", value, err)
	}

	t.Setenv("LYMPH_TEST_BOOL", "not-a-bool")
	if _, err := envBool("LYMPH_TEST_BOOL", false); err == nil {
		t.Fatal("invalid boolean was accepted")
	}

	t.Setenv("LYMPH_TEST_BOOL", "")
	value, err = envBool("LYMPH_TEST_BOOL", true)
	if err != nil || !value {
		t.Fatalf("default value: got %v, %v", value, err)
	}
}
