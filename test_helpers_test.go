package simple_json_db_cache

import (
	"os"
	"testing"
)

func withTempWorkingDir(t *testing.T) string {
	t.Helper()

	oldWD, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd failed: %v", err)
	}

	tempDir := t.TempDir()
	if err := os.Chdir(tempDir); err != nil {
		t.Fatalf("Chdir to temp dir failed: %v", err)
	}

	t.Cleanup(func() {
		if chdirErr := os.Chdir(oldWD); chdirErr != nil {
			t.Fatalf("restore working directory failed: %v", chdirErr)
		}
	})

	return tempDir
}
