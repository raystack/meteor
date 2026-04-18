package bigquery

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsExcludedByLabels(t *testing.T) {
	t.Run("should return false when no exclude labels configured", func(t *testing.T) {
		tableLabels := map[string]string{"env": "prod"}
		assert.False(t, IsExcludedByLabels(tableLabels, nil))
		assert.False(t, IsExcludedByLabels(tableLabels, map[string]string{}))
	})

	t.Run("should return true when table has a matching label", func(t *testing.T) {
		tableLabels := map[string]string{"env": "staging", "team": "data"}
		excludeLabels := map[string]string{"env": "staging"}
		assert.True(t, IsExcludedByLabels(tableLabels, excludeLabels))
	})

	t.Run("should return false when label key matches but value differs", func(t *testing.T) {
		tableLabels := map[string]string{"env": "prod"}
		excludeLabels := map[string]string{"env": "staging"}
		assert.False(t, IsExcludedByLabels(tableLabels, excludeLabels))
	})

	t.Run("should return false when table has no labels", func(t *testing.T) {
		excludeLabels := map[string]string{"env": "staging"}
		assert.False(t, IsExcludedByLabels(nil, excludeLabels))
		assert.False(t, IsExcludedByLabels(map[string]string{}, excludeLabels))
	})

	t.Run("should match any exclude label not all", func(t *testing.T) {
		tableLabels := map[string]string{"env": "staging"}
		excludeLabels := map[string]string{"env": "staging", "lifecycle": "ephemeral"}
		// Matches "env: staging" even though table doesn't have "lifecycle".
		assert.True(t, IsExcludedByLabels(tableLabels, excludeLabels))
	})
}
