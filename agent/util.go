package agent

import (
	"github.com/raystack/meteor/plugins"
	"github.com/raystack/meteor/recipe"
)

func recipeToPluginConfig(pr recipe.PluginRecipe) plugins.Config {
	return plugins.Config{
		URNScope:  pr.Scope,
		RawConfig: pr.Config,
	}
}
