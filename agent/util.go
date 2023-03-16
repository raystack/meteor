package agent

import (
	"github.com/goto/meteor/plugins"
	"github.com/goto/meteor/recipe"
)

func recipeToPluginConfig(pr recipe.PluginRecipe) plugins.Config {
	return plugins.Config{
		URNScope:  pr.Scope,
		RawConfig: pr.Config,
	}
}
