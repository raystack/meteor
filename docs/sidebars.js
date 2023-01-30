module.exports = {
  docsSidebar: [
    'introduction',
    {
      type: "category",
      label: "Guides",
      items: [
        "guides/introduction",
        "guides/installation",
        "guides/list_Plugins",
        "guides/manage_recipes",
        "guides/run_recipes",
        "guides/deployment",
      ],
    },
    {
      type: "category",
      label: "Concepts",
      items: [
        "concepts/overview",
        "concepts/recipe",
        "concepts/source",
        "concepts/processor",
        "concepts/sink"
      ],
    },
    {
      type: "category",
      label: "Reference",
      items: [
        "reference/commands",
        "reference/configuration",
        "reference/metadata_models",
        "reference/extractors",
        "reference/processors",
        "reference/sinks"
      ],
    },
    {
      type: "category",
      label: "Contribute",
      items: [
        "contribute/guide",
        "contribute/contributing",
      ],
    },

  ],
};