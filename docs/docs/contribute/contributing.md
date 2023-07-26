# Contribution Process

## BECOME A COMMITOR & CONTRIBUTE

We are always interested in adding new contributors. What we look for is a series of contributions, good taste, and an ongoing interest in the project.

- Committers will have write access to the Meteor repositories.
- There is no strict protocol for becoming a committer or PMC member. Candidates for new committers are typically people that are active contributors and community members.
- Candidates for new committers can also be suggested by current committers or PMC members.
- If you would like to become a committer, you should start contributing to Meteor in any of the ways mentioned. You might also want to talk to other committers and ask for their advice and guidance.

## WHAT CAN YOU DO?

- You can report a bug or suggest a feature enhancement or can just ask questions. Reach out on Github discussions for this purpose.
- You can modify the code
  - Add any new feature
  - Add new metadata extractors
  - Add new processors
  - Add new sinks
  - Improve Health and Monitoring Metrics
  - Update deprecated libraries or tools
- You can help with documenting new features or improve existing documentation.
- You can also review and accept other contributions if you are a Commitor.

## GUIDELINES

Please follow these practices for you change to get merged fast and smoothly:

- Contributions can only be accepted if they contain appropriate testing \(Unit and Integration Tests\).
- If you are introducing a completely new feature or making any major changes in an existing one, we recommend to start with an RFC and get consensus on the basic design first.
- Make sure your local build is running with all the tests and checkstyle passing.
- If your change is related to user-facing protocols / configurations, you need to make the corresponding change in the documentation as well.
- Docs live in the code repo under `docs` so that changes to that can be done in the same PR as changes to the code.
- Adding a new extractor should follow [this guide](https://github.com/raystack/meteor/tree/8cd8885b49271bd7aa5725101f9315278da646d2/docs/contribute/guide.md#adding-a-new-extractor).
- Adding a new processor should follow [this guide](https://github.com/raystack/meteor/tree/8cd8885b49271bd7aa5725101f9315278da646d2/docs/contribute/guide.md#adding-a-new-processor).
- Adding a new sink should follow [this guide](https://github.com/raystack/meteor/tree/8cd8885b49271bd7aa5725101f9315278da646d2/docs/contribute/guide.md#adding-a-new-sink).
