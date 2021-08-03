# Changelog

All notable changes to this project will be documented in this file. See [standard-version](https://github.com/conventional-changelog/standard-version) for commit guidelines.

## 1.0.0 (2021-08-03)


### ⚠ BREAKING CHANGES

* add metadata model
* change store to factory (#121)

### Features

* add [POST] /v1/recipes to create recipe ([0c30e88](https://github.com/odpf/meteor/commit/0c30e889a8502c4092c46e6033d5b11d44ea5db0))
* add command to scaffold extractor ([#133](https://github.com/odpf/meteor/issues/133)) ([51aa51a](https://github.com/odpf/meteor/commit/51aa51a5331c9cfdd09db15548e9deef21e07ae4))
* add command to scaffold extractor ([#133](https://github.com/odpf/meteor/issues/133)) ([7e56812](https://github.com/odpf/meteor/commit/7e56812fe8a8b99d06b9edf1a267dc95a1305581))
* add config package ([93fb088](https://github.com/odpf/meteor/commit/93fb088167124b2b5dc5eaa21691a7c7e0b93f03))
* add Create method in recipe service ([c17a8d2](https://github.com/odpf/meteor/commit/c17a8d2d2b20cf7c0f80e94859f58c2df0b6f39f))
* add Dockertest for integration tests ([#126](https://github.com/odpf/meteor/issues/126)) ([1078fcf](https://github.com/odpf/meteor/commit/1078fcfafb591f239e542ec57222fa0fb2c7b660))
* add extractors package ([626736f](https://github.com/odpf/meteor/commit/626736f28225e8d2a798119a4c0dc016b4a2323a))
* add Find method in recipe service ([2298a4d](https://github.com/odpf/meteor/commit/2298a4d13e254cb449576c6b5eaeaf92e70d2624))
* add github user extractor ([#134](https://github.com/odpf/meteor/issues/134)) ([5fecf7a](https://github.com/odpf/meteor/commit/5fecf7a989ef4da6f1e945f23e6ecf27762540af))
* add github user extractor ([#134](https://github.com/odpf/meteor/issues/134)) ([349ff5f](https://github.com/odpf/meteor/commit/349ff5f5caaa7ae30d4f82a2c20a30f0b2d986bb))
* add json annotation to models ([d148384](https://github.com/odpf/meteor/commit/d148384917f2f4b79e3c04c7b5992ecdeeebebe8))
* add logger and extractor docs ([#127](https://github.com/odpf/meteor/issues/127)) ([1bce6f3](https://github.com/odpf/meteor/commit/1bce6f32977ce74d9c543335d18d590d7c29a13e))
* add meatdata extractor for Grafana ([#141](https://github.com/odpf/meteor/issues/141)) ([999a127](https://github.com/odpf/meteor/commit/999a127558b32cc1f8dbd971bde17894dc00fc0d))
* add meatdata extractor for Grafana ([#141](https://github.com/odpf/meteor/issues/141)) ([2da7756](https://github.com/odpf/meteor/commit/2da77560bf8ce6e64ea463a4df7d06bf01a10903))
* add metadata model ([8cd8885](https://github.com/odpf/meteor/commit/8cd8885b49271bd7aa5725101f9315278da646d2))
* add more extractors ([e82aa19](https://github.com/odpf/meteor/commit/e82aa19ef96653bf00958ce0fb06a8c9124f2ce6))
* add object storage for persisting recipes ([24d763c](https://github.com/odpf/meteor/commit/24d763cef3a0d4bbc51c9ff5acbbc89d59287fa1))
* add processors package ([4128bcf](https://github.com/odpf/meteor/commit/4128bcf1b7a90a56ea43cfd9d4fd3369fc80c47c))
* add recipe run handler ([3c11a1f](https://github.com/odpf/meteor/commit/3c11a1fa7aeb6678e81efbeda0dfc1ab2ec70558))
* add recipes package ([c613158](https://github.com/odpf/meteor/commit/c613158d22f02e67f8cb26864f94bcbcba705500))
* add run command and improve cli ux ([90eea5a](https://github.com/odpf/meteor/commit/90eea5a620895efba5278721935f3d7d3709e0fd))
* add runner package to run recipe ([f757ed2](https://github.com/odpf/meteor/commit/f757ed2ba591edaae6c2813bfd63b5db114eff2d))
* add sinks package and console sink ([dd0e64a](https://github.com/odpf/meteor/commit/dd0e64a745d429e5be5509560b1f11bcb457f478))
* add stremable extractor ([43fff35](https://github.com/odpf/meteor/commit/43fff3512cc8855f8de60deae7d6957e0ac271cb))
* add support for Bigtable metadata extraction ([#142](https://github.com/odpf/meteor/issues/142)) ([bbca91d](https://github.com/odpf/meteor/commit/bbca91d6c651b0593f47169b3daccfdbee7525dd))
* add support for Bigtable metadata extraction ([#142](https://github.com/odpf/meteor/issues/142)) ([f67d361](https://github.com/odpf/meteor/commit/f67d361ab9f208815c777080b938b633a5e10f93))
* adding column profile metadata in bigquery extractor ([#146](https://github.com/odpf/meteor/issues/146)) ([9259d19](https://github.com/odpf/meteor/commit/9259d195542861e8f1f7d9ca22f9b3ea7576444e)), closes [#1](https://github.com/odpf/meteor/issues/1)
* change store to factory ([#121](https://github.com/odpf/meteor/issues/121)) ([6459ba5](https://github.com/odpf/meteor/commit/6459ba595ffd729c7b5823c6f19db265dbd9809c))
* clickhouse extractor ([#138](https://github.com/odpf/meteor/issues/138)) ([7e7caf5](https://github.com/odpf/meteor/commit/7e7caf5f9325b56b33480f9d3e3f4adc2aa9e1ba))
* clickhouse extractor ([#138](https://github.com/odpf/meteor/issues/138)) ([3535352](https://github.com/odpf/meteor/commit/3535352ab96ae6194eb76056b3a6166aadca1e14))
* CSV extractor ([#140](https://github.com/odpf/meteor/issues/140)) ([5842715](https://github.com/odpf/meteor/commit/584271517c441e0a736f2d86d1f6ee99dfbda1b3))
* dynamic recipe value ([894b3bb](https://github.com/odpf/meteor/commit/894b3bbb1e98bad46e94446d37e9faf6c6c84a22))
* elasticsearch extractor ([#122](https://github.com/odpf/meteor/issues/122)) ([129f8af](https://github.com/odpf/meteor/commit/129f8afa66fddbdcf3758ba035ccc3cdd3a74901))
* elasticsearch extractor ([#122](https://github.com/odpf/meteor/issues/122)) ([d815039](https://github.com/odpf/meteor/commit/d8150391d0b9cc06e44b60a199c98f2eafe322a0))
* **extractors:** add bigquerydataset extractor ([998a80e](https://github.com/odpf/meteor/commit/998a80ea89f5d0f63cc1a4fbdd3f04c82dbbcb09))
* **extractors:** add bigquerytable extractor ([0beebb3](https://github.com/odpf/meteor/commit/0beebb37ef89f0270f16cdddaba88dfc2e8c725c))
* **extractors:** fetches kafka broker from config ([190979b](https://github.com/odpf/meteor/commit/190979b32f3dd2248cb005eda1f6c8c9c47905c0))
* full meteor re-write to support streamable payloads ([6d29f13](https://github.com/odpf/meteor/commit/6d29f13f8198cefd67dfb906c2c7b4dd7573daf8))
* Google Cloud Storage metadata extractor ([#144](https://github.com/odpf/meteor/issues/144)) ([5124f33](https://github.com/odpf/meteor/commit/5124f33818fd31329406237e7bc32a75005a88ef))
* Google Cloud Storage metadata extractor ([#144](https://github.com/odpf/meteor/issues/144)) ([d402bd3](https://github.com/odpf/meteor/commit/d402bd3713d76a3b6f2f8fe457b8ae7ae2269205))
* kafka sink should populate key from reflection ([#151](https://github.com/odpf/meteor/issues/151)) ([94685cc](https://github.com/odpf/meteor/commit/94685ccd1af624698dcda55eb969e65844e74000))
* kafka-sink baseline impl ([93a9d99](https://github.com/odpf/meteor/commit/93a9d99a8dee21b65b43a329fb0dc8a8b399dc91))
* modify extractors to stream data ([#149](https://github.com/odpf/meteor/issues/149)) ([ecc3c0d](https://github.com/odpf/meteor/commit/ecc3c0dc8d3d1dd919b7a44e8bbde810f72edbed))
* mongodb extractor ([#87](https://github.com/odpf/meteor/issues/87)) ([a48dbd5](https://github.com/odpf/meteor/commit/a48dbd5c2d4f7ed87b4364f36190694b4a647403))
* mssql extractor ([e57a76c](https://github.com/odpf/meteor/commit/e57a76c977de77c6ccc5fd4401132d7bc8efadff))
* mysql extractor ([#116](https://github.com/odpf/meteor/issues/116)) ([06d1db3](https://github.com/odpf/meteor/commit/06d1db38a8e9af4a3822b6929557b6589d5cc7fe))
* postgres extractor ([ffacf8f](https://github.com/odpf/meteor/commit/ffacf8f253fe40aa1d47a173cd93ff1cb11b3f38))
* processor plugin ([89f25ce](https://github.com/odpf/meteor/commit/89f25cef795796eef4f490c6c19459049f742f5e))
* run receipe from file ([6d89798](https://github.com/odpf/meteor/commit/6d897982a67fa94b811ee6dea3a4942e205ea3b7))
* setup router ([767b7ea](https://github.com/odpf/meteor/commit/767b7ea441474af46f3a384b37aa8732a0da1856))
* setup telemetry ([aa240c8](https://github.com/odpf/meteor/commit/aa240c8ec586f55b3bb9512330ce86a28d283d1d))
* upgrade golang version to 1.16 ([1377a9d](https://github.com/odpf/meteor/commit/1377a9dcf6b71f713de41c21d70f14043e3addad))
* validate payload using api docs ([cfc0590](https://github.com/odpf/meteor/commit/cfc05908f41000fe6d340e7c5b06057a76db5e5c))


### Bug Fixes

* fix error when statsd is disabled ([924f994](https://github.com/odpf/meteor/commit/924f99473a39cafce8983249725c54b21dcdf8f6))
* loading swagger file crashes due to relative path ([4fcc73d](https://github.com/odpf/meteor/commit/4fcc73d4bdeee7674d0c9a78be0c55e4e4d7d3c3))
* merge conflict ([1a2dd37](https://github.com/odpf/meteor/commit/1a2dd37a6d7f4f14af35b4f9f791f6f4672e1659))
* merge conflict ([5d862ce](https://github.com/odpf/meteor/commit/5d862ce0958ebb8ce138e85fb65b7d278915456c))
* not returning error on failed run ([e279a43](https://github.com/odpf/meteor/commit/e279a43f01af6d5fb93a3ecd6a1ca75c18ed8fdd))
* remove breaking code ([201433c](https://github.com/odpf/meteor/commit/201433cf1baea46b5781861086230e084697f68d))
* resolve conflict for docker-compose file ([00c3277](https://github.com/odpf/meteor/commit/00c3277f1a01b0f1ce35432aed35ae451e0de1e4))
* resolve stremable feature conflicts ([ae499c6](https://github.com/odpf/meteor/commit/ae499c69654a214f70619836d45cd21026e7c6b2))

# Changelog

All notable changes to this project will be documented in this file. See [standard-version](https://github.com/conventional-changelog/standard-version) for commit guidelines.
