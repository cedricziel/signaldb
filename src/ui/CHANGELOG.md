# Changelog

## [0.3.0](https://github.com/cedricziel/signaldb/compare/signaldb-ui-v0.2.2...signaldb-ui-v0.3.0) (2026-10-03)


### ⚠ BREAKING CHANGES

* **traceql:** `Condition` has a new public `op` field, so code that constructs one must set it.

### Features

* **common:** merge discovery fields with the type authority's canonical types ([#2074](https://github.com/cedricziel/signaldb/issues/2074)) ([36b7266](https://github.com/cedricziel/signaldb/commit/36b7266ec9442a10aaec8b7eb45d654b6cb27ec7))
* **evals:** build eval sets from traces ([#1847](https://github.com/cedricziel/signaldb/issues/1847)) ([dfe9e60](https://github.com/cedricziel/signaldb/commit/dfe9e608f9629abee1a542f5020f164d193b8faf))
* **evals:** eval sets pages, upload dialog and saving regressions in the UI ([#1871](https://github.com/cedricziel/signaldb/issues/1871)) ([cb05278](https://github.com/cedricziel/signaldb/commit/cb05278065450372e37f51fc149f4ac04a6ad6a7))
* **evals:** list and compare eval runs from MCP and the CLI ([#1866](https://github.com/cedricziel/signaldb/issues/1866)) ([665f458](https://github.com/cedricziel/signaldb/commit/665f4588808aad9ae567df358c7732f70881aec9))
* **evals:** upload eval results and gate CI on them ([#1857](https://github.com/cedricziel/signaldb/issues/1857)) ([175155a](https://github.com/cedricziel/signaldb/commit/175155a3a2d958d59e16a05961206c0b09f1e79a))
* **processors:** diff Test panel output against the server's decoded input ([#1883](https://github.com/cedricziel/signaldb/issues/1883)) ([7711bc5](https://github.com/cedricziel/signaldb/commit/7711bc5bca22d010fb83766449d52ccdd44291e0))
* **querier:** add histogram_avg, histogram_stddev and histogram_stdvar ([#2187](https://github.com/cedricziel/signaldb/issues/2187)) ([626a417](https://github.com/cedricziel/signaldb/commit/626a41777f5393eb364a00b9fa211cd674f2eb09))
* **query-ir:** correlate to another signal with semi and anti joins (irVersion 11) ([#2052](https://github.com/cedricziel/signaldb/issues/2052)) ([ad0cd64](https://github.com/cedricziel/signaldb/commit/ad0cd644101af8f140dd7ae89fdc09f94dab9ca2))
* **query-ir:** differential flamegraph over a baseline window (irVersion 13) ([#2100](https://github.com/cedricziel/signaldb/issues/2100)) ([5e36ddb](https://github.com/cedricziel/signaldb/commit/5e36ddbea6b114c7c2077eca2ee28b9b0fa8089f))
* **query-ir:** trace result envelope (irVersion 12) ([#2063](https://github.com/cedricziel/signaldb/issues/2063)) ([934b6bc](https://github.com/cedricziel/signaldb/commit/934b6bccf5d5fdb12595c07191804091664ae145))
* recognise AI agents as the gen_ai.agent entity ([#1803](https://github.com/cedricziel/signaldb/issues/1803)) ([e0fcaa0](https://github.com/cedricziel/signaldb/commit/e0fcaa0b3cd8133e23e0ccc8f9e28e43986c41f5))
* **router:** eval sets API for offline agent evals ([#1837](https://github.com/cedricziel/signaldb/issues/1837)) ([b3ce35a](https://github.com/cedricziel/signaldb/commit/b3ce35a7fb9671494e4d78cd29b2673f20205531))
* **router:** list discovery fields with their canonical authority type ([#2075](https://github.com/cedricziel/signaldb/issues/2075)) ([621aa46](https://github.com/cedricziel/signaldb/commit/621aa4674399a628470b73810e033963288a3866))
* **router:** live-tail Query IR rows and trace results (IR v15) ([#2148](https://github.com/cedricziel/signaldb/issues/2148)) ([57ac3c7](https://github.com/cedricziel/signaldb/commit/57ac3c74646037f2198b86f787da3b7a161d6ffd))
* **router:** paginate Query IR rows and trace results (IR v14) ([#2142](https://github.com/cedricziel/signaldb/issues/2142)) ([67023cc](https://github.com/cedricziel/signaldb/commit/67023cc0f71ab345f23d6278f9a917ee3d349941))
* **router:** publish the Query IR stage grammar as typed OpenAPI schemas ([#2088](https://github.com/cedricziel/signaldb/issues/2088)) ([55e79d8](https://github.com/cedricziel/signaldb/commit/55e79d8fdcb6b5703d1d66801477ae7c714ce6e3))
* **router:** publish UI login/logout and the full whoami response in OpenAPI ([#2110](https://github.com/cedricziel/signaldb/issues/2110)) ([664c8fe](https://github.com/cedricziel/signaldb/commit/664c8fe52a51505ef5fc46a63293e1e35a1a1913))
* **router:** report the retention that applies to a query ([#2182](https://github.com/cedricziel/signaldb/issues/2182)) ([91ca14d](https://github.com/cedricziel/signaldb/commit/91ca14dfc6f4478155ef9095c1b84e4078a6da90))
* **router:** scalar result envelope and metric Series labels ([#2001](https://github.com/cedricziel/signaldb/issues/2001)) ([2d72dc1](https://github.com/cedricziel/signaldb/commit/2d72dc1b719ee880ad87101fc042d21465f3affc))
* **router:** type the Query IR request pipeline as IrStage ([#2095](https://github.com/cedricziel/signaldb/issues/2095)) ([414ed92](https://github.com/cedricziel/signaldb/commit/414ed92b136367e2650b1c8de8f0cd12250cce5b))
* **router:** warn match_incomplete_trace from the query report trailer ([#2090](https://github.com/cedricziel/signaldb/issues/2090)) ([7ee6f46](https://github.com/cedricziel/signaldb/commit/7ee6f464d3512df3a2c4fddc06f90302c3c55b16))
* **schema-registry:** accept definition/2 custom registry uploads ([#1823](https://github.com/cedricziel/signaldb/issues/1823)) ([a8c8196](https://github.com/cedricziel/signaldb/commit/a8c819648bd5778478be6e86241802ae4f6f880f))
* **schema:** let registry metric definitions declare aliases ([#2188](https://github.com/cedricziel/signaldb/issues/2188)) ([0817836](https://github.com/cedricziel/signaldb/commit/08178369ba741145ea42dc6ff3aeb2517e2ad1bc))
* show span links in the Query IR, MCP get_trace and the trace view ([#2177](https://github.com/cedricziel/signaldb/issues/2177)) ([b873eab](https://github.com/cedricziel/signaldb/commit/b873eabee7f7e4e836a530a3fc75fe5261cafb49))
* **traceql:** support !=, =~ and !~ and surface search errors over MCP ([#2178](https://github.com/cedricziel/signaldb/issues/2178)) ([ce929f5](https://github.com/cedricziel/signaldb/commit/ce929f5f321e5d826e2ba43edc67d7e26de67659))
* **ui:** add a Connect dialog to the app shell for MCP, CLI and API access ([#2093](https://github.com/cedricziel/signaldb/issues/2093)) ([0539ecf](https://github.com/cedricziel/signaldb/commit/0539ecfebdd29201ef44dca7d9d7d0e9dee2eb9c))
* **ui:** add platform-aware labels to the Real users page ([#1923](https://github.com/cedricziel/signaldb/issues/1923)) ([1fb60b6](https://github.com/cedricziel/signaldb/commit/1fb60b6088f40e8dd4ce85acf8958d0594701f03))
* **ui:** add Query IR reads for the Real users page ([#1842](https://github.com/cedricziel/signaldb/issues/1842)) ([a6cf3f8](https://github.com/cedricziel/signaldb/commit/a6cf3f820acd48fb203119df145af843a13e49ea))
* **ui:** add RUM network and resources IR queries ([#1889](https://github.com/cedricziel/signaldb/issues/1889)) ([8e71d12](https://github.com/cedricziel/signaldb/commit/8e71d12de0f08a96cea5335789e24fa82439ba10))
* **ui:** add RUM per-route views/vitals/errors query builders ([#1896](https://github.com/cedricziel/signaldb/issues/1896)) ([c04f0b8](https://github.com/cedricziel/signaldb/commit/c04f0b8673aac14b987a4a2c0e996d8788e596ad))
* **ui:** add RUM route-detail query builders ([#1897](https://github.com/cedricziel/signaldb/issues/1897)) ([4897b31](https://github.com/cedricziel/signaldb/commit/4897b31666870202b663a760ae698c61d6e4f33d))
* **ui:** add RUM traced-request KPIs and the Frontend → backend panel ([#1891](https://github.com/cedricziel/signaldb/issues/1891)) ([85481b0](https://github.com/cedricziel/signaldb/commit/85481b0b32f5a4261bc09cf3e3dfaf9cd5ede1c5))
* **ui:** add the Errors tab detail's queries and hooks ([#1921](https://github.com/cedricziel/signaldb/issues/1921)) ([34b70f5](https://github.com/cedricziel/signaldb/commit/34b70f5c2f2214946ef2a711b61d36261f817d31))
* **ui:** add the Pages tab's route detail panel ([#1899](https://github.com/cedricziel/signaldb/issues/1899)) ([01428a3](https://github.com/cedricziel/signaldb/commit/01428a3c210258b30f18549688ff06cd727ae8db))
* **ui:** add the Real users Errors tab ([#1920](https://github.com/cedricziel/signaldb/issues/1920)) ([9b424bd](https://github.com/cedricziel/signaldb/commit/9b424bd21bfb96a79aee31b218a3d1ed2047468d))
* **ui:** add the Real users Errors tab selected-group detail UI ([#1922](https://github.com/cedricziel/signaldb/issues/1922)) ([849eb71](https://github.com/cedricziel/signaldb/commit/849eb71cdd581103a180fb3a282192c4babf9a90))
* **ui:** add the Real users Interactions tab ([#1900](https://github.com/cedricziel/signaldb/issues/1900)) ([734715f](https://github.com/cedricziel/signaldb/commit/734715f02c8c2e8b4d1cedfcf02cd54017564ce4))
* **ui:** add the Real users Network tab ([#1890](https://github.com/cedricziel/signaldb/issues/1890)) ([cc2e1c8](https://github.com/cedricziel/signaldb/commit/cc2e1c888ada02bc62d9f0f664f8f18f8f541cab))
* **ui:** add the Real users page ([#1850](https://github.com/cedricziel/signaldb/issues/1850)) ([0edde84](https://github.com/cedricziel/signaldb/commit/0edde8455f4bd312951c7bbeb986fd74b28c0264))
* **ui:** add the Real users Pages tab's route list ([#1898](https://github.com/cedricziel/signaldb/issues/1898)) ([2ecf4a6](https://github.com/cedricziel/signaldb/commit/2ecf4a6ea0369cd5e305a2d6f94708205afcc8bb))
* **ui:** add the Real users session detail view assembly ([#1915](https://github.com/cedricziel/signaldb/issues/1915)) ([5a45629](https://github.com/cedricziel/signaldb/commit/5a45629f55bf1e547afed9c34c36192ff112bdba))
* **ui:** add the Real users sessions list query ([#1910](https://github.com/cedricziel/signaldb/issues/1910)) ([f570467](https://github.com/cedricziel/signaldb/commit/f570467092b1909ba461dc0e04e6e3ac5cdbbf34))
* **ui:** add the Real users Sessions tab list ([#1912](https://github.com/cedricziel/signaldb/issues/1912)) ([2dee9d5](https://github.com/cedricziel/signaldb/commit/2dee9d502c074cd040375798697e843f68e43265))
* **ui:** add the session detail lane timeline component ([#1914](https://github.com/cedricziel/signaldb/issues/1914)) ([3b54260](https://github.com/cedricziel/signaldb/commit/3b542600393d435560d76b07c2ae0642be9d1943))
* **ui:** add the session detail's exception panel ([#1917](https://github.com/cedricziel/signaldb/issues/1917)) ([6a57710](https://github.com/cedricziel/signaldb/commit/6a57710ed494a62b93af2be681abbe24584ed2fb))
* **ui:** add the Sessions list's free-text filter ([#1911](https://github.com/cedricziel/signaldb/issues/1911)) ([224502e](https://github.com/cedricziel/signaldb/commit/224502e0dbb936bb68a12b9134a8e3deb9aa882f))
* **ui:** AppShell component, synced to Claude Design ([#1839](https://github.com/cedricziel/signaldb/issues/1839)) ([41c3312](https://github.com/cedricziel/signaldb/commit/41c331209f798a463c9b480096db2fd10a844013))
* **ui:** complete the Setup tab's browser instrumentation snippets ([#1925](https://github.com/cedricziel/signaldb/issues/1925)) ([50056ab](https://github.com/cedricziel/signaldb/commit/50056abd43e97c452e1b967b0f6f548641ecd012))
* **ui:** Evaluate section for offline agent evals ([#1830](https://github.com/cedricziel/signaldb/issues/1830)) ([4779181](https://github.com/cedricziel/signaldb/commit/477918102436070fbbc0f06670ea15c51098fe29))
* **ui:** load more rows of an Explore IR query page by page ([#2145](https://github.com/cedricziel/signaldb/issues/2145)) ([e011352](https://github.com/cedricziel/signaldb/commit/e01135294ab1c4e636848645769c027fd5d81930))
* **ui:** open a session from a pasted id in the command palette ([#1918](https://github.com/cedricziel/signaldb/issues/1918)) ([d0d702a](https://github.com/cedricziel/signaldb/commit/d0d702aed5a98f7c937afad3df29279643a83ffc))
* **ui:** read every metric type from the metrics source ([#1944](https://github.com/cedricziel/signaldb/issues/1944)) ([918d7ab](https://github.com/cedricziel/signaldb/commit/918d7ab05edae9ae58f844892cc859df444f5c9f))
* **ui:** recover from an expired reverse-proxy login ([#1804](https://github.com/cedricziel/signaldb/issues/1804)) ([a60aa4b](https://github.com/cedricziel/signaldb/commit/a60aa4b445cb998f6340e4337b94cb0ab64ae90b))
* **ui:** route login, logout and whoami through the generated client ([#2111](https://github.com/cedricziel/signaldb/issues/2111)) ([40a5735](https://github.com/cedricziel/signaldb/commit/40a5735ea0b72cbc1080f768bd6d8180a33c1287))
* **ui:** RUM self-instrumentation (route template, browser identity, clicks) ([#1836](https://github.com/cedricziel/signaldb/issues/1836)) ([d1a8655](https://github.com/cedricziel/signaldb/commit/d1a865590235912ab4cfa15f7530a236473229fe))
* **ui:** show a request's backend trace in the session detail ([#1916](https://github.com/cedricziel/signaldb/issues/1916)) ([49419b4](https://github.com/cedricziel/signaldb/commit/49419b46aa514b11b0a39d068c3a5c6502e2c81c))
* **ui:** tail the Logs list in live mode instead of re-running the window ([#2151](https://github.com/cedricziel/signaldb/issues/2151)) ([deaf256](https://github.com/cedricziel/signaldb/commit/deaf25620cabc285dd248c0e75d544c696273a52))
* **ui:** theming, responsive layout and navigation fixes from the UI audit ([#1846](https://github.com/cedricziel/signaldb/issues/1846)) ([64c1ba9](https://github.com/cedricziel/signaldb/commit/64c1ba9bcfbf45552daa2ae82e2371dd16b807c0))
* **ui:** touch-resizable panes and an overflow cue on the Overview map ([#1884](https://github.com/cedricziel/signaldb/issues/1884)) ([0404236](https://github.com/cedricziel/signaldb/commit/0404236559294f6ee5dc0e4aaccc055ff9e53b4a))


### Bug Fixes

* **querier:** IR range aggregates difference each series against itself ([#2009](https://github.com/cedricziel/signaldb/issues/2009)) ([4a1cbc4](https://github.com/cedricziel/signaldb/commit/4a1cbc4a7a9ff455de450f8db419f76496f60440))
* **query-ir:** keep the newest flamegraph profiles and reject inverted windows ([#2098](https://github.com/cedricziel/signaldb/issues/2098)) ([dda9bad](https://github.com/cedricziel/signaldb/commit/dda9bad2c86140ee9be861ee0e4cd5f2788cf8a9))
* **router:** read data for sample:true and flag partial discovery statistics ([#2176](https://github.com/cedricziel/signaldb/issues/2176)) ([ab5dcf7](https://github.com/cedricziel/signaldb/commit/ab5dcf78f47567a6e2a8f8a10dcf5dfd06cdfed5))
* **ui:** always show PWA updates, and recover from a crashed stale build ([#1795](https://github.com/cedricziel/signaldb/issues/1795)) ([5817557](https://github.com/cedricziel/signaldb/commit/58175578f2dfb8769aaf0818789016d31d1b0d7a))
* **ui:** anchor phone nav overlays below the top bar ([#2160](https://github.com/cedricziel/signaldb/issues/2160)) ([7fbef44](https://github.com/cedricziel/signaldb/commit/7fbef4434c8289c33c41748341f22c4171c07b73))
* **ui:** clean up visual issues from the Storybook page audit ([#1876](https://github.com/cedricziel/signaldb/issues/1876)) ([33af317](https://github.com/cedricziel/signaldb/commit/33af31767794d2d6611715bd9dcd8fefb446346e))
* **ui:** clear the Real users page's route/session/error-group selection when switching apps ([#1924](https://github.com/cedricziel/signaldb/issues/1924)) ([cbf01cf](https://github.com/cedricziel/signaldb/commit/cbf01cfb19113d20851a92a4baf7e01d4c12353b))
* **ui:** correct colour and label semantics in catalog, errors and overview ([#1875](https://github.com/cedricziel/signaldb/issues/1875)) ([1db984b](https://github.com/cedricziel/signaldb/commit/1db984b56fe6075d9b038081326e9a00d46515fd))
* **ui:** correct data and chart glitches on the eval pages ([#1882](https://github.com/cedricziel/signaldb/issues/1882)) ([f1b5aaa](https://github.com/cedricziel/signaldb/commit/f1b5aaa08b17df23fdeb78ef42d44ee48278f09f))
* **ui:** finish the Processors pages and clean up the test-run diff ([#1873](https://github.com/cedricziel/signaldb/issues/1873)) ([62f7e25](https://github.com/cedricziel/signaldb/commit/62f7e2555d607c5ab907c08cefdf8f5aec5f4db6))
* **ui:** hold data queries until the session is known ([#2127](https://github.com/cedricziel/signaldb/issues/2127)) ([42acfde](https://github.com/cedricziel/signaldb/commit/42acfdefeeb4f0a357f5f9517824137ffe660422))
* **ui:** pass the Setup tab's log exporter as an options object ([#1953](https://github.com/cedricziel/signaldb/issues/1953)) ([746a678](https://github.com/cedricziel/signaldb/commit/746a67876ce60a98e7c229330d29708e70ec478a))
* **ui:** polish the Metrics view chart, legend and query builder ([#1874](https://github.com/cedricziel/signaldb/issues/1874)) ([f537b51](https://github.com/cedricziel/signaldb/commit/f537b51aadd1ddeba8e51bec0896c6b941cee02c))
* **ui:** polish the Traces view from the Storybook audit ([#1877](https://github.com/cedricziel/signaldb/issues/1877)) ([d7f38de](https://github.com/cedricziel/signaldb/commit/d7f38de76309a5826ded1ba6826acb2ac5eea253))
* **ui:** responsive tables and page-story checks at every width ([#1853](https://github.com/cedricziel/signaldb/issues/1853)) ([665b576](https://github.com/cedricziel/signaldb/commit/665b576a69a3066eb965c61c6fcbc4b996d37616))
* **ui:** stop RUM formula names colliding with their query names ([#1952](https://github.com/cedricziel/signaldb/issues/1952)) ([225e9ca](https://github.com/cedricziel/signaldb/commit/225e9cad6a9556ec884bc77b0f1898528ef66f11))
* **ui:** treat the all-zero parent span id as no parent in trace detail ([#1799](https://github.com/cedricziel/signaldb/issues/1799)) ([a62fce2](https://github.com/cedricziel/signaldb/commit/a62fce2aadbbd657762d6012c1a90093bffc9d3e))


### Documentation

* describe PromQL execution through the Query IR ([#2042](https://github.com/cedricziel/signaldb/issues/2042)) ([3375b34](https://github.com/cedricziel/signaldb/commit/3375b344ff68b1c6f1b546bf36f7de00dd508357))
* **openspec:** archive real-user-monitoring with what shipped ([#1878](https://github.com/cedricziel/signaldb/issues/1878)) ([9c3ede8](https://github.com/cedricziel/signaldb/commit/9c3ede8869da4e696a30529384c4362e04b9f985))


### Code Refactoring

* **agents:** delegate implementation to oss:coder ([#1796](https://github.com/cedricziel/signaldb/issues/1796)) ([57403d8](https://github.com/cedricziel/signaldb/commit/57403d8880cd4a99fc9f131869a8f58d084728ef))
* **ui:** build Query IR stages with the generated Ir* types ([#2094](https://github.com/cedricziel/signaldb/issues/2094)) ([962ebd4](https://github.com/cedricziel/signaldb/commit/962ebd4a26a2f9aa88db3ed35c28d2e15974702d))


### Tests

* **ui:** add app-scoped RUM error groups and backend-cause batch ([#1919](https://github.com/cedricziel/signaldb/issues/1919)) ([5a88ca8](https://github.com/cedricziel/signaldb/commit/5a88ca815a222c430a7035535b00f134193e54af))
* **ui:** add failing tests for session detail lane/event merging ([#1913](https://github.com/cedricziel/signaldb/issues/1913)) ([ead67be](https://github.com/cedricziel/signaldb/commit/ead67be892be38db65a70f4a6e2b07f937a05316))
* **ui:** add per-tab Connect stories and register the dialog for design-sync ([#2106](https://github.com/cedricziel/signaldb/issues/2106)) ([625ca5b](https://github.com/cedricziel/signaldb/commit/625ca5b77d3fae1acffd555bb03b34900a0547d1))
* **ui:** give the RUM inline trace waterfall test room for its wait ([#2020](https://github.com/cedricziel/signaldb/issues/2020)) ([1437fc5](https://github.com/cedricziel/signaldb/commit/1437fc57b9167de1cd27f3c464dca8ecb7b9fbf0))
* **ui:** give the RUM session-detail tests room for their waits ([#2004](https://github.com/cedricziel/signaldb/issues/2004)) ([ef9bd17](https://github.com/cedricziel/signaldb/commit/ef9bd17d4712a3842e8019ca4885f9bebcb4d06d))
* **ui:** let every RUM Sessions test wait for the tab to settle ([#2037](https://github.com/cedricziel/signaldb/issues/2037)) ([be6da37](https://github.com/cedricziel/signaldb/commit/be6da376263e06bf8ed48c19cdff2c616b7976d5))
* **ui:** wait for the RUM app list before clicking session timeline marks ([#2031](https://github.com/cedricziel/signaldb/issues/2031)) ([a767dbe](https://github.com/cedricziel/signaldb/commit/a767dbef6e4f9bd8ab7f4f68c22cc1116e75600c))
* **ui:** wait for the RUM timeline to remount before selecting an event ([#2036](https://github.com/cedricziel/signaldb/issues/2036)) ([6ec67a5](https://github.com/cedricziel/signaldb/commit/6ec67a53adef7981eaf6fb0f7178180bb1d4e3c9))


### Build System

* **deps-dev:** bump typescript-eslint from 8.67.0 to 8.70.1 ([#2115](https://github.com/cedricziel/signaldb/issues/2115)) ([278a6b5](https://github.com/cedricziel/signaldb/commit/278a6b5b3d76a9118ba5dea3336e13446d9cafea))
* **deps:** bump @opentelemetry/api-logs from 0.221.0 to 0.222.0 ([#2114](https://github.com/cedricziel/signaldb/issues/2114)) ([696c396](https://github.com/cedricziel/signaldb/commit/696c39667eed08f19cbdda651d241ca9a4e6e23e))

## [0.2.2](https://github.com/cedricziel/signaldb/compare/signaldb-ui-v0.2.1...signaldb-ui-v0.2.2) (2026-09-23)


### Features

* demo mode and a TrueNAS demo app with a trimmed OpenTelemetry Demo ([#1632](https://github.com/cedricziel/signaldb/issues/1632)) ([d6da0cf](https://github.com/cedricziel/signaldb/commit/d6da0cfb53d79b8167d92e0c3689aea65323d97b))
* GitHub App integration for connecting a tenant's repositories ([#1600](https://github.com/cedricziel/signaldb/issues/1600)) ([6c9721e](https://github.com/cedricziel/signaldb/commit/6c9721ef0cf630df85e227a07be6ae30ee263191))
* **logs:** filter on an attribute's real dotted key from the explore UI ([#1594](https://github.com/cedricziel/signaldb/issues/1594)) ([523633b](https://github.com/cedricziel/signaldb/commit/523633b43096658ff884886134753e783500e1e6))
* multi-tenant MCP OAuth grants ([#1541](https://github.com/cedricziel/signaldb/issues/1541)) ([c5b49b0](https://github.com/cedricziel/signaldb/commit/c5b49b018f749a72b639366a18223081cecef7cc))
* per-API-key allowed origins for browser (CORS) ingestion ([#1548](https://github.com/cedricziel/signaldb/issues/1548)) ([6e966dd](https://github.com/cedricziel/signaldb/commit/6e966ddaf2740e3648583223828c6af715b6d331))
* per-tenant, per-dataset OTTL telemetry processors ([#1603](https://github.com/cedricziel/signaldb/issues/1603)) ([2fc1022](https://github.com/cedricziel/signaldb/commit/2fc102232b1d925418b02e68393af8917184016e))
* **router:** attach an existing GitHub App installation to a tenant ([#1618](https://github.com/cedricziel/signaldb/issues/1618)) ([0ab8e95](https://github.com/cedricziel/signaldb/commit/0ab8e95581f5213b02e8cded8af5b2b71c827516))
* **router:** serialize API timestamps as native UTC DateTime ([#1643](https://github.com/cedricziel/signaldb/issues/1643)) ([1327fae](https://github.com/cedricziel/signaldb/commit/1327fae5760510f7e2180ab9e323ba6961d8b657))
* source context for stack frames from linked GitHub repositories ([#1601](https://github.com/cedricziel/signaldb/issues/1601)) ([acca49c](https://github.com/cedricziel/signaldb/commit/acca49ca770ab96464b12676211144bc20b5cc7c))
* **ui:** add a refresh button next to the time picker ([#1610](https://github.com/cedricziel/signaldb/issues/1610)) ([3b722df](https://github.com/cedricziel/signaldb/commit/3b722dfa1bd5428eaa724ba74445aadc9bf63bd1))
* **ui:** declutter attribute lists and add entity pivots in the explore views ([#1593](https://github.com/cedricziel/signaldb/issues/1593)) ([2189701](https://github.com/cedricziel/signaldb/commit/2189701fcb5f51faccaeb82ad4506d49172a91bf))
* **ui:** degrade catalog entity identity per source ([#1626](https://github.com/cedricziel/signaldb/issues/1626)) ([af19554](https://github.com/cedricziel/signaldb/commit/af1955432513b740c1aed02d0876bf93c42d2a3b))
* **ui:** guard every navigation against unsaved edits and clear AA on the light accent ([#1591](https://github.com/cedricziel/signaldb/issues/1591)) ([fb2361f](https://github.com/cedricziel/signaldb/commit/fb2361f646c848ae4c1d1cf2ca1d80eb3b0550e4))
* **ui:** install Storybook and publish it to GitHub Pages ([#1639](https://github.com/cedricziel/signaldb/issues/1639)) ([55813aa](https://github.com/cedricziel/signaldb/commit/55813aa29645ce11c1a449e0e3a7e3759bc87b52))
* **ui:** live ingest verification and a deferred service-worker update ([#1587](https://github.com/cedricziel/signaldb/issues/1587)) ([a351a29](https://github.com/cedricziel/signaldb/commit/a351a297c7975c9944ed82fb9809b297790c6e76))
* **ui:** make the Explore UI installable as a PWA ([#1555](https://github.com/cedricziel/signaldb/issues/1555)) ([6510a1d](https://github.com/cedricziel/signaldb/commit/6510a1d49ffdcfda59016bf677d713fdf8a29615))
* **ui:** move the explore UI onto the query IR ([#1627](https://github.com/cedricziel/signaldb/issues/1627)) ([c20ad3e](https://github.com/cedricziel/signaldb/commit/c20ad3e6a91e43ba01c201c6c37faafd376d6b6d))
* **ui:** page stories and shared components for Storybook ([#1649](https://github.com/cedricziel/signaldb/issues/1649)) ([bb588bb](https://github.com/cedricziel/signaldb/commit/bb588bbb1605de0eb4d1788da6b1f246d6292a1e))
* **ui:** page stories for the admin, Processors and Schema screens ([#1666](https://github.com/cedricziel/signaldb/issues/1666)) ([392da0e](https://github.com/cedricziel/signaldb/commit/392da0e61d99f69c2f9eb21e3f7861110a0b63c9))
* **ui:** page stories for the Metrics, Profiles, Errors, Catalog and Query tabs ([#1661](https://github.com/cedricziel/signaldb/issues/1661)) ([a497290](https://github.com/cedricziel/signaldb/commit/a497290ca11a6a2dd87a5c41ffe94c15fed1f1da))
* **ui:** stories for the app shell, login and consent screens ([#1659](https://github.com/cedricziel/signaldb/issues/1659)) ([42dbac0](https://github.com/cedricziel/signaldb/commit/42dbac0b1da5db03ad0bc380cd15797eb051b71e))


### Bug Fixes

* **auth:** slide browser session expiry forward on activity ([#1578](https://github.com/cedricziel/signaldb/issues/1578)) ([a8cfad3](https://github.com/cedricziel/signaldb/commit/a8cfad3d853e024510b0f82975f5c9971d428b68))
* **ui:** chart keyboard access, tooltip placement, palette and metric tile sizing ([#1585](https://github.com/cedricziel/signaldb/issues/1585)) ([406028b](https://github.com/cedricziel/signaldb/commit/406028b0b3814ceef13497e5c05f76eb1f280524))
* **ui:** cover the consent route with the unsaved-changes guard and let the editor's own redirects through ([#1592](https://github.com/cedricziel/signaldb/issues/1592)) ([b2e7d09](https://github.com/cedricziel/signaldb/commit/b2e7d09fc6daf7cac03a0fd1e85487b9823838d4))
* **ui:** explore UI flow breakages, layout defects and responsive arrangement ([#1584](https://github.com/cedricziel/signaldb/issues/1584)) ([ce69231](https://github.com/cedricziel/signaldb/commit/ce69231444221f7b335e8d0d93b229041587b18b))
* **ui:** live facet counts, span-to-profile unit and absent identity filters ([#1586](https://github.com/cedricziel/signaldb/issues/1586)) ([57bf4f1](https://github.com/cedricziel/signaldb/commit/57bf4f1e2878bc080f4bcc10847a5a3302d0ed8c))
* **ui:** make the top-bar logo a link back to Explore ([#1542](https://github.com/cedricziel/signaldb/issues/1542)) ([6c5652a](https://github.com/cedricziel/signaldb/commit/6c5652a91fdc15a6b9071a9505c49a156cc72b88))
* **ui:** roving focus follows the pointer and stays in range, signed axis compaction, distinct palette ([#1589](https://github.com/cedricziel/signaldb/issues/1589)) ([cbb0e8c](https://github.com/cedricziel/signaldb/commit/cbb0e8c2b415e91c1c627b4a670a6c17f93d76b1))
* **ui:** stop nanosToMs truncating sub-millisecond durations to zero ([#1545](https://github.com/cedricziel/signaldb/issues/1545)) ([946e1f8](https://github.com/cedricziel/signaldb/commit/946e1f8cb93f298527b2f247c59f591da9fb8514))
* **ui:** stop the default dataset badge duplicating its own id ([#1546](https://github.com/cedricziel/signaldb/issues/1546)) ([b99d359](https://github.com/cedricziel/signaldb/commit/b99d35950352fcd7c2e572bbc50d3f64ee4d5c91))
* **ui:** stop the PWA service worker from swallowing the GitHub OAuth callback ([#1612](https://github.com/cedricziel/signaldb/issues/1612)) ([f5c9e38](https://github.com/cedricziel/signaldb/commit/f5c9e38d5acac58803e4d1cffa59da51650ad02a))
* **ui:** surface query failures and hung requests on Traces and Catalog ([#1544](https://github.com/cedricziel/signaldb/issues/1544)) ([66ed94a](https://github.com/cedricziel/signaldb/commit/66ed94ac93c24bf3ab07343ef95ab868e5adedd2))


### Styles

* **ui:** one visual vocabulary for gutters, tables, headings, buttons, chips, empty states and errors ([#1590](https://github.com/cedricziel/signaldb/issues/1590)) ([9af6070](https://github.com/cedricziel/signaldb/commit/9af607038b279139c0a073a98d6f912faacff5c1))


### Code Refactoring

* remove cross-crate dead code ([#1647](https://github.com/cedricziel/signaldb/issues/1647)) ([8b5b1d9](https://github.com/cedricziel/signaldb/commit/8b5b1d98f1150e75a8306beea29bb90465a4f921))

## [0.2.1](https://github.com/cedricziel/signaldb/compare/signaldb-ui-v0.2.0...signaldb-ui-v0.2.1) (2026-09-12)


### Features

* **auth:** OIDC login (relying-party SSO) ([#1485](https://github.com/cedricziel/signaldb/issues/1485)) ([c681bee](https://github.com/cedricziel/signaldb/commit/c681bee369d9a1b636357edf70b6f88f236b96a2))
* **compactor:** keep a bounded value sketch so discovery can suggest values ([#1329](https://github.com/cedricziel/signaldb/issues/1329)) ([dd64a3d](https://github.com/cedricziel/signaldb/commit/dd64a3dd8a8846499ac75bea818ba938c6ca9a87))
* dedicated login page with a login-configuration probe ([#1484](https://github.com/cedricziel/signaldb/issues/1484)) ([d536466](https://github.com/cedricziel/signaldb/commit/d53646688a580256711f0534ae7ed526c58a769a))
* implement multi-dataset restriction for API keys and OAuth grants ([#1475](https://github.com/cedricziel/signaldb/issues/1475)) ([11deba9](https://github.com/cedricziel/signaldb/commit/11deba995c6937324576f87e87284a1580faa624))
* **router:** serve query discovery from the registry and statistics ([#1312](https://github.com/cedricziel/signaldb/issues/1312)) ([41d2738](https://github.com/cedricziel/signaldb/commit/41d27384df6e90bd9e9731218e084dd27581e20b))
* **schema-registry:** accept keys= batch resolution on GET /api/v1/schema/metrics ([#1508](https://github.com/cedricziel/signaldb/issues/1508)) ([6facbdc](https://github.com/cedricziel/signaldb/commit/6facbdcd182285bf54c1d2e922724d6bdeb6bae6))
* self-serve connection details for agents ([public] config, /api/v1/connection, MCP connection_info) ([#1474](https://github.com/cedricziel/signaldb/issues/1474)) ([ad78cd1](https://github.com/cedricziel/signaldb/commit/ad78cd1981282426b65b7dcac50ddc38eeea7f80))
* **ui:** chart an entity's metrics in the catalog ([#1368](https://github.com/cedricziel/signaldb/issues/1368)) ([deba724](https://github.com/cedricziel/signaldb/commit/deba7241690f4f6390c1da806abd19e78e116c17))
* **ui:** discover catalog entities from the schema registry across every signal ([#1350](https://github.com/cedricziel/signaldb/issues/1350)) ([5e6f67d](https://github.com/cedricziel/signaldb/commit/5e6f67d1fec16f6286f43398f854496a75b53d80))
* **ui:** make the frontend responsive across shell, explore, and screen widgets ([#1438](https://github.com/cedricziel/signaldb/issues/1438)) ([7f79d30](https://github.com/cedricziel/signaldb/commit/7f79d3093c72fadc360ed514638faaae63f05219))
* **ui:** one Dialog shell and inline confirmation for destructive actions ([#1466](https://github.com/cedricziel/signaldb/issues/1466)) ([c68be01](https://github.com/cedricziel/signaldb/commit/c68be0120f19a52a891201ce3afe5dadc91411c1))
* **ui:** remove the old login modal, redirect to /login instead ([#1539](https://github.com/cedricziel/signaldb/issues/1539)) ([7dc4fd8](https://github.com/cedricziel/signaldb/commit/7dc4fd88983efa5265f510644b4257d831012c35))


### Bug Fixes

* **auth:** remove the dataset_id legacy shims from multi-dataset-key-restriction ([#1480](https://github.com/cedricziel/signaldb/issues/1480)) ([e8c85de](https://github.com/cedricziel/signaldb/commit/e8c85dedc0a9a73c5a133b952e858603d78c0c36))
* **query-ir:** stop an unknown group-by field from answering silently ([#1301](https://github.com/cedricziel/signaldb/issues/1301)) ([b4f8464](https://github.com/cedricziel/signaldb/commit/b4f8464f71192f80d407f81e8bd837efd8fafd79))
* **ui:** align catalog cache keys and resolve entity identity from the schema ([#1354](https://github.com/cedricziel/signaldb/issues/1354)) ([0cc9910](https://github.com/cedricziel/signaldb/commit/0cc99105bfe4cc3ab0a48c9e23eacb0b20a48525))
* **ui:** pre-check all ingest scopes on API key creation ([#1440](https://github.com/cedricziel/signaldb/issues/1440)) ([cb26cfb](https://github.com/cedricziel/signaldb/commit/cb26cfb642e15af662d102c2509c0f5482bd3f3d))
* **ui:** resolve a catalog detail page's entity type from the observed set ([#1366](https://github.com/cedricziel/signaldb/issues/1366)) ([3f76e1d](https://github.com/cedricziel/signaldb/commit/3f76e1db18acfdc6bc0dde8c351838a58dc23b2f))
* **ui:** resolve empty screen after login and add a dedicated /login route ([#1473](https://github.com/cedricziel/signaldb/issues/1473)) ([13feff8](https://github.com/cedricziel/signaldb/commit/13feff808f7378515475994df1348581ee874693))
* **ui:** shared sort headers, class collisions, dead tokens, toolbar wrap, dev proxy ([#1455](https://github.com/cedricziel/signaldb/issues/1455)) ([b5060ea](https://github.com/cedricziel/signaldb/commit/b5060ea94f79db4055aa77ca4baf1c63dc6c464b))
* **ui:** span-detail drawer and small-screen rules for the explore toolbars ([#1468](https://github.com/cedricziel/signaldb/issues/1468)) ([e63761a](https://github.com/cedricziel/signaldb/commit/e63761a563773508699c30b6e68cddac6204dacf))


### Code Refactoring

* **ui:** one error alert, skeleton loaders and empty-state wording across the explore views ([#1465](https://github.com/cedricziel/signaldb/issues/1465)) ([0b702cd](https://github.com/cedricziel/signaldb/commit/0b702cd6de330caf3774a644ad0ce4de418af62b))
* **ui:** one resizer, one attribute-key combobox, one note class ([#1463](https://github.com/cedricziel/signaldb/issues/1463)) ([70aed7e](https://github.com/cedricziel/signaldb/commit/70aed7ec1968cfcf253e66e86ccd4fc8538846cb))
* **ui:** table header base rule, shared scroll wrapper, soft flame fills ([#1462](https://github.com/cedricziel/signaldb/issues/1462)) ([e30840f](https://github.com/cedricziel/signaldb/commit/e30840f9599af357aa87139d17c3943fd88adfa8))

## [0.2.0](https://github.com/cedricziel/signaldb/compare/signaldb-ui-v0.1.2...signaldb-ui-v0.2.0) (2026-08-17)


### ⚠ BREAKING CHANGES

* **auth:** POST /api/v1/admin/tenants/{id}/api-keys requires a non-empty `scopes` array; bodies without it are rejected.
* **cli+mcp:** signaldb-cli tenant/api-key/dataset commands move under `admin` (e.g. `signaldb-cli admin tenant list`), and queries now require a language flag (`signaldb-cli query --sql|--promql|--logql|--traceql|--ir`). No back-compat aliases are provided (post-1.0).

### Features

* add an Errors & Exceptions tab ([#1167](https://github.com/cedricziel/signaldb/issues/1167)) ([79f3749](https://github.com/cedricziel/signaldb/commit/79f374916a8add7aa47abd0c8569e13c560a2d7c))
* **api:** code-first OpenAPI — generate spec + Rust/TS clients from annotations ([#856](https://github.com/cedricziel/signaldb/issues/856)) ([e34fbfb](https://github.com/cedricziel/signaldb/commit/e34fbfbd094034416f78597c59b306975dd97271))
* **api:** document Tempo trace query endpoints in OpenAPI + SDK ([#861](https://github.com/cedricziel/signaldb/issues/861)) ([a1e0d7f](https://github.com/cedricziel/signaldb/commit/a1e0d7f9f3c355f8bf73da686db1952487c3e046))
* **auth:** schema:read/schema:write API-key scopes, scopes on every key surface ([#1217](https://github.com/cedricziel/signaldb/issues/1217)) ([34c7a28](https://github.com/cedricziel/signaldb/commit/34c7a28e4e62fad7a05089c1a3543739d6e28450))
* **auth:** tenant:manage API-key scope for the tenant management API ([#1266](https://github.com/cedricziel/signaldb/issues/1266)) ([9dfc193](https://github.com/cedricziel/signaldb/commit/9dfc193a85e813b42f8658bf97cbfd30e3b78f2e))
* **cli+mcp:** CLI & MCP as pure SDK consumers — query --&lt;lang&gt;, admin grouping (Phase 1) ([#892](https://github.com/cedricziel/signaldb/issues/892)) ([92a439e](https://github.com/cedricziel/signaldb/commit/92a439e112da96029733d93db7f274c20c29cbc5))
* compute the traces group table on the server, via a scoped IR aggregate ([#1092](https://github.com/cedricziel/signaldb/issues/1092)) ([ec5c284](https://github.com/cedricziel/signaldb/commit/ec5c284cbe57c0ce34da7f295f08502de2493b82))
* **logs:** surface trace_id/span_id in log query responses ([#1048](https://github.com/cedricziel/signaldb/issues/1048)) ([5a84a04](https://github.com/cedricziel/signaldb/commit/5a84a04b3582befd76ea5f231b887f2cbed253ea))
* **mcp-admin-tool-parity:** platform-admin and tenant self-management tool/CLI parity ([#1261](https://github.com/cedricziel/signaldb/issues/1261)) ([1eadc72](https://github.com/cedricziel/signaldb/commit/1eadc728ace70aff10fa01aaa8766012ace2df4c))
* **mcp:** OAuth 2.1 + DCR connector support for Claude and OpenAI ([#899](https://github.com/cedricziel/signaldb/issues/899)) ([4d0104a](https://github.com/cedricziel/signaldb/commit/4d0104a608ee392e9b25acf686dcd7359fc37631))
* metric/label discovery (MCP+CLI+SDK) and prom/loki UI migration ([#1041](https://github.com/cedricziel/signaldb/issues/1041)) ([afcc72e](https://github.com/cedricziel/signaldb/commit/afcc72e9f87a45e74c97171e8919b90868cd54f4))
* native Query IR — versioned structured query surface (query-ir-core) ([#882](https://github.com/cedricziel/signaldb/issues/882)) ([8774ac0](https://github.com/cedricziel/signaldb/commit/8774ac0fbbe4686cb7aa8b0bba73dbc25f185689))
* **query-ir:** add v2 heatmaps ([#1102](https://github.com/cedricziel/signaldb/issues/1102)) ([96184cf](https://github.com/cedricziel/signaldb/commit/96184cf42809a4cbf0e4a15f592cb544dbb7a597))
* **query-ir:** flamegraph result envelope for profiles ([#1144](https://github.com/cedricziel/signaldb/issues/1144)) ([394407f](https://github.com/cedricziel/signaldb/commit/394407f72756b15c97cb6ce6efcf01ce0b61b33b))
* Real trace-context parenting for documentLoad + complementary log-record telemetry ([#1117](https://github.com/cedricziel/signaldb/issues/1117)) ([43a7c63](https://github.com/cedricziel/signaldb/commit/43a7c63a42a55aed11df304387d286f4bb5bccb9))
* retry throttled requests in every SignalDB client ([#1260](https://github.com/cedricziel/signaldb/issues/1260)) ([3342dcc](https://github.com/cedricziel/signaldb/commit/3342dcced2cbc489adc7bf5076a0c9059b805adb))
* return server trace context and timings on HTTP responses (Server-Timing + traceresponse) ([#918](https://github.com/cedricziel/signaldb/issues/918)) ([453dd20](https://github.com/cedricziel/signaldb/commit/453dd2050eee95f3daf1c96f77e56964e99a2bb1))
* **router:** Pyroscope OpenAPI parity (CLI/MCP/UI/SDK) ([#1268](https://github.com/cedricziel/signaldb/issues/1268)) ([2b54e2d](https://github.com/cedricziel/signaldb/commit/2b54e2d693801a0bfd9afdf4e982abfac6efc955))
* **router:** schema registry API under /api/v1/schema ([#1219](https://github.com/cedricziel/signaldb/issues/1219)) ([71af424](https://github.com/cedricziel/signaldb/commit/71af424a0d96eb3f87198af4c4213bb89106cf28))
* **sdk:** query surface — SDK covers PromQL/LogQL/TraceQL + Flight SQL (Phase 0) ([#890](https://github.com/cedricziel/signaldb/issues/890)) ([1fde946](https://github.com/cedricziel/signaldb/commit/1fde946cc308ef134f01492b72a3fc874e1c8f95))
* **self-monitoring:** runtime-configurable browser telemetry export ([#842](https://github.com/cedricziel/signaldb/issues/842)) ([343b928](https://github.com/cedricziel/signaldb/commit/343b92877d1291406de25923e671ab2a54a98028))
* signal rate-limit throttling with Retry-After and a generous default burst ([#1256](https://github.com/cedricziel/signaldb/issues/1256)) ([5584f3f](https://github.com/cedricziel/signaldb/commit/5584f3f1ef7461401a7f1bbbf24302308192b43d))
* span.kind facet + TraceQL support ([#1125](https://github.com/cedricziel/signaldb/issues/1125)) ([35735e5](https://github.com/cedricziel/signaldb/commit/35735e5d204b4fb9f89ddce1dd15296bf9ddfe3c))
* **tempo:** back trace tag discovery with real querier data ([#1258](https://github.com/cedricziel/signaldb/issues/1258)) ([4aeda0d](https://github.com/cedricziel/signaldb/commit/4aeda0d3314fbe7b5546f0411657fdc646e301dd))
* **tenant-table-listing:** list tenant tables from the Iceberg catalog ([#1267](https://github.com/cedricziel/signaldb/issues/1267)) ([5a444c2](https://github.com/cedricziel/signaldb/commit/5a444c261eeab5643d5d2d866385c07e2772ceee))
* **ui:** add a Catalog tab, entities discovered from telemetry ([#1132](https://github.com/cedricziel/signaldb/issues/1132)) ([9f90539](https://github.com/cedricziel/signaldb/commit/9f90539dbf563c496571223e656de0302995c486))
* **ui:** add a faceted search sidebar to the traces tab ([#1076](https://github.com/cedricziel/signaldb/issues/1076)) ([81a8c24](https://github.com/cedricziel/signaldb/commit/81a8c24f455e69816360e18514c97c754d72d90a))
* **ui:** add user menu and management pages ([#1105](https://github.com/cedricziel/signaldb/issues/1105)) ([c49a93f](https://github.com/cedricziel/signaldb/commit/c49a93ff5d112ce36335c19b12ac3404cdb4a8ba))
* **ui:** catalog entity pages are routes (/catalog/:entity/:identity) ([#1234](https://github.com/cedricziel/signaldb/issues/1234)) ([d99569e](https://github.com/cedricziel/signaldb/commit/d99569eca023528575c03dd30e3e27b09ea9a9c8))
* **ui:** facets with a selection first and expanded ([#1289](https://github.com/cedricziel/signaldb/issues/1289)) ([fef70a7](https://github.com/cedricziel/signaldb/commit/fef70a73e4871d5c2a882d32ae8ee8878910991a))
* **ui:** group, sort, and compile span attributes for readability ([#1123](https://github.com/cedricziel/signaldb/issues/1123)) ([2a99ad2](https://github.com/cedricziel/signaldb/commit/2a99ad23fac4002b2c0ec9c50fa694d7013c914d))
* **ui:** make the explore volume charts readable, and give traces one ([#1075](https://github.com/cedricziel/signaldb/issues/1075)) ([91ec80d](https://github.com/cedricziel/signaldb/commit/91ec80da2a8009a6237fa0e939961b00305fd0f3))
* **ui:** make the span-detail and facet/field sidebars resizable ([#1124](https://github.com/cedricziel/signaldb/issues/1124)) ([73023ae](https://github.com/cedricziel/signaldb/commit/73023aefabe9f29b3fe778c0d4110c6ff3512e58))
* **ui:** multi-select span.kind facet with a boundary-kinds default ([#1288](https://github.com/cedricziel/signaldb/issues/1288)) ([d0fdeef](https://github.com/cedricziel/signaldb/commit/d0fdeef1b16783328a5b60b79a478d469e603678))
* **ui:** native Profiles tab + Catalog/Traces UX improvements ([#1164](https://github.com/cedricziel/signaldb/issues/1164)) ([a9d9223](https://github.com/cedricziel/signaldb/commit/a9d9223aa67a419b924513e961cc61d9ac6c97f5))
* **ui:** read trace detail over the Query IR ([#1284](https://github.com/cedricziel/signaldb/issues/1284)) ([7737eeb](https://github.com/cedricziel/signaldb/commit/7737eebb1ac05bb53d2b73c624286041f79d8423))
* **ui:** render span events and exceptions in the trace view ([#849](https://github.com/cedricziel/signaldb/issues/849)) ([5427c05](https://github.com/cedricziel/signaldb/commit/5427c0527c0cd1d7591da3d9077b1aa88714729a))
* **ui:** resolve attribute labels to semantic titles and descriptions ([#1222](https://github.com/cedricziel/signaldb/issues/1222)) ([e54f935](https://github.com/cedricziel/signaldb/commit/e54f935e2872c543b82ec5937757e18abdc4d869))
* **ui:** rich data-point tooltips on every visualization panel ([#1233](https://github.com/cedricziel/signaldb/issues/1233)) ([a781acb](https://github.com/cedricziel/signaldb/commit/a781acb6014a6264d0430a5d3a5173fe7227c6e3))
* **ui:** rich hover tooltip on waterfall spans ([#1279](https://github.com/cedricziel/signaldb/issues/1279)) ([7db94af](https://github.com/cedricziel/signaldb/commit/7db94af9cc94281a62502d9af04bc6711cd95fbf))
* **ui:** schema hub for inspecting and managing registries ([#1221](https://github.com/cedricziel/signaldb/issues/1221)) ([a265f1a](https://github.com/cedricziel/signaldb/commit/a265f1a80575afa22c3b6926181c7f3d9b315cd6))


### Bug Fixes

* address review findings from [#1260](https://github.com/cedricziel/signaldb/issues/1260) ([#1270](https://github.com/cedricziel/signaldb/issues/1270)) ([d5a6ff5](https://github.com/cedricziel/signaldb/commit/d5a6ff50c49644942cfdc4663d7ab7a2d95fe0fb))
* **common,router:** include every known dataset in the tables grouping ([#1269](https://github.com/cedricziel/signaldb/issues/1269)) ([a895618](https://github.com/cedricziel/signaldb/commit/a8956181e5fd7f4cb91432d5f9622175708d2d70))
* **logql:** carry log and resource attributes as structured metadata ([#1094](https://github.com/cedricziel/signaldb/issues/1094)) ([26b9d15](https://github.com/cedricziel/signaldb/commit/26b9d15457ac84c96ba2affe28d3ea520b40c664))
* **mcp:** refresh expired OAuth credentials ([#1100](https://github.com/cedricziel/signaldb/issues/1100)) ([54484e6](https://github.com/cedricziel/signaldb/commit/54484e69083b66e676fcff4e6e4d46fe2c73a766))
* **query-ir:** reapply flamegraph Option fix dropped by a stale merge ([#1146](https://github.com/cedricziel/signaldb/issues/1146)) ([811bb11](https://github.com/cedricziel/signaldb/commit/811bb111b8274e85a181203182a6dd462c3c9438))
* **router:** bound Tempo tag-values queries by time window ([#929](https://github.com/cedricziel/signaldb/issues/929)) ([#979](https://github.com/cedricziel/signaldb/issues/979)) ([7cc301a](https://github.com/cedricziel/signaldb/commit/7cc301adc539a77540682d155425bace30ddc803))
* **ui:** catalog multi-source discovery, trace routing fix, span_kind fix ([#1210](https://github.com/cedricziel/signaldb/issues/1210)) ([84446f2](https://github.com/cedricziel/signaldb/commit/84446f2ef450be67fb14dbb6c4b4feb477ea0d04))
* **ui:** clear XHR timing resources like fetch instrumentation ([#1034](https://github.com/cedricziel/signaldb/issues/1034)) ([a0bcf10](https://github.com/cedricziel/signaldb/commit/a0bcf10ebe8696960f86d267285b6db266c8eb7b))
* **ui:** collapse high-cardinality navigation span name ([#876](https://github.com/cedricziel/signaldb/issues/876)) ([692efb7](https://github.com/cedricziel/signaldb/commit/692efb73eb2a97bc2fa0887575a9cd834a0faf4a))
* **ui:** draw a zero-duration parent span over its subtree ([#1285](https://github.com/cedricziel/signaldb/issues/1285)) ([a08c7b5](https://github.com/cedricziel/signaldb/commit/a08c7b50bc8c8924d4c47dcbf53d537dc9c47c2b))
* **ui:** give trace/group drill-down real history entries, add a not-found page ([#1130](https://github.com/cedricziel/signaldb/issues/1130)) ([fc68b88](https://github.com/cedricziel/signaldb/commit/fc68b88385024e91188459a03fa347ef3545c75b))
* **ui:** keep tenant context sticky across links that drop the query string ([#1226](https://github.com/cedricziel/signaldb/issues/1226)) ([29ac523](https://github.com/cedricziel/signaldb/commit/29ac523ace850847e6090aee9e8d0e3d78d98fc4))
* **ui:** keep the current path when writing the sticky tenant back into the URL ([#1247](https://github.com/cedricziel/signaldb/issues/1247)) ([641f640](https://github.com/cedricziel/signaldb/commit/641f6403557aaab60817e1fd74978d5b20df16c3))
* **ui:** keep the schema tooltip out of scrolling panes ([#1276](https://github.com/cedricziel/signaldb/issues/1276)) ([c765c9d](https://github.com/cedricziel/signaldb/commit/c765c9d26626e023992bab80e6466786c870a42a))
* **ui:** make signal tabs real routes, not a single replaced entry ([#1128](https://github.com/cedricziel/signaldb/issues/1128)) ([acc1339](https://github.com/cedricziel/signaldb/commit/acc13398ba44a018d6d74ac86c86630a6968f8d5))
* **ui:** make the flame graph tooltip follow the pointer ([#1278](https://github.com/cedricziel/signaldb/issues/1278)) ([bfbe6c9](https://github.com/cedricziel/signaldb/commit/bfbe6c9f8d3329a6e709c7b8f33229b577c6d993))
* **ui:** read the trace detail's span duration from duration_nanos ([#1286](https://github.com/cedricziel/signaldb/issues/1286)) ([1281df2](https://github.com/cedricziel/signaldb/commit/1281df2e95d277ac79de1a8a1038b3c8625074e3))
* **ui:** remember the tenant context across tabs so bare deep links resume it ([#1231](https://github.com/cedricziel/signaldb/issues/1231)) ([c794883](https://github.com/cedricziel/signaldb/commit/c7948837a17325b729fe81df45fc9b19d81acfa9))
* **ui:** repair query-IR 400s from Layer 2 logical schema migration ([#1118](https://github.com/cedricziel/signaldb/issues/1118)) ([7e1bea8](https://github.com/cedricziel/signaldb/commit/7e1bea823af0ad26a2aa76ca106d17838dff9596))
* **ui:** route Metrics builder default queries through Query IR ([#1138](https://github.com/cedricziel/signaldb/issues/1138)) ([4056261](https://github.com/cedricziel/signaldb/commit/4056261e0d406d5ae73dc2fe20bc136b8e866bb8))


### Code Refactoring

* **cli:** make signaldb-cli depend only on the SDK (+ create_user API) ([#874](https://github.com/cedricziel/signaldb/issues/874)) ([8e5cce5](https://github.com/cedricziel/signaldb/commit/8e5cce56c821d69917b55cc8c21a9a2ef55864b7))
* **tempo-api:** simplify pass ([#1176](https://github.com/cedricziel/signaldb/issues/1176)) ([10fd364](https://github.com/cedricziel/signaldb/commit/10fd36487971613586084cc1eb29c0dd93a99b9d))
* **ui:** simplify pass ([#1192](https://github.com/cedricziel/signaldb/issues/1192)) ([4c67615](https://github.com/cedricziel/signaldb/commit/4c67615500632225aeeaade5cf745dc8607c9c6d))

## [0.1.2](https://github.com/cedricziel/signaldb/compare/signaldb-ui-v0.1.1...signaldb-ui-v0.1.2) (2026-07-30)


### Features

* CPU self-profiling + Profiles tab in the Explore UI ([#835](https://github.com/cedricziel/signaldb/issues/835)) ([9434734](https://github.com/cedricziel/signaldb/commit/94347345da14db950c760db21ad8516f9fcbac92))
* **ui:** cardinality warnings in the metrics builder ([#834](https://github.com/cedricziel/signaldb/issues/834)) ([7dc8d8a](https://github.com/cedricziel/signaldb/commit/7dc8d8af033fa10bc1155402137a8ff7166bb218))
* **ui:** group-first traces view with selectable dimensions, RED columns, and drill-in ([#824](https://github.com/cedricziel/signaldb/issues/824)) ([65112aa](https://github.com/cedricziel/signaldb/commit/65112aae4f6bbc572543d9102c822089701a3a0c))
* **ui:** instrument browser frontend with OpenTelemetry ([#830](https://github.com/cedricziel/signaldb/issues/830)) ([2bb21de](https://github.com/cedricziel/signaldb/commit/2bb21de6515d3da4756668a9753f94f7eff6ccc1))
* **ui:** metrics explore visual query builder ([#828](https://github.com/cedricziel/signaldb/issues/828)) ([673f0d9](https://github.com/cedricziel/signaldb/commit/673f0d95781ec06e2d2d0f75f6023d20d0159abb))

## [0.1.1](https://github.com/cedricziel/signaldb/compare/signaldb-ui-v0.1.0...signaldb-ui-v0.1.1) (2026-07-30)


### Features

* **auth:** add scoped tenant self-service ([7830c3d](https://github.com/cedricziel/signaldb/commit/7830c3d706c21480f9767bca8639e5fcb82622bc))
* embedded UI session auth + tenant-scoped whoami ([#773](https://github.com/cedricziel/signaldb/issues/773)) ([f217064](https://github.com/cedricziel/signaldb/commit/f217064d3f31002132761040bc8a82fe1c5e9c59))
* native explore UI for logs, traces, and metrics ([#768](https://github.com/cedricziel/signaldb/issues/768)) ([5db53c9](https://github.com/cedricziel/signaldb/commit/5db53c9f87b791c1f1d9590c6a1288db376da92b))
* **ui:** use human account sessions ([c35d405](https://github.com/cedricziel/signaldb/commit/c35d405838ca97df26e50cd9fab83630ac4e2b7c))


### Bug Fixes

* **ui:** sign in once — email/password login with a post-login tenant picker ([#794](https://github.com/cedricziel/signaldb/issues/794)) ([1feafbf](https://github.com/cedricziel/signaldb/commit/1feafbfc187069944c34a5903d65552f740c2d3a))
