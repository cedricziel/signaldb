:robot: I have created a release *beep* *boop*
---


<details><summary>grafana-plugin: 1.3.2</summary>

## [1.3.2](https://github.com/cedricziel/signaldb/compare/grafana-plugin-v1.3.1...grafana-plugin-v1.3.2) (2026-10-08)


### Build System

* **deps-dev:** bump css-loader from 7.1.4 to 7.1.5 ([#2113](https://github.com/cedricziel/signaldb/issues/2113)) ([ce67b81](https://github.com/cedricziel/signaldb/commit/ce67b81f69864c0c39fb0680abfe69b1d7bfe033))
* **deps-dev:** bump prettier from 3.9.6 to 3.9.9 ([#2116](https://github.com/cedricziel/signaldb/issues/2116)) ([12bb01e](https://github.com/cedricziel/signaldb/commit/12bb01e5276133f6c17c45d6818ed15c896b66bf))
* **deps-dev:** bump the build-tools group across 1 directory with 2 updates ([#2215](https://github.com/cedricziel/signaldb/issues/2215)) ([6d68a65](https://github.com/cedricziel/signaldb/commit/6d68a654a86afc464006c51f08c240a71835be54))
* **deps-dev:** bump the typescript-eslint group with 2 updates ([#2112](https://github.com/cedricziel/signaldb/issues/2112)) ([069d9a6](https://github.com/cedricziel/signaldb/commit/069d9a6b5194e608720209ca06890e7ee32a661f))
* **deps-dev:** bump the typescript-eslint group with 2 updates ([#2214](https://github.com/cedricziel/signaldb/issues/2214)) ([4dd42e5](https://github.com/cedricziel/signaldb/commit/4dd42e5f43dca8716da12392b8cce43c67e2e6df))
* **deps:** bump object from 0.37.3 to 0.39.1 ([#2203](https://github.com/cedricziel/signaldb/issues/2203)) ([b6de77a](https://github.com/cedricziel/signaldb/commit/b6de77a4378388901382c517ed72e7a93c309c48))
* **deps:** bump the grafana-ecosystem group across 1 directory with 5 updates ([#2212](https://github.com/cedricziel/signaldb/issues/2212)) ([21807fd](https://github.com/cedricziel/signaldb/commit/21807fd7d87d0e858ad898d97ec294e850548e2e))
* **deps:** bump thiserror in /src/grafana-plugin/backend ([#1906](https://github.com/cedricziel/signaldb/issues/1906)) ([10412a5](https://github.com/cedricziel/signaldb/commit/10412a575b7192f2dab164837da8100efe16cbc1))
* **deps:** bump tokio in /src/grafana-plugin/backend ([#2199](https://github.com/cedricziel/signaldb/issues/2199)) ([312cfc1](https://github.com/cedricziel/signaldb/commit/312cfc129bb1592b461f0e4a67caa67c3676f028))
</details>

<details><summary>signaldb-ui: 0.3.0</summary>

## [0.3.0](https://github.com/cedricziel/signaldb/compare/signaldb-ui-v0.2.2...signaldb-ui-v0.3.0) (2026-10-08)


###   BREAKING CHANGES

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
* **router:** report the oldest data a query's source holds ([#2191](https://github.com/cedricziel/signaldb/issues/2191)) ([7911eb5](https://github.com/cedricziel/signaldb/commit/7911eb512868a40075187e47fdce0303cf1507a0))
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
* **ui:** add RUM traced-request KPIs and the Frontend ’ backend panel ([#1891](https://github.com/cedricziel/signaldb/issues/1891)) ([85481b0](https://github.com/cedricziel/signaldb/commit/85481b0b32f5a4261bc09cf3e3dfaf9cd5ede1c5))
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
* **ui:** pin playwright to the @playwright/test range ([#2210](https://github.com/cedricziel/signaldb/issues/2210)) ([7674738](https://github.com/cedricziel/signaldb/commit/767473889f3d0273939548721be665346f377de9))
* **ui:** polish the Metrics view chart, legend and query builder ([#1874](https://github.com/cedricziel/signaldb/issues/1874)) ([f537b51](https://github.com/cedricziel/signaldb/commit/f537b51aadd1ddeba8e51bec0896c6b941cee02c))
* **ui:** polish the Traces view from the Storybook audit ([#1877](https://github.com/cedricziel/signaldb/issues/1877)) ([d7f38de](https://github.com/cedricziel/signaldb/commit/d7f38de76309a5826ded1ba6826acb2ac5eea253))
* **ui:** probe the session before mounting routes for a remembered tenant ([#2205](https://github.com/cedricziel/signaldb/issues/2205)) ([3c4ffad](https://github.com/cedricziel/signaldb/commit/3c4ffadc57f6751000d6e7172283b407e67fc18e))
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

* **deps-dev:** bump sharp from 0.35.4 to 0.35.5 ([#2208](https://github.com/cedricziel/signaldb/issues/2208)) ([1bd5188](https://github.com/cedricziel/signaldb/commit/1bd5188aed54706c7643c1214f900463ca1a9418))
* **deps-dev:** bump the typescript-eslint group with 2 updates ([#2214](https://github.com/cedricziel/signaldb/issues/2214)) ([4dd42e5](https://github.com/cedricziel/signaldb/commit/4dd42e5f43dca8716da12392b8cce43c67e2e6df))
* **deps-dev:** bump typescript-eslint from 8.67.0 to 8.70.1 ([#2115](https://github.com/cedricziel/signaldb/issues/2115)) ([278a6b5](https://github.com/cedricziel/signaldb/commit/278a6b5b3d76a9118ba5dea3336e13446d9cafea))
* **deps:** bump @opentelemetry/api-logs from 0.221.0 to 0.222.0 ([#2114](https://github.com/cedricziel/signaldb/issues/2114)) ([696c396](https://github.com/cedricziel/signaldb/commit/696c39667eed08f19cbdda651d241ca9a4e6e23e))
* **deps:** bump @opentelemetry/auto-instrumentations-web ([#2216](https://github.com/cedricziel/signaldb/issues/2216)) ([b30ec6f](https://github.com/cedricziel/signaldb/commit/b30ec6f0edadbf81704373820935435d8a59d77d))
* **deps:** bump @opentelemetry/instrumentation from 0.221.0 to 0.222.0 ([#2217](https://github.com/cedricziel/signaldb/issues/2217)) ([0a708d1](https://github.com/cedricziel/signaldb/commit/0a708d1f3ff14ad892f868282b3277ae40af6b5b))
* **deps:** bump @tanstack/react-virtual from 3.14.9 to 3.14.13 ([#2218](https://github.com/cedricziel/signaldb/issues/2218)) ([4493fa2](https://github.com/cedricziel/signaldb/commit/4493fa28e7bebd20fd0f30abe8ae06da97575538))
* **deps:** bump object from 0.37.3 to 0.39.1 ([#2203](https://github.com/cedricziel/signaldb/issues/2203)) ([b6de77a](https://github.com/cedricziel/signaldb/commit/b6de77a4378388901382c517ed72e7a93c309c48))
</details>

<details><summary>loki-api: 0.1.4</summary>

## [0.1.4](https://github.com/cedricziel/signaldb/compare/loki-api-v0.1.3...loki-api-v0.1.4) (2026-10-08)


### Build System

* **deps:** bump object from 0.37.3 to 0.39.1 ([#2203](https://github.com/cedricziel/signaldb/issues/2203)) ([b6de77a](https://github.com/cedricziel/signaldb/commit/b6de77a4378388901382c517ed72e7a93c309c48))
* fix the beta test leg for cargo's unused-dependency lints ([#2047](https://github.com/cedricziel/signaldb/issues/2047)) ([6867d69](https://github.com/cedricziel/signaldb/commit/6867d69dceb26f2e55ccfac31e60ae42aad76418))
</details>

<details><summary>mcp-server: 0.3.0</summary>

## [0.3.0](https://github.com/cedricziel/signaldb/compare/mcp-server-v0.2.2...mcp-server-v0.3.0) (2026-10-08)


###   BREAKING CHANGES

* **traceql:** `Condition` has a new public `op` field, so code that constructs one must set it.
* **mcp-server:** discover_attributes and discover_metrics return the Query IR metadata envelope (logical dotted names, levels, cost) instead of a Tempo tag list, a Loki/Prometheus {status,data} array, or Pyroscope {names}. scope narrows by attribute level rather than routing to the Tempo v2 endpoints, and tag values need a declared set, a statistics sketch, or sample: true.
* `"from": "metrics_histogram"` is rejected as an unknown source. Use `"from": "metrics"`; add a `metric.type = histogram` filter where only histogram rows are wanted. `histogram_quantile` on `metrics` already reads histogram rows only.

### Features

* **cli,mcp:** follow a Query IR live tail through the IR endpoint ([#2150](https://github.com/cedricziel/signaldb/issues/2150)) ([1219d9d](https://github.com/cedricziel/signaldb/commit/1219d9d971107f485ffb7a4737bda901eceaedc0))
* **cli,mcp:** page Query IR results through the IR endpoint ([#2144](https://github.com/cedricziel/signaldb/issues/2144)) ([9fc147d](https://github.com/cedricziel/signaldb/commit/9fc147d416a5329a31040af566424f40db3e5f6c))
* **evals:** build eval sets from traces ([#1847](https://github.com/cedricziel/signaldb/issues/1847)) ([dfe9e60](https://github.com/cedricziel/signaldb/commit/dfe9e608f9629abee1a542f5020f164d193b8faf))
* **evals:** list and compare eval runs from MCP and the CLI ([#1866](https://github.com/cedricziel/signaldb/issues/1866)) ([665f458](https://github.com/cedricziel/signaldb/commit/665f4588808aad9ae567df358c7732f70881aec9))
* **evals:** upload eval results and gate CI on them ([#1857](https://github.com/cedricziel/signaldb/issues/1857)) ([175155a](https://github.com/cedricziel/signaldb/commit/175155a3a2d958d59e16a05961206c0b09f1e79a))
* **mcp-server:** discover attributes and metrics through the Query IR ([#2077](https://github.com/cedricziel/signaldb/issues/2077)) ([37d58ea](https://github.com/cedricziel/signaldb/commit/37d58eac6e5ac2b3766b324e494f0c4901812a5a))
* **mcp:** add a list_services tool ([#2180](https://github.com/cedricziel/signaldb/issues/2180)) ([1e048d5](https://github.com/cedricziel/signaldb/commit/1e048d5a2da962559256a17d5311bdcf26760450)), closes [#2172](https://github.com/cedricziel/signaldb/issues/2172)
* **mcp:** serve the query-ir reference as fetchable sections ([#2179](https://github.com/cedricziel/signaldb/issues/2179)) ([afc4ff2](https://github.com/cedricziel/signaldb/commit/afc4ff28e47bd8b69c227fc35cb8a556e8482527)), closes [#2173](https://github.com/cedricziel/signaldb/issues/2173)
* **processors:** diff Test panel output against the server's decoded input ([#1883](https://github.com/cedricziel/signaldb/issues/1883)) ([7711bc5](https://github.com/cedricziel/signaldb/commit/7711bc5bca22d010fb83766449d52ccdd44291e0))
* **querier:** add the exemplars IR source over metric_exemplars ([#1946](https://github.com/cedricziel/signaldb/issues/1946)) ([5c2c348](https://github.com/cedricziel/signaldb/commit/5c2c3484fc7e4f25219ab05e0813ca0b9c4e3c0c))
* recognise AI agents as the gen_ai.agent entity ([#1803](https://github.com/cedricziel/signaldb/issues/1803)) ([e0fcaa0](https://github.com/cedricziel/signaldb/commit/e0fcaa0b3cd8133e23e0ccc8f9e28e43986c41f5))
* remove the metrics_histogram IR source ([#1945](https://github.com/cedricziel/signaldb/issues/1945)) ([c16f82c](https://github.com/cedricziel/signaldb/commit/c16f82c100199ef0f297e366208c07d8242501c5))
* **router:** eval sets API for offline agent evals ([#1837](https://github.com/cedricziel/signaldb/issues/1837)) ([b3ce35a](https://github.com/cedricziel/signaldb/commit/b3ce35a7fb9671494e4d78cd29b2673f20205531))
* **router:** list discovery fields with their canonical authority type ([#2075](https://github.com/cedricziel/signaldb/issues/2075)) ([621aa46](https://github.com/cedricziel/signaldb/commit/621aa4674399a628470b73810e033963288a3866))
* **router:** publish UI login/logout and the full whoami response in OpenAPI ([#2110](https://github.com/cedricziel/signaldb/issues/2110)) ([664c8fe](https://github.com/cedricziel/signaldb/commit/664c8fe52a51505ef5fc46a63293e1e35a1a1913))
* **router:** type the Query IR request pipeline as IrStage ([#2095](https://github.com/cedricziel/signaldb/issues/2095)) ([414ed92](https://github.com/cedricziel/signaldb/commit/414ed92b136367e2650b1c8de8f0cd12250cce5b))
* show span links in the Query IR, MCP get_trace and the trace view ([#2177](https://github.com/cedricziel/signaldb/issues/2177)) ([b873eab](https://github.com/cedricziel/signaldb/commit/b873eabee7f7e4e836a530a3fc75fe5261cafb49))
* **traceql:** support !=, =~ and !~ and surface search errors over MCP ([#2178](https://github.com/cedricziel/signaldb/issues/2178)) ([ce929f5](https://github.com/cedricziel/signaldb/commit/ce929f5f321e5d826e2ba43edc67d7e26de67659))


### Bug Fixes

* **mcp:** name every missing field in query_ir document errors ([#1798](https://github.com/cedricziel/signaldb/issues/1798)) ([bcf8804](https://github.com/cedricziel/signaldb/commit/bcf8804e36438aee133171d11a16a46557d6eacf))
* **mcp:** read get_trace over the Query IR ([#1810](https://github.com/cedricziel/signaldb/issues/1810)) ([8032175](https://github.com/cedricziel/signaldb/commit/8032175d56649cebfd078f9ce3a361f6e60c1d8f))
* **mcp:** report profiles_for_trace durations and cover the tool end to end ([#2131](https://github.com/cedricziel/signaldb/issues/2131)) ([760bcbb](https://github.com/cedricziel/signaldb/commit/760bcbbe5c251560e4b3928d8949157ad942bded))
* **mcp:** serve sessionless requests from clients that skip initialize ([#2198](https://github.com/cedricziel/signaldb/issues/2198)) ([b6387f5](https://github.com/cedricziel/signaldb/commit/b6387f58f4f8c9036f99a9bdfaf68d32351b76fc))
* **query-ir:** resolve trace.id/span.id on traces and logs, name bad predicates over MCP ([#2206](https://github.com/cedricziel/signaldb/issues/2206)) ([b462073](https://github.com/cedricziel/signaldb/commit/b462073340a278c6ac5f4335afe4f18b5398ebce))
* **router:** read data for sample:true and flag partial discovery statistics ([#2176](https://github.com/cedricziel/signaldb/issues/2176)) ([ab5dcf7](https://github.com/cedricziel/signaldb/commit/ab5dcf78f47567a6e2a8f8a10dcf5dfd06cdfed5))


### Code Refactoring

* **mcp-server:** compare profiles through the Query IR baseline ([#2103](https://github.com/cedricziel/signaldb/issues/2103)) ([153704e](https://github.com/cedricziel/signaldb/commit/153704eb82aa6955f197ec354f9abcb5bc0b3d7e))
* **mcp-server:** complete prompt arguments through the Query IR ([#2097](https://github.com/cedricziel/signaldb/issues/2097)) ([3ab363d](https://github.com/cedricziel/signaldb/commit/3ab363d67579e4d75cb3bb551f5325ae270938bc))
* **mcp-server:** discover profile types through the Query IR ([#2101](https://github.com/cedricziel/signaldb/issues/2101)) ([b6ff25a](https://github.com/cedricziel/signaldb/commit/b6ff25a19b4e4d0578231c9ceeec249d88237d35))
* **mcp-server:** list a trace's profiles through the Query IR ([#2104](https://github.com/cedricziel/signaldb/issues/2104)) ([a351006](https://github.com/cedricziel/signaldb/commit/a351006e33b8cdba9c06c319c1fa5e53c26b6428))
* **mcp-server:** search profiles through the Query IR flamegraph ([#2102](https://github.com/cedricziel/signaldb/issues/2102)) ([26db597](https://github.com/cedricziel/signaldb/commit/26db597571d076b420eb4011510e5189f3e2cfb8))


### Build System

* **deps:** bump object from 0.37.3 to 0.39.1 ([#2203](https://github.com/cedricziel/signaldb/issues/2203)) ([b6de77a](https://github.com/cedricziel/signaldb/commit/b6de77a4378388901382c517ed72e7a93c309c48))
* fix the beta test leg for cargo's unused-dependency lints ([#2047](https://github.com/cedricziel/signaldb/issues/2047)) ([6867d69](https://github.com/cedricziel/signaldb/commit/6867d69dceb26f2e55ccfac31e60ae42aad76418))
</details>

<details><summary>prometheus-api: 0.1.5</summary>

## [0.1.5](https://github.com/cedricziel/signaldb/compare/prometheus-api-v0.1.4...prometheus-api-v0.1.5) (2026-10-08)


### Build System

* **deps:** bump object from 0.37.3 to 0.39.1 ([#2203](https://github.com/cedricziel/signaldb/issues/2203)) ([b6de77a](https://github.com/cedricziel/signaldb/commit/b6de77a4378388901382c517ed72e7a93c309c48))
* fix the beta test leg for cargo's unused-dependency lints ([#2047](https://github.com/cedricziel/signaldb/issues/2047)) ([6867d69](https://github.com/cedricziel/signaldb/commit/6867d69dceb26f2e55ccfac31e60ae42aad76418))
</details>

<details><summary>pyroscope-api: 0.1.4</summary>

## [0.1.4](https://github.com/cedricziel/signaldb/compare/pyroscope-api-v0.1.3...pyroscope-api-v0.1.4) (2026-10-08)


### Build System

* **deps:** bump object from 0.37.3 to 0.39.1 ([#2203](https://github.com/cedricziel/signaldb/issues/2203)) ([b6de77a](https://github.com/cedricziel/signaldb/commit/b6de77a4378388901382c517ed72e7a93c309c48))
* fix the beta test leg for cargo's unused-dependency lints ([#2047](https://github.com/cedricziel/signaldb/issues/2047)) ([6867d69](https://github.com/cedricziel/signaldb/commit/6867d69dceb26f2e55ccfac31e60ae42aad76418))
</details>

<details><summary>signal-producer: 0.2.3</summary>

## [0.2.3](https://github.com/cedricziel/signaldb/compare/signal-producer-v0.2.2...signal-producer-v0.2.3) (2026-10-08)


### Build System

* **deps:** bump object from 0.37.3 to 0.39.1 ([#2203](https://github.com/cedricziel/signaldb/issues/2203)) ([b6de77a](https://github.com/cedricziel/signaldb/commit/b6de77a4378388901382c517ed72e7a93c309c48))
* fix the beta test leg for cargo's unused-dependency lints ([#2047](https://github.com/cedricziel/signaldb/issues/2047)) ([6867d69](https://github.com/cedricziel/signaldb/commit/6867d69dceb26f2e55ccfac31e60ae42aad76418))
</details>

<details><summary>signaldb-api: 0.2.3</summary>

## [0.2.3](https://github.com/cedricziel/signaldb/compare/signaldb-api-v0.2.2...signaldb-api-v0.2.3) (2026-10-08)


### Features

* **router:** eval sets API for offline agent evals ([#1837](https://github.com/cedricziel/signaldb/issues/1837)) ([b3ce35a](https://github.com/cedricziel/signaldb/commit/b3ce35a7fb9671494e4d78cd29b2673f20205531))


### Build System

* **deps:** bump object from 0.37.3 to 0.39.1 ([#2203](https://github.com/cedricziel/signaldb/issues/2203)) ([b6de77a](https://github.com/cedricziel/signaldb/commit/b6de77a4378388901382c517ed72e7a93c309c48))
* fix the beta test leg for cargo's unused-dependency lints ([#2047](https://github.com/cedricziel/signaldb/issues/2047)) ([6867d69](https://github.com/cedricziel/signaldb/commit/6867d69dceb26f2e55ccfac31e60ae42aad76418))
</details>

<details><summary>signaldb-sdk: 0.3.0</summary>

## [0.3.0](https://github.com/cedricziel/signaldb/compare/signaldb-sdk-v0.2.2...signaldb-sdk-v0.3.0) (2026-10-08)


###   BREAKING CHANGES

* **traceql:** `Condition` has a new public `op` field, so code that constructs one must set it.
* **mcp-server:** discover_attributes and discover_metrics return the Query IR metadata envelope (logical dotted names, levels, cost) instead of a Tempo tag list, a Loki/Prometheus {status,data} array, or Pyroscope {names}. scope narrows by attribute level rather than routing to the Tempo v2 endpoints, and tag values need a declared set, a statistics sketch, or sample: true.

### Features

* **common:** merge discovery fields with the type authority's canonical types ([#2074](https://github.com/cedricziel/signaldb/issues/2074)) ([36b7266](https://github.com/cedricziel/signaldb/commit/36b7266ec9442a10aaec8b7eb45d654b6cb27ec7))
* **evals:** build eval sets from traces ([#1847](https://github.com/cedricziel/signaldb/issues/1847)) ([dfe9e60](https://github.com/cedricziel/signaldb/commit/dfe9e608f9629abee1a542f5020f164d193b8faf))
* **evals:** eval sets pages, upload dialog and saving regressions in the UI ([#1871](https://github.com/cedricziel/signaldb/issues/1871)) ([cb05278](https://github.com/cedricziel/signaldb/commit/cb05278065450372e37f51fc149f4ac04a6ad6a7))
* **evals:** upload eval results and gate CI on them ([#1857](https://github.com/cedricziel/signaldb/issues/1857)) ([175155a](https://github.com/cedricziel/signaldb/commit/175155a3a2d958d59e16a05961206c0b09f1e79a))
* **mcp-server:** discover attributes and metrics through the Query IR ([#2077](https://github.com/cedricziel/signaldb/issues/2077)) ([37d58ea](https://github.com/cedricziel/signaldb/commit/37d58eac6e5ac2b3766b324e494f0c4901812a5a))
* **processors:** diff Test panel output against the server's decoded input ([#1883](https://github.com/cedricziel/signaldb/issues/1883)) ([7711bc5](https://github.com/cedricziel/signaldb/commit/7711bc5bca22d010fb83766449d52ccdd44291e0))
* **querier:** add histogram_avg, histogram_stddev and histogram_stdvar ([#2187](https://github.com/cedricziel/signaldb/issues/2187)) ([626a417](https://github.com/cedricziel/signaldb/commit/626a41777f5393eb364a00b9fa211cd674f2eb09))
* **query-ir:** correlate to another signal with semi and anti joins (irVersion 11) ([#2052](https://github.com/cedricziel/signaldb/issues/2052)) ([ad0cd64](https://github.com/cedricziel/signaldb/commit/ad0cd644101af8f140dd7ae89fdc09f94dab9ca2))
* **query-ir:** differential flamegraph over a baseline window (irVersion 13) ([#2100](https://github.com/cedricziel/signaldb/issues/2100)) ([5e36ddb](https://github.com/cedricziel/signaldb/commit/5e36ddbea6b114c7c2077eca2ee28b9b0fa8089f))
* **query-ir:** trace result envelope (irVersion 12) ([#2063](https://github.com/cedricziel/signaldb/issues/2063)) ([934b6bc](https://github.com/cedricziel/signaldb/commit/934b6bccf5d5fdb12595c07191804091664ae145))
* **router:** eval sets API for offline agent evals ([#1837](https://github.com/cedricziel/signaldb/issues/1837)) ([b3ce35a](https://github.com/cedricziel/signaldb/commit/b3ce35a7fb9671494e4d78cd29b2673f20205531))
* **router:** list discovery fields with their canonical authority type ([#2075](https://github.com/cedricziel/signaldb/issues/2075)) ([621aa46](https://github.com/cedricziel/signaldb/commit/621aa4674399a628470b73810e033963288a3866))
* **router:** live-tail Query IR rows and trace results (IR v15) ([#2148](https://github.com/cedricziel/signaldb/issues/2148)) ([57ac3c7](https://github.com/cedricziel/signaldb/commit/57ac3c74646037f2198b86f787da3b7a161d6ffd))
* **router:** paginate Query IR rows and trace results (IR v14) ([#2142](https://github.com/cedricziel/signaldb/issues/2142)) ([67023cc](https://github.com/cedricziel/signaldb/commit/67023cc0f71ab345f23d6278f9a917ee3d349941))
* **router:** publish the Query IR stage grammar as typed OpenAPI schemas ([#2088](https://github.com/cedricziel/signaldb/issues/2088)) ([55e79d8](https://github.com/cedricziel/signaldb/commit/55e79d8fdcb6b5703d1d66801477ae7c714ce6e3))
* **router:** publish UI login/logout and the full whoami response in OpenAPI ([#2110](https://github.com/cedricziel/signaldb/issues/2110)) ([664c8fe](https://github.com/cedricziel/signaldb/commit/664c8fe52a51505ef5fc46a63293e1e35a1a1913))
* **router:** report the oldest data a query's source holds ([#2191](https://github.com/cedricziel/signaldb/issues/2191)) ([7911eb5](https://github.com/cedricziel/signaldb/commit/7911eb512868a40075187e47fdce0303cf1507a0))
* **router:** report the retention that applies to a query ([#2182](https://github.com/cedricziel/signaldb/issues/2182)) ([91ca14d](https://github.com/cedricziel/signaldb/commit/91ca14dfc6f4478155ef9095c1b84e4078a6da90))
* **router:** scalar result envelope and metric Series labels ([#2001](https://github.com/cedricziel/signaldb/issues/2001)) ([2d72dc1](https://github.com/cedricziel/signaldb/commit/2d72dc1b719ee880ad87101fc042d21465f3affc))
* **router:** type the Query IR request pipeline as IrStage ([#2095](https://github.com/cedricziel/signaldb/issues/2095)) ([414ed92](https://github.com/cedricziel/signaldb/commit/414ed92b136367e2650b1c8de8f0cd12250cce5b))
* **router:** warn match_incomplete_trace from the query report trailer ([#2090](https://github.com/cedricziel/signaldb/issues/2090)) ([7ee6f46](https://github.com/cedricziel/signaldb/commit/7ee6f464d3512df3a2c4fddc06f90302c3c55b16))
* **schema-registry:** accept definition/2 custom registry uploads ([#1823](https://github.com/cedricziel/signaldb/issues/1823)) ([a8c8196](https://github.com/cedricziel/signaldb/commit/a8c819648bd5778478be6e86241802ae4f6f880f))
* **schema:** let registry metric definitions declare aliases ([#2188](https://github.com/cedricziel/signaldb/issues/2188)) ([0817836](https://github.com/cedricziel/signaldb/commit/08178369ba741145ea42dc6ff3aeb2517e2ad1bc))
* **traceql:** support !=, =~ and !~ and surface search errors over MCP ([#2178](https://github.com/cedricziel/signaldb/issues/2178)) ([ce929f5](https://github.com/cedricziel/signaldb/commit/ce929f5f321e5d826e2ba43edc67d7e26de67659))


### Bug Fixes

* **query-ir:** keep the newest flamegraph profiles and reject inverted windows ([#2098](https://github.com/cedricziel/signaldb/issues/2098)) ([dda9bad](https://github.com/cedricziel/signaldb/commit/dda9bad2c86140ee9be861ee0e4cd5f2788cf8a9))
* **router:** read data for sample:true and flag partial discovery statistics ([#2176](https://github.com/cedricziel/signaldb/issues/2176)) ([ab5dcf7](https://github.com/cedricziel/signaldb/commit/ab5dcf78f47567a6e2a8f8a10dcf5dfd06cdfed5))


### Documentation

* describe PromQL execution through the Query IR ([#2042](https://github.com/cedricziel/signaldb/issues/2042)) ([3375b34](https://github.com/cedricziel/signaldb/commit/3375b344ff68b1c6f1b546bf36f7de00dd508357))


### Build System

* **deps:** bump object from 0.37.3 to 0.39.1 ([#2203](https://github.com/cedricziel/signaldb/issues/2203)) ([b6de77a](https://github.com/cedricziel/signaldb/commit/b6de77a4378388901382c517ed72e7a93c309c48))
* fix the beta test leg for cargo's unused-dependency lints ([#2047](https://github.com/cedricziel/signaldb/issues/2047)) ([6867d69](https://github.com/cedricziel/signaldb/commit/6867d69dceb26f2e55ccfac31e60ae42aad76418))
</details>

<details><summary>tempo-api: 0.1.4</summary>

## [0.1.4](https://github.com/cedricziel/signaldb/compare/tempo-api-v0.1.3...tempo-api-v0.1.4) (2026-10-08)


### Build System

* **deps:** bump object from 0.37.3 to 0.39.1 ([#2203](https://github.com/cedricziel/signaldb/issues/2203)) ([b6de77a](https://github.com/cedricziel/signaldb/commit/b6de77a4378388901382c517ed72e7a93c309c48))
* fix the beta test leg for cargo's unused-dependency lints ([#2047](https://github.com/cedricziel/signaldb/issues/2047)) ([6867d69](https://github.com/cedricziel/signaldb/commit/6867d69dceb26f2e55ccfac31e60ae42aad76418))
</details>

<details><summary>tests-integration: 0.1.8</summary>

### Dependencies


</details>

<details><summary>acceptor: 0.5.0</summary>

## [0.5.0](https://github.com/cedricziel/signaldb/compare/acceptor-v0.4.1...acceptor-v0.5.0) (2026-10-08)


###   BREAKING CHANGES

* **tests-integration:** metric tables are recreated as metrics/metric_exemplars; pre-cutover metric data is dropped, not migrated.
* metric tables are recreated as metrics/metric_exemplars; pre-cutover metric data is dropped, not migrated.

### Features

* **acceptor:** build off-type attribute warnings for OTLP exports ([#1833](https://github.com/cedricziel/signaldb/issues/1833)) ([b6b74ff](https://github.com/cedricziel/signaldb/commit/b6b74ffdd6d73d3bea2f0d0137ddd1dbee7259c8))
* **acceptor:** cap attributes per record at ingest ([#2185](https://github.com/cedricziel/signaldb/issues/2185)) ([56cc1f0](https://github.com/cedricziel/signaldb/commit/56cc1f0cad0aef215eb54639f187f55cd8055af5)), closes [#821](https://github.com/cedricziel/signaldb/issues/821)
* **acceptor:** override attribute limits per tenant ([#2186](https://github.com/cedricziel/signaldb/issues/2186)) ([6f8d408](https://github.com/cedricziel/signaldb/commit/6f8d408c823bc1fbd95533cadfb8cd6d1f24791b)), closes [#821](https://github.com/cedricziel/signaldb/issues/821)
* **acceptor:** target the wide metrics table under the wide layout ([#1907](https://github.com/cedricziel/signaldb/issues/1907)) ([e6fb521](https://github.com/cedricziel/signaldb/commit/e6fb521164fa99ba4ebf987de022696576916cf9))
* **acceptor:** warn OTLP senders about off-type attribute values ([#1834](https://github.com/cedricziel/signaldb/issues/1834)) ([1538139](https://github.com/cedricziel/signaldb/commit/1538139f06b5eac84158048f54283040b236185a))
* cut metrics over to the typed metrics layout ([#1928](https://github.com/cedricziel/signaldb/issues/1928)) ([e416d6f](https://github.com/cedricziel/signaldb/commit/e416d6f79029d36930b0b98a288517ac18a0b7ff))
* **evals:** accept evaluation results sent as span events ([#1858](https://github.com/cedricziel/signaldb/issues/1858)) ([1c96f50](https://github.com/cedricziel/signaldb/commit/1c96f50f398c747ca1ada2d3c8ead74c5516afd3))
* **evals:** upload eval results and gate CI on them ([#1857](https://github.com/cedricziel/signaldb/issues/1857)) ([175155a](https://github.com/cedricziel/signaldb/commit/175155a3a2d958d59e16a05961206c0b09f1e79a))


### Bug Fixes

* **acceptor:** acknowledge an exporter's resend of an already-durable batch ([#1814](https://github.com/cedricziel/signaldb/issues/1814)) ([ff52ded](https://github.com/cedricziel/signaldb/commit/ff52deda64b5b803c4040cee0db249a6beb5d7f4))
* **acceptor:** dedup client resends across acceptor replicas and restarts ([#1821](https://github.com/cedricziel/signaldb/issues/1821)) ([98cf40e](https://github.com/cedricziel/signaldb/commit/98cf40e5e6bc7d97f3b43564591df2039b9c8070))
* **compactor:** advertise COMPACTOR_ADVERTISE_ADDR instead of the bind address ([#2107](https://github.com/cedricziel/signaldb/issues/2107)) ([2b8adb7](https://github.com/cedricziel/signaldb/commit/2b8adb7f23df02ee44a7d7f72dde53bcdd436254)), closes [#1844](https://github.com/cedricziel/signaldb/issues/1844)
* **telemetry:** namespace bare log fields flagged by weaver live-check ([#1879](https://github.com/cedricziel/signaldb/issues/1879)) ([90dbf09](https://github.com/cedricziel/signaldb/commit/90dbf09c31f7181f4f97c4a0bdb6c79f0f78aca3)), closes [#912](https://github.com/cedricziel/signaldb/issues/912)


### Performance Improvements

* **acceptor:** send WAL IPC bytes to the writer without re-encoding ([#2192](https://github.com/cedricziel/signaldb/issues/2192)) ([be7e5ee](https://github.com/cedricziel/signaldb/commit/be7e5ee93f9f539ed86adf476e03fe044ee00768)), closes [#942](https://github.com/cedricziel/signaldb/issues/942)


### Code Refactoring

* **common:** let ServiceBootstrap resolve the advertised address ([#2121](https://github.com/cedricziel/signaldb/issues/2121)) ([6bef83b](https://github.com/cedricziel/signaldb/commit/6bef83b68b873ea7407d0f92981665a2b4cb420e))
* drop the dead MetricsLayout switch and *_with_layout helpers ([#1961](https://github.com/cedricziel/signaldb/issues/1961)) ([4a98be3](https://github.com/cedricziel/signaldb/commit/4a98be3e108b2c67be847db23de0f768c1ad2a58))


### Tests

* **ingest:** pin WAL format and typed ingest before layer-5 enforcement ([#1828](https://github.com/cedricziel/signaldb/issues/1828)) ([a4b0d74](https://github.com/cedricziel/signaldb/commit/a4b0d740cbb4eab0535a2552797996e47047aa80))
* **tests-integration:** add an end-to-end metrics cutover test ([#1929](https://github.com/cedricziel/signaldb/issues/1929)) ([bb47677](https://github.com/cedricziel/signaldb/commit/bb4767732f40a82ca1af1e4eab88a8dc11580829))


### Build System

* **deps:** bump object from 0.37.3 to 0.39.1 ([#2203](https://github.com/cedricziel/signaldb/issues/2203)) ([b6de77a](https://github.com/cedricziel/signaldb/commit/b6de77a4378388901382c517ed72e7a93c309c48))
* fix the beta test leg for cargo's unused-dependency lints ([#2047](https://github.com/cedricziel/signaldb/issues/2047)) ([6867d69](https://github.com/cedricziel/signaldb/commit/6867d69dceb26f2e55ccfac31e60ae42aad76418))
</details>

<details><summary>common: 0.5.0</summary>

## [0.5.0](https://github.com/cedricziel/signaldb/compare/common-v0.4.1...common-v0.5.0) (2026-10-08)


###   BREAKING CHANGES

* **querier:** a standalone querier with no `memory_limit_mb` is now bounded (set 0 to opt out), and a tenant running more than 8 concurrent queries gets RESOURCE_EXHAUSTED unless `max_concurrent_queries_per_tenant` is raised (0 = unlimited).
* `"from": "metrics_histogram"` is rejected as an unknown source. Use `"from": "metrics"`; add a `metric.type = histogram` filter where only histogram rows are wanted. `histogram_quantile` on `metrics` already reads histogram rows only.
* **tests-integration:** metric tables are recreated as metrics/metric_exemplars; pre-cutover metric data is dropped, not migrated.
* metric tables are recreated as metrics/metric_exemplars; pre-cutover metric data is dropped, not migrated.
* **common:** drop legacy map-layout attribute reads ([#1808](https://github.com/cedricziel/signaldb/issues/1808))
* drop the legacy attr_tokens column ([#1794](https://github.com/cedricziel/signaldb/issues/1794))
* existing tables still in the legacy map<string,string> attribute layout are dropped and recreated in the typed layout the next time they are loaded; pre-cutover data in those tables is not migrated.

### Features

* **acceptor:** build off-type attribute warnings for OTLP exports ([#1833](https://github.com/cedricziel/signaldb/issues/1833)) ([b6b74ff](https://github.com/cedricziel/signaldb/commit/b6b74ffdd6d73d3bea2f0d0137ddd1dbee7259c8))
* **acceptor:** cap attributes per record at ingest ([#2185](https://github.com/cedricziel/signaldb/issues/2185)) ([56cc1f0](https://github.com/cedricziel/signaldb/commit/56cc1f0cad0aef215eb54639f187f55cd8055af5)), closes [#821](https://github.com/cedricziel/signaldb/issues/821)
* **acceptor:** override attribute limits per tenant ([#2186](https://github.com/cedricziel/signaldb/issues/2186)) ([6f8d408](https://github.com/cedricziel/signaldb/commit/6f8d408c823bc1fbd95533cadfb8cd6d1f24791b)), closes [#821](https://github.com/cedricziel/signaldb/issues/821)
* **acceptor:** target the wide metrics table under the wide layout ([#1907](https://github.com/cedricziel/signaldb/issues/1907)) ([e6fb521](https://github.com/cedricziel/signaldb/commit/e6fb521164fa99ba4ebf987de022696576916cf9))
* **common:** add a non-blocking canonical-type snapshot cache ([#1832](https://github.com/cedricziel/signaldb/issues/1832)) ([f051347](https://github.com/cedricziel/signaldb/commit/f051347f39f6d329346ff2952b440cbe7c7c03e3))
* **common:** add the Query IR pagination cursor codec ([#2136](https://github.com/cedricziel/signaldb/issues/2136)) ([54f01b4](https://github.com/cedricziel/signaldb/commit/54f01b40dc4f46882751e3998853c104dc6a75e6))
* **common:** declare the typed metrics and metric_exemplars schemas ([#1886](https://github.com/cedricziel/signaldb/issues/1886)) ([9095c9f](https://github.com/cedricziel/signaldb/commit/9095c9f252f6bfead8ea76d7ee8f2c996caa69bd))
* **common:** derive a stable metric series_id ([#1892](https://github.com/cedricziel/signaldb/issues/1892)) ([74ba456](https://github.com/cedricziel/signaldb/commit/74ba456791be4e445739871931764bda0414a180))
* **common:** evolve typed promoted attribute columns ([#1856](https://github.com/cedricziel/signaldb/issues/1856)) ([03ffc30](https://github.com/cedricziel/signaldb/commit/03ffc3008a680c315ffa216f56a85eddeb896d86))
* **common:** merge discovery fields with the type authority's canonical types ([#2074](https://github.com/cedricziel/signaldb/issues/2074)) ([36b7266](https://github.com/cedricziel/signaldb/commit/36b7266ec9442a10aaec8b7eb45d654b6cb27ec7))
* **common:** persist per-level promotion streaks ([#1859](https://github.com/cedricziel/signaldb/issues/1859)) ([8f93bb2](https://github.com/cedricziel/signaldb/commit/8f93bb2969e0a8a7c9a3e271e0159a34c5688380))
* **common:** sign and bound Query IR cursors ([#2137](https://github.com/cedricziel/signaldb/issues/2137)) ([f41dbe4](https://github.com/cedricziel/signaldb/commit/f41dbe4632c5c588af77c4600274e22f1fb7acb8))
* **common:** source-aware attribute qualifier shared with the planner ([#2073](https://github.com/cedricziel/signaldb/issues/2073)) ([a0ec69f](https://github.com/cedricziel/signaldb/commit/a0ec69ff596d0033ce7064e9cedbb6dd8ef6790e))
* **common:** track per-level attribute stats in the catalog ([#1840](https://github.com/cedricziel/signaldb/issues/1840)) ([9426473](https://github.com/cedricziel/signaldb/commit/94264738879612413de30e9ea93716496fbf4203))
* **compactor:** count attribute presence and demand per level ([#1841](https://github.com/cedricziel/signaldb/issues/1841)) ([b11170d](https://github.com/cedricziel/signaldb/commit/b11170d5e3e56271f10f159e7c5107efc83ecc62))
* **compactor:** demote idle and over-budget promoted attribute columns ([#1867](https://github.com/cedricziel/signaldb/issues/1867)) ([3714e3c](https://github.com/cedricziel/signaldb/commit/3714e3cf4f0c1eb5e65b30d18e9c6b684141758f))
* cut attribute storage over to the typed layout ([#1791](https://github.com/cedricziel/signaldb/issues/1791)) ([79b1fff](https://github.com/cedricziel/signaldb/commit/79b1fff7184ee1b3a97dfaf6fbf2d994a6199b39))
* cut metrics over to the typed metrics layout ([#1928](https://github.com/cedricziel/signaldb/issues/1928)) ([e416d6f](https://github.com/cedricziel/signaldb/commit/e416d6f79029d36930b0b98a288517ac18a0b7ff))
* **evals:** accept evaluation results sent as span events ([#1858](https://github.com/cedricziel/signaldb/issues/1858)) ([1c96f50](https://github.com/cedricziel/signaldb/commit/1c96f50f398c747ca1ada2d3c8ead74c5516afd3))
* **evals:** build eval sets from traces ([#1847](https://github.com/cedricziel/signaldb/issues/1847)) ([dfe9e60](https://github.com/cedricziel/signaldb/commit/dfe9e608f9629abee1a542f5020f164d193b8faf))
* **evals:** eval sets pages, upload dialog and saving regressions in the UI ([#1871](https://github.com/cedricziel/signaldb/issues/1871)) ([cb05278](https://github.com/cedricziel/signaldb/commit/cb05278065450372e37f51fc149f4ac04a6ad6a7))
* **evals:** list and compare eval runs from MCP and the CLI ([#1866](https://github.com/cedricziel/signaldb/issues/1866)) ([665f458](https://github.com/cedricziel/signaldb/commit/665f4588808aad9ae567df358c7732f70881aec9))
* **evals:** upload eval results and gate CI on them ([#1857](https://github.com/cedricziel/signaldb/issues/1857)) ([175155a](https://github.com/cedricziel/signaldb/commit/175155a3a2d958d59e16a05961206c0b09f1e79a))
* **querier:** add the exemplars IR source over metric_exemplars ([#1946](https://github.com/cedricziel/signaldb/issues/1946)) ([5c2c348](https://github.com/cedricziel/signaldb/commit/5c2c3484fc7e4f25219ab05e0813ca0b9c4e3c0c))
* **querier:** bound a Query IR page to a live-tail window ([#2146](https://github.com/cedricziel/signaldb/issues/2146)) ([10421b0](https://github.com/cedricziel/signaldb/commit/10421b0ec2a787b62f515ba61b8e193787e5940b))
* **querier:** bound memory and per-tenant concurrency by default ([#2161](https://github.com/cedricziel/signaldb/issues/2161)) ([93dc0d9](https://github.com/cedricziel/signaldb/commit/93dc0d9e60c84ddfc6fb876b09e40be4e7b305cc))
* **querier:** cut a page off a sorted Query IR result ([#2138](https://github.com/cedricziel/signaldb/issues/2138)) ([3dfe244](https://github.com/cedricziel/signaldb/commit/3dfe24454916835dad0025181660e13b78096303))
* **querier:** decode histogram buckets through a layout-agnostic BucketCol ([#1901](https://github.com/cedricziel/signaldb/issues/1901)) ([699f111](https://github.com/cedricziel/signaldb/commit/699f11135a32642c88ae09391fae0ae49bb189e7))
* **querier:** execute a Query IR ticket's page ([#2139](https://github.com/cedricziel/signaldb/issues/2139)) ([32a3f27](https://github.com/cedricziel/signaldb/commit/32a3f271e3e737a26587f702a217d4c7b69ae323))
* **querier:** expose every metric type through the IR metrics source ([#1942](https://github.com/cedricziel/signaldb/issues/1942)) ([f5bf726](https://github.com/cedricziel/signaldb/commit/f5bf7263964d1ccf143b8aae756eef6aacd3d4cd))
* **querier:** prune scans with the warm containment index ([#1788](https://github.com/cedricziel/signaldb/issues/1788)) ([46acbe2](https://github.com/cedricziel/signaldb/commit/46acbe26eb091c1e800333d22356d9e6f6efaacd))
* **querier:** read per-level typed promoted attribute columns ([#1848](https://github.com/cedricziel/signaldb/issues/1848)) ([dbcf51b](https://github.com/cedricziel/signaldb/commit/dbcf51b3bf5f8a09c451e57e236d903329983cf9))
* **query-ir:** correlate to another signal with semi and anti joins (irVersion 11) ([#2052](https://github.com/cedricziel/signaldb/issues/2052)) ([ad0cd64](https://github.com/cedricziel/signaldb/commit/ad0cd644101af8f140dd7ae89fdc09f94dab9ca2))
* **query-ir:** structural match stage over traces (irVersion 12) ([#2065](https://github.com/cedricziel/signaldb/issues/2065)) ([c34ee37](https://github.com/cedricziel/signaldb/commit/c34ee3789ae0b695f0e2a052e5af28adb1d2e284))
* recognise AI agents as the gen_ai.agent entity ([#1803](https://github.com/cedricziel/signaldb/issues/1803)) ([e0fcaa0](https://github.com/cedricziel/signaldb/commit/e0fcaa0b3cd8133e23e0ccc8f9e28e43986c41f5))
* remove the metrics_histogram IR source ([#1945](https://github.com/cedricziel/signaldb/issues/1945)) ([c16f82c](https://github.com/cedricziel/signaldb/commit/c16f82c100199ef0f297e366208c07d8242501c5))
* **router:** eval sets API for offline agent evals ([#1837](https://github.com/cedricziel/signaldb/issues/1837)) ([b3ce35a](https://github.com/cedricziel/signaldb/commit/b3ce35a7fb9671494e4d78cd29b2673f20205531))
* **router:** publish the Query IR stage grammar as typed OpenAPI schemas ([#2088](https://github.com/cedricziel/signaldb/issues/2088)) ([55e79d8](https://github.com/cedricziel/signaldb/commit/55e79d8fdcb6b5703d1d66801477ae7c714ce6e3))
* **router:** publish UI login/logout and the full whoami response in OpenAPI ([#2110](https://github.com/cedricziel/signaldb/issues/2110)) ([664c8fe](https://github.com/cedricziel/signaldb/commit/664c8fe52a51505ef5fc46a63293e1e35a1a1913))
* **router:** report the oldest data a query's source holds ([#2191](https://github.com/cedricziel/signaldb/issues/2191)) ([7911eb5](https://github.com/cedricziel/signaldb/commit/7911eb512868a40075187e47fdce0303cf1507a0))
* **router:** report the retention that applies to a query ([#2182](https://github.com/cedricziel/signaldb/issues/2182)) ([91ca14d](https://github.com/cedricziel/signaldb/commit/91ca14dfc6f4478155ef9095c1b84e4078a6da90))
* **router:** warn match_incomplete_trace from the query report trailer ([#2090](https://github.com/cedricziel/signaldb/issues/2090)) ([7ee6f46](https://github.com/cedricziel/signaldb/commit/7ee6f464d3512df3a2c4fddc06f90302c3c55b16))
* **schema-registry:** accept definition/2 custom registry uploads ([#1823](https://github.com/cedricziel/signaldb/issues/1823)) ([a8c8196](https://github.com/cedricziel/signaldb/commit/a8c819648bd5778478be6e86241802ae4f6f880f))
* **schema:** bundle Docker Stats receiver metrics for the container entity ([#2209](https://github.com/cedricziel/signaldb/issues/2209)) ([b839ed5](https://github.com/cedricziel/signaldb/commit/b839ed56d4743fd15c4912d6f73d77f95c7b3077))
* **schema:** let registry metric definitions declare aliases ([#2188](https://github.com/cedricziel/signaldb/issues/2188)) ([0817836](https://github.com/cedricziel/signaldb/commit/08178369ba741145ea42dc6ff3aeb2517e2ad1bc))
* show span links in the Query IR, MCP get_trace and the trace view ([#2177](https://github.com/cedricziel/signaldb/issues/2177)) ([b873eab](https://github.com/cedricziel/signaldb/commit/b873eabee7f7e4e836a530a3fc75fe5261cafb49))
* **writer:** drop legacy metric tables from the reconciler under the wide layout ([#1908](https://github.com/cedricziel/signaldb/issues/1908)) ([ed8a311](https://github.com/cedricziel/signaldb/commit/ed8a311d9d124e7a754f06c9503c61e75ed61896))
* **writer:** surface off-type attribute values and type-pin conflicts ([#1829](https://github.com/cedricziel/signaldb/issues/1829)) ([b4bbfb4](https://github.com/cedricziel/signaldb/commit/b4bbfb464e9cb3e96c5afe82cc543b9f5beaf401))
* **writer:** transform wire exemplars into metric_exemplars rows ([#1894](https://github.com/cedricziel/signaldb/issues/1894)) ([27ac405](https://github.com/cedricziel/signaldb/commit/27ac4054d78fa3464ac953c4682ec5030d421ed2))
* **writer:** transform wire metrics into the typed metrics layout ([#1893](https://github.com/cedricziel/signaldb/issues/1893)) ([ef96961](https://github.com/cedricziel/signaldb/commit/ef969619aa00fb944cdafcfbedbc6a47a73c28fd))
* **writer:** write warm-index tokens for opted-in typed tables ([#1783](https://github.com/cedricziel/signaldb/issues/1783)) ([c2c1dba](https://github.com/cedricziel/signaldb/commit/c2c1dbab6de9d9175bd4f06615d556f67ed4fe82))


### Bug Fixes

* **acceptor:** acknowledge an exporter's resend of an already-durable batch ([#1814](https://github.com/cedricziel/signaldb/issues/1814)) ([ff52ded](https://github.com/cedricziel/signaldb/commit/ff52deda64b5b803c4040cee0db249a6beb5d7f4))
* **acceptor:** dedup client resends across acceptor replicas and restarts ([#1821](https://github.com/cedricziel/signaldb/issues/1821)) ([98cf40e](https://github.com/cedricziel/signaldb/commit/98cf40e5e6bc7d97f3b43564591df2039b9c8070))
* **common:** clear attribute statistics when a table is recreated ([#2169](https://github.com/cedricziel/signaldb/issues/2169)) ([63e5286](https://github.com/cedricziel/signaldb/commit/63e5286c7d6c5b41b493d0bf98d10c1bed9a8738))
* **compactor:** advertise COMPACTOR_ADVERTISE_ADDR instead of the bind address ([#2107](https://github.com/cedricziel/signaldb/issues/2107)) ([2b8adb7](https://github.com/cedricziel/signaldb/commit/2b8adb7f23df02ee44a7d7f72dde53bcdd436254)), closes [#1844](https://github.com/cedricziel/signaldb/issues/1844)
* **config:** merge a tenant's schema block over the global [schema] ([#2086](https://github.com/cedricziel/signaldb/issues/2086)) ([0832345](https://github.com/cedricziel/signaldb/commit/0832345521808cf865d627580dc1a0621692adcc))
* **discovery:** keep statistics coverage honest about what it bounds ([#2184](https://github.com/cedricziel/signaldb/issues/2184)) ([211cce5](https://github.com/cedricziel/signaldb/commit/211cce55e7d11d533d929f728b903004f605b03d))
* **querier:** keep the correlate trailer compatible across adjacent releases ([#2051](https://github.com/cedricziel/signaldb/issues/2051)) ([706f25e](https://github.com/cedricziel/signaldb/commit/706f25ed1c9d01b5d08ea6b368ff31cd5fc199f7))
* **querier:** resolve label columns by their origin-key doc ([#2183](https://github.com/cedricziel/signaldb/issues/2183)) ([73768e9](https://github.com/cedricziel/signaldb/commit/73768e96e645b50157c04864dac913637f2d1068))
* **router:** read data for sample:true and flag partial discovery statistics ([#2176](https://github.com/cedricziel/signaldb/issues/2176)) ([ab5dcf7](https://github.com/cedricziel/signaldb/commit/ab5dcf78f47567a6e2a8f8a10dcf5dfd06cdfed5))
* **self-monitoring:** give span_error ERROR logs the error text as body ([#2089](https://github.com/cedricziel/signaldb/issues/2089)) ([eb84c7a](https://github.com/cedricziel/signaldb/commit/eb84c7a9a0f60537d2755bd13690ff300ff6844f)), closes [#1825](https://github.com/cedricziel/signaldb/issues/1825)
* **telemetry:** namespace bare log fields flagged by weaver live-check ([#1879](https://github.com/cedricziel/signaldb/issues/1879)) ([90dbf09](https://github.com/cedricziel/signaldb/commit/90dbf09c31f7181f4f97c4a0bdb6c79f0f78aca3)), closes [#912](https://github.com/cedricziel/signaldb/issues/912)
* **type-authority:** expire cached signal scopes so registry edits reach ingest ([#2087](https://github.com/cedricziel/signaldb/issues/2087)) ([670277b](https://github.com/cedricziel/signaldb/commit/670277b61d83e82b81856d1f3b228bfee35b55e0))
* **writer:** retry the legacy metric table purge on converged datasets ([#1949](https://github.com/cedricziel/signaldb/issues/1949)) ([1ec8d3f](https://github.com/cedricziel/signaldb/commit/1ec8d3f9a87f165e2cf7e69bfbe2b4bcd145a1a5))


### Performance Improvements

* **acceptor:** send WAL IPC bytes to the writer without re-encoding ([#2192](https://github.com/cedricziel/signaldb/issues/2192)) ([be7e5ee](https://github.com/cedricziel/signaldb/commit/be7e5ee93f9f539ed86adf476e03fe044ee00768)), closes [#942](https://github.com/cedricziel/signaldb/issues/942)
* **common:** build Flight schemas once per process ([#2190](https://github.com/cedricziel/signaldb/issues/2190)) ([e84c7a4](https://github.com/cedricziel/signaldb/commit/e84c7a4b337b8a5c1132d6c4535c63e009e51feb)), closes [#942](https://github.com/cedricziel/signaldb/issues/942)
* **common:** share registry documents from SchemaResolver::get ([#1819](https://github.com/cedricziel/signaldb/issues/1819)) ([2fe61c1](https://github.com/cedricziel/signaldb/commit/2fe61c16af8956b49c4d223795c0549dc94727e7))
* **common:** stop gRPC-compressing Flight requests ([#2207](https://github.com/cedricziel/signaldb/issues/2207)) ([1ca77d8](https://github.com/cedricziel/signaldb/commit/1ca77d8209a98542dd9749d31980e2522261c8c0)), closes [#942](https://github.com/cedricziel/signaldb/issues/942)
* **compactor:** size the compaction scan batch from bytes per row ([#2181](https://github.com/cedricziel/signaldb/issues/2181)) ([96bf899](https://github.com/cedricziel/signaldb/commit/96bf8994075bd0c01a533b12114c835e03b8fbf5)), closes [#1358](https://github.com/cedricziel/signaldb/issues/1358)
* **querier:** reuse resolved Iceberg tables for a short TTL ([#2162](https://github.com/cedricziel/signaldb/issues/2162)) ([a3ef245](https://github.com/cedricziel/signaldb/commit/a3ef245e5a9e4860367d0b832ec53bf68128c202))
* **querier:** rewrite promoted-attribute filters for row-group pruning ([#1852](https://github.com/cedricziel/signaldb/issues/1852)) ([19cbf31](https://github.com/cedricziel/signaldb/commit/19cbf31284acd0cbf7dbb06c45fc31915dfa01b2))
* **querier:** stream do_get results instead of buffering them ([#2164](https://github.com/cedricziel/signaldb/issues/2164)) ([e3d3087](https://github.com/cedricziel/signaldb/commit/e3d308782ab637f57fd4ac9dc79588aa0cee9a04))
* **router:** decode querier results as they arrive ([#2165](https://github.com/cedricziel/signaldb/issues/2165)) ([60f55b7](https://github.com/cedricziel/signaldb/commit/60f55b7c05267a4f869b0d24c3ded8630fc92a0a)), closes [#938](https://github.com/cedricziel/signaldb/issues/938)


### Code Refactoring

* **common:** drop legacy map-layout attribute reads ([#1808](https://github.com/cedricziel/signaldb/issues/1808)) ([fc4c1f7](https://github.com/cedricziel/signaldb/commit/fc4c1f7beff6a8d1c788e029f7aa94f41aacbd5c))
* **common:** let ServiceBootstrap resolve the advertised address ([#2121](https://github.com/cedricziel/signaldb/issues/2121)) ([6bef83b](https://github.com/cedricziel/signaldb/commit/6bef83b68b873ea7407d0f92981665a2b4cb420e))
* **common:** share schema-evolution commit and verification ([#1855](https://github.com/cedricziel/signaldb/issues/1855)) ([3cb433e](https://github.com/cedricziel/signaldb/commit/3cb433e9aa11f5175f6ba13fbb3b63438b5c4752))
* **common:** share typed-home expression and column helpers ([#1785](https://github.com/cedricziel/signaldb/issues/1785)) ([062e6fc](https://github.com/cedricziel/signaldb/commit/062e6fcd07fdf02958aa9c744f9203e76c416563))
* drop the dead MetricsLayout switch and *_with_layout helpers ([#1961](https://github.com/cedricziel/signaldb/issues/1961)) ([4a98be3](https://github.com/cedricziel/signaldb/commit/4a98be3e108b2c67be847db23de0f768c1ad2a58))
* drop the legacy attr_tokens column ([#1794](https://github.com/cedricziel/signaldb/issues/1794)) ([a4b84a7](https://github.com/cedricziel/signaldb/commit/a4b84a7b7128e822524b5379036ded18873f4fa9))
* **querier:** report correlate bounds as a structured Flight trailer ([#2050](https://github.com/cedricziel/signaldb/issues/2050)) ([d538baf](https://github.com/cedricziel/signaldb/commit/d538baf416f8710b922b3c275a65873edd4cdd86))
* remove the legacy per-type metric schemas.toml sections ([#1968](https://github.com/cedricziel/signaldb/issues/1968)) ([42b62f9](https://github.com/cedricziel/signaldb/commit/42b62f986ce3e4987ab09fa53864fc5dbfe0e69f))
* remove the legacy per-type metric TableSchema variants ([#1967](https://github.com/cedricziel/signaldb/issues/1967)) ([83dbff3](https://github.com/cedricziel/signaldb/commit/83dbff3a2068c10cfeaa153ed2f2ff2d12db3767))
* share typed attribute container helpers ([#1815](https://github.com/cedricziel/signaldb/issues/1815)) ([45be827](https://github.com/cedricziel/signaldb/commit/45be827a12365bca5b39443db66ae3f16764fbc8))


### Tests

* **common:** make catalog-unavailability test root-safe ([#1818](https://github.com/cedricziel/signaldb/issues/1818)) ([caaf23e](https://github.com/cedricziel/signaldb/commit/caaf23e6a4f8964106901851f1fb488f6f9485e1))
* **common:** make the SQLITE_BUSY retry test deterministic ([#1990](https://github.com/cedricziel/signaldb/issues/1990)) ([4570dad](https://github.com/cedricziel/signaldb/commit/4570dadd239cb1ad765fdc7fe00208b5114c0faf))
* derive per-tenant table counts and switch metrics fixtures to wire-format batches ([#1927](https://github.com/cedricziel/signaldb/issues/1927)) ([dbeddc5](https://github.com/cedricziel/signaldb/commit/dbeddc5b78665446fbc7dc2c151816a3bd21feb6))
* **ingest:** pin WAL format and typed ingest before layer-5 enforcement ([#1828](https://github.com/cedricziel/signaldb/issues/1828)) ([a4b0d74](https://github.com/cedricziel/signaldb/commit/a4b0d740cbb4eab0535a2552797996e47047aa80))
* move metric fixtures off the legacy per-type tables ([#1966](https://github.com/cedricziel/signaldb/issues/1966)) ([4f239c7](https://github.com/cedricziel/signaldb/commit/4f239c7da68a4a7db76667044a724b19fe691127))
* **tests-integration:** add an end-to-end metrics cutover test ([#1929](https://github.com/cedricziel/signaldb/issues/1929)) ([bb47677](https://github.com/cedricziel/signaldb/commit/bb4767732f40a82ca1af1e4eab88a8dc11580829))


### Build System

* **deps:** bump object from 0.37.3 to 0.39.1 ([#2203](https://github.com/cedricziel/signaldb/issues/2203)) ([b6de77a](https://github.com/cedricziel/signaldb/commit/b6de77a4378388901382c517ed72e7a93c309c48))
* fix the beta test leg for cargo's unused-dependency lints ([#2047](https://github.com/cedricziel/signaldb/issues/2047)) ([6867d69](https://github.com/cedricziel/signaldb/commit/6867d69dceb26f2e55ccfac31e60ae42aad76418))
</details>

<details><summary>compactor: 0.5.0</summary>

## [0.5.0](https://github.com/cedricziel/signaldb/compare/compactor-v0.4.1...compactor-v0.5.0) (2026-10-08)


###   BREAKING CHANGES

* **common:** drop legacy map-layout attribute reads ([#1808](https://github.com/cedricziel/signaldb/issues/1808))
* remove the legacy attribute map write path ([#1793](https://github.com/cedricziel/signaldb/issues/1793))

### Features

* **acceptor:** target the wide metrics table under the wide layout ([#1907](https://github.com/cedricziel/signaldb/issues/1907)) ([e6fb521](https://github.com/cedricziel/signaldb/commit/e6fb521164fa99ba4ebf987de022696576916cf9))
* **compactor:** backfill typed per-level attribute columns ([#1860](https://github.com/cedricziel/signaldb/issues/1860)) ([855c20f](https://github.com/cedricziel/signaldb/commit/855c20f5ef57c26a979986dbab4935a8d6e771fd))
* **compactor:** count attribute presence and demand per level ([#1841](https://github.com/cedricziel/signaldb/issues/1841)) ([b11170d](https://github.com/cedricziel/signaldb/commit/b11170d5e3e56271f10f159e7c5107efc83ecc62))
* **compactor:** decide typed per-level attribute promotions ([#1862](https://github.com/cedricziel/signaldb/issues/1862)) ([a2e8496](https://github.com/cedricziel/signaldb/commit/a2e849616edc3936614f28d53b17ab2cd3754401))
* **compactor:** demote idle and over-budget promoted attribute columns ([#1867](https://github.com/cedricziel/signaldb/issues/1867)) ([3714e3c](https://github.com/cedricziel/signaldb/commit/3714e3cf4f0c1eb5e65b30d18e9c6b684141758f))
* **compactor:** stop auto-promoting new label columns ([#1861](https://github.com/cedricziel/signaldb/issues/1861)) ([3d53b1e](https://github.com/cedricziel/signaldb/commit/3d53b1e5338c5f555c9660e860411f2d3dba75a4))
* **router:** report the retention that applies to a query ([#2182](https://github.com/cedricziel/signaldb/issues/2182)) ([91ca14d](https://github.com/cedricziel/signaldb/commit/91ca14dfc6f4478155ef9095c1b84e4078a6da90))
* **writer:** write warm-index tokens for opted-in typed tables ([#1783](https://github.com/cedricziel/signaldb/issues/1783)) ([c2c1dba](https://github.com/cedricziel/signaldb/commit/c2c1dbab6de9d9175bd4f06615d556f67ed4fe82))


### Bug Fixes

* **common:** clear attribute statistics when a table is recreated ([#2169](https://github.com/cedricziel/signaldb/issues/2169)) ([63e5286](https://github.com/cedricziel/signaldb/commit/63e5286c7d6c5b41b493d0bf98d10c1bed9a8738))
* **compactor:** advertise COMPACTOR_ADVERTISE_ADDR instead of the bind address ([#2107](https://github.com/cedricziel/signaldb/issues/2107)) ([2b8adb7](https://github.com/cedricziel/signaldb/commit/2b8adb7f23df02ee44a7d7f72dde53bcdd436254)), closes [#1844](https://github.com/cedricziel/signaldb/issues/1844)
* **discovery:** keep statistics coverage honest about what it bounds ([#2184](https://github.com/cedricziel/signaldb/issues/2184)) ([211cce5](https://github.com/cedricziel/signaldb/commit/211cce55e7d11d533d929f728b903004f605b03d))
* **router:** read data for sample:true and flag partial discovery statistics ([#2176](https://github.com/cedricziel/signaldb/issues/2176)) ([ab5dcf7](https://github.com/cedricziel/signaldb/commit/ab5dcf78f47567a6e2a8f8a10dcf5dfd06cdfed5))
* **telemetry:** namespace bare log fields flagged by weaver live-check ([#1879](https://github.com/cedricziel/signaldb/issues/1879)) ([90dbf09](https://github.com/cedricziel/signaldb/commit/90dbf09c31f7181f4f97c4a0bdb6c79f0f78aca3)), closes [#912](https://github.com/cedricziel/signaldb/issues/912)


### Performance Improvements

* **compactor:** size the compaction scan batch from bytes per row ([#2181](https://github.com/cedricziel/signaldb/issues/2181)) ([96bf899](https://github.com/cedricziel/signaldb/commit/96bf8994075bd0c01a533b12114c835e03b8fbf5)), closes [#1358](https://github.com/cedricziel/signaldb/issues/1358)


### Code Refactoring

* **common:** drop legacy map-layout attribute reads ([#1808](https://github.com/cedricziel/signaldb/issues/1808)) ([fc4c1f7](https://github.com/cedricziel/signaldb/commit/fc4c1f7beff6a8d1c788e029f7aa94f41aacbd5c))
* **common:** let ServiceBootstrap resolve the advertised address ([#2121](https://github.com/cedricziel/signaldb/issues/2121)) ([6bef83b](https://github.com/cedricziel/signaldb/commit/6bef83b68b873ea7407d0f92981665a2b4cb420e))
* remove the legacy attribute map write path ([#1793](https://github.com/cedricziel/signaldb/issues/1793)) ([9962ba9](https://github.com/cedricziel/signaldb/commit/9962ba962ca3da95107442dd0d7140cb0b2fd98e))


### Tests

* **compactor,router:** convert legacy attribute fixtures to the typed layout ([#1807](https://github.com/cedricziel/signaldb/issues/1807)) ([c9f06d1](https://github.com/cedricziel/signaldb/commit/c9f06d16ac6e6b1c0c610a4df40a8e67d02dea8b))
* move metric fixtures off the legacy per-type tables ([#1966](https://github.com/cedricziel/signaldb/issues/1966)) ([4f239c7](https://github.com/cedricziel/signaldb/commit/4f239c7da68a4a7db76667044a724b19fe691127))


### Build System

* **deps:** bump object from 0.37.3 to 0.39.1 ([#2203](https://github.com/cedricziel/signaldb/issues/2203)) ([b6de77a](https://github.com/cedricziel/signaldb/commit/b6de77a4378388901382c517ed72e7a93c309c48))
* fix the beta test leg for cargo's unused-dependency lints ([#2047](https://github.com/cedricziel/signaldb/issues/2047)) ([6867d69](https://github.com/cedricziel/signaldb/commit/6867d69dceb26f2e55ccfac31e60ae42aad76418))
</details>

<details><summary>querier: 0.5.0</summary>

## [0.5.0](https://github.com/cedricziel/signaldb/compare/querier-v0.4.1...querier-v0.5.0) (2026-10-08)


###   BREAKING CHANGES

* **traceql:** `Condition` has a new public `op` field, so code that constructs one must set it.
* **querier:** a standalone querier with no `memory_limit_mb` is now bounded (set 0 to opt out), and a tenant running more than 8 concurrent queries gets RESOURCE_EXHAUSTED unless `max_concurrent_queries_per_tenant` is raised (0 = unlimited).
* `"from": "metrics_histogram"` is rejected as an unknown source. Use `"from": "metrics"`; add a `metric.type = histogram` filter where only histogram rows are wanted. `histogram_quantile` on `metrics` already reads histogram rows only.
* **querier:** drop the IR planner's legacy attribute paths ([#1813](https://github.com/cedricziel/signaldb/issues/1813))
* **common:** drop legacy map-layout attribute reads ([#1808](https://github.com/cedricziel/signaldb/issues/1808))
* remove the legacy attribute map write path ([#1793](https://github.com/cedricziel/signaldb/issues/1793))
* drop the legacy attr_tokens column ([#1794](https://github.com/cedricziel/signaldb/issues/1794))
* existing tables still in the legacy map<string,string> attribute layout are dropped and recreated in the typed layout the next time they are loaded; pre-cutover data in those tables is not migrated.

### Features

* **common:** source-aware attribute qualifier shared with the planner ([#2073](https://github.com/cedricziel/signaldb/issues/2073)) ([a0ec69f](https://github.com/cedricziel/signaldb/commit/a0ec69ff596d0033ce7064e9cedbb6dd8ef6790e))
* cut attribute storage over to the typed layout ([#1791](https://github.com/cedricziel/signaldb/issues/1791)) ([79b1fff](https://github.com/cedricziel/signaldb/commit/79b1fff7184ee1b3a97dfaf6fbf2d994a6199b39))
* **querier:** add histogram_avg, histogram_stddev and histogram_stdvar ([#2187](https://github.com/cedricziel/signaldb/issues/2187)) ([626a417](https://github.com/cedricziel/signaldb/commit/626a41777f5393eb364a00b9fa211cd674f2eb09))
* **querier:** add the exemplars IR source over metric_exemplars ([#1946](https://github.com/cedricziel/signaldb/issues/1946)) ([5c2c348](https://github.com/cedricziel/signaldb/commit/5c2c3484fc7e4f25219ab05e0813ca0b9c4e3c0c))
* **querier:** address colliding point attributes with the point. qualifier ([#1994](https://github.com/cedricziel/signaldb/issues/1994)) ([b68014f](https://github.com/cedricziel/signaldb/commit/b68014f97aec27ffa81bdb6d62992c76404cd184))
* **querier:** bound a Query IR page to a live-tail window ([#2146](https://github.com/cedricziel/signaldb/issues/2146)) ([10421b0](https://github.com/cedricziel/signaldb/commit/10421b0ec2a787b62f515ba61b8e193787e5940b))
* **querier:** bound memory and per-tenant concurrency by default ([#2161](https://github.com/cedricziel/signaldb/issues/2161)) ([93dc0d9](https://github.com/cedricziel/signaldb/commit/93dc0d9e60c84ddfc6fb876b09e40be4e7b305cc))
* **querier:** canonical series label sets for metric Series ([#1993](https://github.com/cedricziel/signaldb/issues/1993)) ([df5ef6d](https://github.com/cedricziel/signaldb/commit/df5ef6d58827d43c3c7d52cab5db5dd17d6795bc))
* **querier:** correlate to another signal with inner and left joins ([#2053](https://github.com/cedricziel/signaldb/issues/2053)) ([e25f6da](https://github.com/cedricziel/signaldb/commit/e25f6da0ad24aa0b8e35c54a3e13dbf3126b0e8c))
* **querier:** count match traces cut by the query range ([#2091](https://github.com/cedricziel/signaldb/issues/2091)) ([932817d](https://github.com/cedricziel/signaldb/commit/932817d7f0c657e006233b50d78ac4dc21fa9b3f))
* **querier:** covering_instants UDF for evaluation-instant windows ([#1959](https://github.com/cedricziel/signaldb/issues/1959)) ([65f9c13](https://github.com/cedricziel/signaldb/commit/65f9c13dea3b0bd005c8bc8b03dfaa09ffc80775))
* **querier:** cut a page off a sorted Query IR result ([#2138](https://github.com/cedricziel/signaldb/issues/2138)) ([3dfe244](https://github.com/cedricziel/signaldb/commit/3dfe24454916835dad0025181660e13b78096303))
* **querier:** decode histogram buckets through a layout-agnostic BucketCol ([#1901](https://github.com/cedricziel/signaldb/issues/1901)) ([699f111](https://github.com/cedricziel/signaldb/commit/699f11135a32642c88ae09391fae0ae49bb189e7))
* **querier:** execute a Query IR ticket's page ([#2139](https://github.com/cedricziel/signaldb/issues/2139)) ([32a3f27](https://github.com/cedricziel/signaldb/commit/32a3f271e3e737a26587f702a217d4c7b69ae323))
* **querier:** execute the IR absent and over_time stages ([#2025](https://github.com/cedricziel/signaldb/issues/2025)) ([154ef0a](https://github.com/cedricziel/signaldb/commit/154ef0af783447c1a8d725e811494d53aa88d129))
* **querier:** execute the IR binop stage with a number operand ([#2026](https://github.com/cedricziel/signaldb/issues/2026)) ([30899f5](https://github.com/cedricziel/signaldb/commit/30899f54e71f889a10f067d17c4c51e51e0179ed))
* **querier:** execute the IR binop stage with a sub-document operand ([#2027](https://github.com/cedricziel/signaldb/issues/2027)) ([d74af7b](https://github.com/cedricziel/signaldb/commit/d74af7bd2eccdce2cab81dc610dfb04eff0bba28))
* **querier:** execute the IR histogram_fraction stage ([#2030](https://github.com/cedricziel/signaldb/issues/2030)) ([1e20520](https://github.com/cedricziel/signaldb/commit/1e205201efaf805380885ffc315eb8da104e66b7))
* **querier:** execute the IR labels stage over metric Series ([#2021](https://github.com/cedricziel/signaldb/issues/2021)) ([d866522](https://github.com/cedricziel/signaldb/commit/d86652211b4757176834381aedcc486ac0e59785))
* **querier:** execute the IR map and filter stages over metric Series ([#2022](https://github.com/cedricziel/signaldb/issues/2022)) ([6bf0b4e](https://github.com/cedricziel/signaldb/commit/6bf0b4edab8867de1206ead1f9b127b77145e811))
* **querier:** execute the IR reduce stage over metric Series ([#2024](https://github.com/cedricziel/signaldb/issues/2024)) ([4ee0463](https://github.com/cedricziel/signaldb/commit/4ee0463d2888d49e9635e725f2cdda79b249f4d9))
* **querier:** execute the IR sort stage over metric Series ([#2023](https://github.com/cedricziel/signaldb/issues/2023)) ([61bd207](https://github.com/cedricziel/signaldb/commit/61bd207af93016b215bdbb5a49c90d91ae213600))
* **querier:** exponential histogram math for metric ops ([#1955](https://github.com/cedricziel/signaldb/issues/1955)) ([d08a2cb](https://github.com/cedricziel/signaldb/commit/d08a2cb14b9ca866f30ad5bdd45e50aa2622b27b))
* **querier:** expose every metric type through the IR metrics source ([#1942](https://github.com/cedricziel/signaldb/issues/1942)) ([f5bf726](https://github.com/cedricziel/signaldb/commit/f5bf7263964d1ccf143b8aae756eef6aacd3d4cd))
* **querier:** fetch one row past a Query IR page and bound trace pages ([#2140](https://github.com/cedricziel/signaldb/issues/2140)) ([5152fa4](https://github.com/cedricziel/signaldb/commit/5152fa479d4ca715f0be0eb6144c4950180648c2))
* **querier:** fold binop operand batches into matching state ([#1978](https://github.com/cedricziel/signaldb/issues/1978)) ([06271c0](https://github.com/cedricziel/signaldb/commit/06271c019643a4f6be83da970154bd9b505f6286))
* **querier:** histogram UDAF over typed explicit and exponential buckets ([#1964](https://github.com/cedricziel/signaldb/issues/1964)) ([b7041bd](https://github.com/cedricziel/signaldb/commit/b7041bdfed5582c77500c7e70dd84cbfc53f0b64))
* **querier:** honour OTLP staleness markers in the sample stage ([#1999](https://github.com/cedricziel/signaldb/issues/1999)) ([44cbacc](https://github.com/cedricziel/signaldb/commit/44cbacc18eea32cdfd1b619671e73f5b9e98c2d8))
* **querier:** IR histogram_quantile honours an instant-mode lookback ([#2016](https://github.com/cedricziel/signaldb/issues/2016)) ([a7f4e6e](https://github.com/cedricziel/signaldb/commit/a7f4e6e88dd1fd9e01e4e53ed1e485deb6823750))
* **querier:** label-set UDFs for metric Series ([#1995](https://github.com/cedricziel/signaldb/issues/1995)) ([e0c6247](https://github.com/cedricziel/signaldb/commit/e0c6247c1db1e73f7e6b7cba5d41bd9069d6d97d))
* **querier:** merge explicit histograms with different bounds on their union ([#2014](https://github.com/cedricziel/signaldb/issues/2014)) ([06a4425](https://github.com/cedricziel/signaldb/commit/06a4425733859ebdc71bc323fd8baedd45a990fe))
* **querier:** per-bucket vector matching core for the binop stage ([#1977](https://github.com/cedricziel/signaldb/issues/1977)) ([522328f](https://github.com/cedricziel/signaldb/commit/522328fc8474af41f7989515c34b7883615a9f07))
* **querier:** per-series histogram rate and merge math ([#1962](https://github.com/cedricziel/signaldb/issues/1962)) ([6d9bcc8](https://github.com/cedricziel/signaldb/commit/6d9bcc8c9be281bb7c4e9c40363673999bef8064))
* **querier:** plan histogram statistics over evaluation instants ([#2005](https://github.com/cedricziel/signaldb/issues/2005)) ([b2842a2](https://github.com/cedricziel/signaldb/commit/b2842a234a3be0b8e8267e8ecce84e40080d3a1b))
* **querier:** plan range functions over evaluation instants per series ([#2008](https://github.com/cedricziel/signaldb/issues/2008)) ([62a1162](https://github.com/cedricziel/signaldb/commit/62a1162dfcb84468f688f6488db03af8ebf5c01f))
* **querier:** plan scalar, vector and the time/constant pseudo-sources ([#1998](https://github.com/cedricziel/signaldb/issues/1998)) ([14a9a22](https://github.com/cedricziel/signaldb/commit/14a9a228a199d4898ccb01e05acd685bc37f8a1c))
* **querier:** plan the sample stage into a metric Series frame ([#1997](https://github.com/cedricziel/signaldb/issues/1997)) ([08be3ae](https://github.com/cedricziel/signaldb/commit/08be3aed3be4f04a2f7a39c17f26b6026e0c37fc))
* **querier:** probe warm-index blooms to prune data files ([#1787](https://github.com/cedricziel/signaldb/issues/1787)) ([a705ffd](https://github.com/cedricziel/signaldb/commit/a705ffd185be6b9a879a6594637eb5f03024bb20))
* **querier:** prune scans with the warm containment index ([#1788](https://github.com/cedricziel/signaldb/issues/1788)) ([46acbe2](https://github.com/cedricziel/signaldb/commit/46acbe26eb091c1e800333d22356d9e6f6efaacd))
* **querier:** read IR metrics from the typed metrics layout ([#1903](https://github.com/cedricziel/signaldb/issues/1903)) ([2454e5c](https://github.com/cedricziel/signaldb/commit/2454e5c5a5bf176ebea7ebb0ebdadec4452c01b3))
* **querier:** read per-level typed promoted attribute columns ([#1848](https://github.com/cedricziel/signaldb/issues/1848)) ([dbcf51b](https://github.com/cedricziel/signaldb/commit/dbcf51b3bf5f8a09c451e57e236d903329983cf9))
* **querier:** read PromQL metrics from the typed metrics layout ([#1902](https://github.com/cedricziel/signaldb/issues/1902)) ([aba7fa1](https://github.com/cedricziel/signaldb/commit/aba7fa1480c9006e0c64c6c6cc9ed4f78c33bafb))
* **querier:** recognize warm-index probes in typed attribute filters ([#1786](https://github.com/cedricziel/signaldb/issues/1786)) ([df5f82f](https://github.com/cedricziel/signaldb/commit/df5f82f91fdd99d1239a7e5dd0a2a40771dadc66))
* **querier:** record per-level attribute demand from IR queries ([#1854](https://github.com/cedricziel/signaldb/issues/1854)) ([885864f](https://github.com/cedricziel/signaldb/commit/885864f162ca3f0f4fa1cce01f87cbb7a9ac3630))
* **querier:** run histogram_quantile on the metrics source and reject summaries ([#1943](https://github.com/cedricziel/signaldb/issues/1943)) ([e233913](https://github.com/cedricziel/signaldb/commit/e233913119f124d588b5b55634b58ae868ad29a0))
* **querier:** temporality-aware range math for metric series ([#1958](https://github.com/cedricziel/signaldb/issues/1958)) ([5e69f41](https://github.com/cedricziel/signaldb/commit/5e69f4105cf934122e13bde071a3a43d07b5bba3))
* **querier:** typed histogram point parsing and accumulator state ([#1963](https://github.com/cedricziel/signaldb/issues/1963)) ([a74778f](https://github.com/cedricziel/signaldb/commit/a74778f919cf2b7461e3dda5645a6b38a50a8fd4))
* **querier:** VectorMatchNode/Exec on a shared querier query planner ([#1979](https://github.com/cedricziel/signaldb/issues/1979)) ([f88f18e](https://github.com/cedricziel/signaldb/commit/f88f18e6ccf61a8741cbd1a324747ae9f9daf4e5))
* **querier:** windowed range accumulator UDF ([#1960](https://github.com/cedricziel/signaldb/issues/1960)) ([10118db](https://github.com/cedricziel/signaldb/commit/10118dbe25d59541457507dd4a63300db63607d6))
* **query-ir:** add count_distinct aggregate (IR v9) ([#1831](https://github.com/cedricziel/signaldb/issues/1831)) ([0a403ac](https://github.com/cedricziel/signaldb/commit/0a403ac3e5292657a5a88513a5d4ea1d1e0abd4b))
* **query-ir:** add document-level page and tail with their validation ([#2133](https://github.com/cedricziel/signaldb/issues/2133)) ([32732f4](https://github.com/cedricziel/signaldb/commit/32732f49d64d32e1e42929ee9836e2e2b4cd3ab3))
* **query-ir:** add histogram_fraction and the histogram window ([#1975](https://github.com/cedricziel/signaldb/issues/1975)) ([6034fe9](https://github.com/cedricziel/signaldb/commit/6034fe9e346bf9d5852c4277686a6700fc68457c))
* **query-ir:** add metric point streams and the Scalar relation at irVersion 10 ([#1969](https://github.com/cedricziel/signaldb/issues/1969)) ([2b529a2](https://github.com/cedricziel/signaldb/commit/2b529a28a91eacdc0b3cc8ae0d503580adcf05d8))
* **query-ir:** add the binop stage with vector matching ([#1974](https://github.com/cedricziel/signaldb/issues/1974)) ([cb457df](https://github.com/cedricziel/signaldb/commit/cb457df53660e5d36bb1bb32037da83f495a94f8))
* **query-ir:** add the document step and the time/constant pseudo-sources ([#1970](https://github.com/cedricziel/signaldb/issues/1970)) ([4da9e65](https://github.com/cedricziel/signaldb/commit/4da9e657c71d2d47d9f221e9021fbc1878614ad2))
* **query-ir:** add the filter, sort, absent and over_time series stages ([#1973](https://github.com/cedricziel/signaldb/issues/1973)) ([cd43346](https://github.com/cedricziel/signaldb/commit/cd433463aaccaa1a3ac7798040aaa8eb369840d6))
* **query-ir:** add the reduce, map and labels series stages ([#1972](https://github.com/cedricziel/signaldb/issues/1972)) ([065acd1](https://github.com/cedricziel/signaldb/commit/065acd1c5450f3d683cc164f951e272d2e83822a))
* **query-ir:** correlate to another signal with semi and anti joins (irVersion 11) ([#2052](https://github.com/cedricziel/signaldb/issues/2052)) ([ad0cd64](https://github.com/cedricziel/signaldb/commit/ad0cd644101af8f140dd7ae89fdc09f94dab9ca2))
* **query-ir:** differential flamegraph over a baseline window (irVersion 13) ([#2100](https://github.com/cedricziel/signaldb/issues/2100)) ([5e36ddb](https://github.com/cedricziel/signaldb/commit/5e36ddbea6b114c7c2077eca2ee28b9b0fa8089f))
* **query-ir:** filter trace spans on their events and links ([#2062](https://github.com/cedricziel/signaldb/issues/2062)) ([db966c5](https://github.com/cedricziel/signaldb/commit/db966c593dc3cb1d0e244a579f81c781c19e5d78))
* **query-ir:** pin the label semantics PromQL and the IR share ([#1982](https://github.com/cedricziel/signaldb/issues/1982)) ([2cc5dac](https://github.com/cedricziel/signaldb/commit/2cc5dac65a921d141de4dd6f6d6cdfca12684dd2))
* **query-ir:** structural match stage over traces (irVersion 12) ([#2065](https://github.com/cedricziel/signaldb/issues/2065)) ([c34ee37](https://github.com/cedricziel/signaldb/commit/c34ee3789ae0b695f0e2a052e5af28adb1d2e284))
* remove the metrics_histogram IR source ([#1945](https://github.com/cedricziel/signaldb/issues/1945)) ([c16f82c](https://github.com/cedricziel/signaldb/commit/c16f82c100199ef0f297e366208c07d8242501c5))
* **router:** list discovery fields with their canonical authority type ([#2075](https://github.com/cedricziel/signaldb/issues/2075)) ([621aa46](https://github.com/cedricziel/signaldb/commit/621aa4674399a628470b73810e033963288a3866))
* **router:** warn match_incomplete_trace from the query report trailer ([#2090](https://github.com/cedricziel/signaldb/issues/2090)) ([7ee6f46](https://github.com/cedricziel/signaldb/commit/7ee6f464d3512df3a2c4fddc06f90302c3c55b16))
* show span links in the Query IR, MCP get_trace and the trace view ([#2177](https://github.com/cedricziel/signaldb/issues/2177)) ([b873eab](https://github.com/cedricziel/signaldb/commit/b873eabee7f7e4e836a530a3fc75fe5261cafb49))
* **traceql:** support !=, =~ and !~ and surface search errors over MCP ([#2178](https://github.com/cedricziel/signaldb/issues/2178)) ([ce929f5](https://github.com/cedricziel/signaldb/commit/ce929f5f321e5d826e2ba43edc67d7e26de67659))


### Bug Fixes

* **compactor:** advertise COMPACTOR_ADVERTISE_ADDR instead of the bind address ([#2107](https://github.com/cedricziel/signaldb/issues/2107)) ([2b8adb7](https://github.com/cedricziel/signaldb/commit/2b8adb7f23df02ee44a7d7f72dde53bcdd436254)), closes [#1844](https://github.com/cedricziel/signaldb/issues/1844)
* correlate follow-ups from layer 9 (version error, target attribute demand) ([#2061](https://github.com/cedricziel/signaldb/issues/2061)) ([cf31200](https://github.com/cedricziel/signaldb/commit/cf3120059404ad3f6c00beb1321c06ce31648d4b))
* **querier:** address metric Series review findings ([#2003](https://github.com/cedricziel/signaldb/issues/2003)) ([e8a13d1](https://github.com/cedricziel/signaldb/commit/e8a13d12226e7a63162a6e2539cd4b7ee7b9785d))
* **querier:** bound stepped metrics aggregates on the bare timestamp column ([#2129](https://github.com/cedricziel/signaldb/issues/2129)) ([596c5b3](https://github.com/cedricziel/signaldb/commit/596c5b322c3bd4af45da9fd4213250172c80eea3))
* **querier:** correlate target attribute demand and typed resolution on a missing target ([#2071](https://github.com/cedricziel/signaldb/issues/2071)) ([15e8cdc](https://github.com/cedricziel/signaldb/commit/15e8cdcd6cb83a3590809f61c24aabebf9ec1667))
* **querier:** count cut traces missing a span-set in match_incomplete_trace ([#2130](https://github.com/cedricziel/signaldb/issues/2130)) ([6ddc7a3](https://github.com/cedricziel/signaldb/commit/6ddc7a35491ff36c2a1f3ca21cf4cbe09626a5a8))
* **querier:** derive a series identity where series_id is missing ([#2015](https://github.com/cedricziel/signaldb/issues/2015)) ([5a578e4](https://github.com/cedricziel/signaldb/commit/5a578e42024b7d25d27bdfd598624a220584f992))
* **querier:** histogram statistics skip unreadable rows and read histograms only ([#2013](https://github.com/cedricziel/signaldb/issues/2013)) ([0233f48](https://github.com/cedricziel/signaldb/commit/0233f48b850c65ef6d7a290dc0b31c1e734e2388))
* **querier:** histogram_fraction parity with Prometheus ([#2038](https://github.com/cedricziel/signaldb/issues/2038)) ([8cd9ccb](https://github.com/cedricziel/signaldb/commit/8cd9ccbc5771212327a9992e4089841c95b68722))
* **querier:** IR histogram_quantile differences each series against itself ([#2006](https://github.com/cedricziel/signaldb/issues/2006)) ([1a31fab](https://github.com/cedricziel/signaldb/commit/1a31fabb4f1db2b661374b410a1b7e73e8221d05))
* **querier:** IR metric operator errors are 400s, checked at plan time ([#2011](https://github.com/cedricziel/signaldb/issues/2011)) ([381b6a8](https://github.com/cedricziel/signaldb/commit/381b6a8aef42ac61d77a77962d577884635fe9be))
* **querier:** IR range aggregates difference each series against itself ([#2009](https://github.com/cedricziel/signaldb/issues/2009)) ([4a1cbc4](https://github.com/cedricziel/signaldb/commit/4a1cbc4a7a9ff455de450f8db419f76496f60440))
* **querier:** keep any QuerierError raised inside execution ([#2064](https://github.com/cedricziel/signaldb/issues/2064)) ([bd6d43d](https://github.com/cedricziel/signaldb/commit/bd6d43d49084c18d701b92f371b2202292534b9c))
* **querier:** keep the correlate trailer compatible across adjacent releases ([#2051](https://github.com/cedricziel/signaldb/issues/2051)) ([706f25e](https://github.com/cedricziel/signaldb/commit/706f25ed1c9d01b5d08ea6b368ff31cd5fc199f7))
* **querier:** missing-table and subquery-instant fixes for IR Series ([#2032](https://github.com/cedricziel/signaldb/issues/2032)) ([926d644](https://github.com/cedricziel/signaldb/commit/926d644b59347021fa65b6605a4b78512fec8af1))
* **querier:** pointwise temporality and value-drop resets in range math ([#2012](https://github.com/cedricziel/signaldb/issues/2012)) ([b1775e5](https://github.com/cedricziel/signaldb/commit/b1775e566a0ffbe5bb75c172ed39cebda6d97e85))
* **querier:** Prometheus parity for quantile, absent, topk and the step limit ([#2035](https://github.com/cedricziel/signaldb/issues/2035)) ([0cdcf28](https://github.com/cedricziel/signaldb/commit/0cdcf28237122e6db38db5a392bce9c3ddb8668f))
* **querier:** PromQL histogram values on the step buckets, with @ and a 5m lookback ([#2017](https://github.com/cedricziel/signaldb/issues/2017)) ([11d1b58](https://github.com/cedricziel/signaldb/commit/11d1b58d116f578c6120910e2ee2c7856b5b416c))
* **querier:** read the partition hour a sample window opens in ([#2028](https://github.com/cedricziel/signaldb/issues/2028)) ([4691ad7](https://github.com/cedricziel/signaldb/commit/4691ad79db7795fa40dd6d30aa4ef0d2a7966489))
* **querier:** reject histogram_quantile over Summary metrics ([#2072](https://github.com/cedricziel/signaldb/issues/2072)) ([aa3cb05](https://github.com/cedricziel/signaldb/commit/aa3cb05087419b8a58e0160a3cf542f789be739e))
* **querier:** reject Series that collapse to one labelset ([#2033](https://github.com/cedricziel/signaldb/issues/2033)) ([1db9ca0](https://github.com/cedricziel/signaldb/commit/1db9ca0e771b4a1f9def2091737ef8c99877d850))
* **querier:** resolve each table once per query ([#2175](https://github.com/cedricziel/signaldb/issues/2175)) ([a2c24ee](https://github.com/cedricziel/signaldb/commit/a2c24eea5f6a5024afe0dde74079c878b11bd90d)), closes [#949](https://github.com/cedricziel/signaldb/issues/949)
* **querier:** resolve label columns by their origin-key doc ([#2183](https://github.com/cedricziel/signaldb/issues/2183)) ([73768e9](https://github.com/cedricziel/signaldb/commit/73768e96e645b50157c04864dac913637f2d1068))
* **querier:** run Series stages over a histogram_quantile ([#2034](https://github.com/cedricziel/signaldb/issues/2034)) ([f8badb4](https://github.com/cedricziel/signaldb/commit/f8badb4790055f1811e49244a4345ef3e7b1adab))
* **querier:** surface InvalidInput raised inside DataFusion execution as a client error ([#1957](https://github.com/cedricziel/signaldb/issues/1957)) ([63b2c91](https://github.com/cedricziel/signaldb/commit/63b2c915289b7aa23775fc47a3fd01e413819db6))
* **query-ir:** keep the newest flamegraph profiles and reject inverted windows ([#2098](https://github.com/cedricziel/signaldb/issues/2098)) ([dda9bad](https://github.com/cedricziel/signaldb/commit/dda9bad2c86140ee9be861ee0e4cd5f2788cf8a9))
* **query-ir:** resolve trace.id/span.id on traces and logs, name bad predicates over MCP ([#2206](https://github.com/cedricziel/signaldb/issues/2206)) ([b462073](https://github.com/cedricziel/signaldb/commit/b462073340a278c6ac5f4335afe4f18b5398ebce))
* **router:** read data for sample:true and flag partial discovery statistics ([#2176](https://github.com/cedricziel/signaldb/issues/2176)) ([ab5dcf7](https://github.com/cedricziel/signaldb/commit/ab5dcf78f47567a6e2a8f8a10dcf5dfd06cdfed5))
* **telemetry:** namespace bare log fields flagged by weaver live-check ([#1879](https://github.com/cedricziel/signaldb/issues/1879)) ([90dbf09](https://github.com/cedricziel/signaldb/commit/90dbf09c31f7181f4f97c4a0bdb6c79f0f78aca3)), closes [#912](https://github.com/cedricziel/signaldb/issues/912)


### Performance Improvements

* **querier:** reuse resolved Iceberg tables for a short TTL ([#2162](https://github.com/cedricziel/signaldb/issues/2162)) ([a3ef245](https://github.com/cedricziel/signaldb/commit/a3ef245e5a9e4860367d0b832ec53bf68128c202))
* **querier:** rewrite promoted-attribute filters for row-group pruning ([#1852](https://github.com/cedricziel/signaldb/issues/1852)) ([19cbf31](https://github.com/cedricziel/signaldb/commit/19cbf31284acd0cbf7dbb06c45fc31915dfa01b2))
* **querier:** stream do_get results instead of buffering them ([#2164](https://github.com/cedricziel/signaldb/issues/2164)) ([e3d3087](https://github.com/cedricziel/signaldb/commit/e3d308782ab637f57fd4ac9dc79588aa0cee9a04))


### Documentation

* describe PromQL execution through the Query IR ([#2042](https://github.com/cedricziel/signaldb/issues/2042)) ([3375b34](https://github.com/cedricziel/signaldb/commit/3375b344ff68b1c6f1b546bf36f7de00dd508357))
* **openspec:** explore materialized ancestry for structural matching (for review) ([#2092](https://github.com/cedricziel/signaldb/issues/2092)) ([282a19c](https://github.com/cedricziel/signaldb/commit/282a19c6a7414328b0de982e930991002bfc6f56))


### Code Refactoring

* **common:** drop legacy map-layout attribute reads ([#1808](https://github.com/cedricziel/signaldb/issues/1808)) ([fc4c1f7](https://github.com/cedricziel/signaldb/commit/fc4c1f7beff6a8d1c788e029f7aa94f41aacbd5c))
* **common:** let ServiceBootstrap resolve the advertised address ([#2121](https://github.com/cedricziel/signaldb/issues/2121)) ([6bef83b](https://github.com/cedricziel/signaldb/commit/6bef83b68b873ea7407d0f92981665a2b4cb420e))
* **common:** share typed-home expression and column helpers ([#1785](https://github.com/cedricziel/signaldb/issues/1785)) ([062e6fc](https://github.com/cedricziel/signaldb/commit/062e6fcd07fdf02958aa9c744f9203e76c416563))
* drop the legacy attr_tokens column ([#1794](https://github.com/cedricziel/signaldb/issues/1794)) ([a4b84a7](https://github.com/cedricziel/signaldb/commit/a4b84a7b7128e822524b5379036ded18873f4fa9))
* **querier:** decode histogram buckets from typed list columns only ([#1937](https://github.com/cedricziel/signaldb/issues/1937)) ([2144d7a](https://github.com/cedricziel/signaldb/commit/2144d7ad283956ac94fc015f879c723534330712))
* **querier:** define the metrics IR sources over the metrics table ([#1940](https://github.com/cedricziel/signaldb/issues/1940)) ([4a26403](https://github.com/cedricziel/signaldb/commit/4a26403a3e606dfdd69ad55842e359b08f4bea31))
* **querier:** delete the IR tests pinned to the legacy metrics union ([#1938](https://github.com/cedricziel/signaldb/issues/1938)) ([210924f](https://github.com/cedricziel/signaldb/commit/210924f0def29e44a1a8c6eff84fdc9916840cab))
* **querier:** drop the IR planner's legacy attribute paths ([#1813](https://github.com/cedricziel/signaldb/issues/1813)) ([6240b2d](https://github.com/cedricziel/signaldb/commit/6240b2dd958260556ddda1661dc5ed79b224dbb2))
* **querier:** drop the legacy PromQL evaluator ([#2044](https://github.com/cedricziel/signaldb/issues/2044)) ([7b0c7fb](https://github.com/cedricziel/signaldb/commit/7b0c7fbde50d51c352c03e08be30cb3e55c74cc4))
* **querier:** read PromQL metrics only from the wide metrics table ([#1936](https://github.com/cedricziel/signaldb/issues/1936)) ([94c6614](https://github.com/cedricziel/signaldb/commit/94c661405e9308640e30d10884f8b7af7c804369))
* **querier:** report correlate bounds as a structured Flight trailer ([#2050](https://github.com/cedricziel/signaldb/issues/2050)) ([d538baf](https://github.com/cedricziel/signaldb/commit/d538baf416f8710b922b3c275a65873edd4cdd86))
* **querier:** run the IR metrics tests against the wide table only ([#1939](https://github.com/cedricziel/signaldb/issues/1939)) ([41dfdc4](https://github.com/cedricziel/signaldb/commit/41dfdc451514b8e753afd35c13ed572001efaf69))
* **querier:** serve metric metadata from MetricMetadataService ([#2043](https://github.com/cedricziel/signaldb/issues/2043)) ([98111fb](https://github.com/cedricziel/signaldb/commit/98111fb8674db6e24cd719e0bb3cd235ac54d3a2))
* remove the legacy attribute map write path ([#1793](https://github.com/cedricziel/signaldb/issues/1793)) ([9962ba9](https://github.com/cedricziel/signaldb/commit/9962ba962ca3da95107442dd0d7140cb0b2fd98e))
* share typed attribute container helpers ([#1815](https://github.com/cedricziel/signaldb/issues/1815)) ([45be827](https://github.com/cedricziel/signaldb/commit/45be827a12365bca5b39443db66ae3f16764fbc8))


### Tests

* move metric fixtures off the legacy per-type tables ([#1966](https://github.com/cedricziel/signaldb/issues/1966)) ([4f239c7](https://github.com/cedricziel/signaldb/commit/4f239c7da68a4a7db76667044a724b19fe691127))
* **querier:** assert promotion is invariant across typed attributes ([#1849](https://github.com/cedricziel/signaldb/issues/1849)) ([9e6e258](https://github.com/cedricziel/signaldb/commit/9e6e258fc4ddea8e0bc5af4e801c7cb7920bf358))
* **querier:** convert legacy attribute fixtures to the typed layout ([#1809](https://github.com/cedricziel/signaldb/issues/1809)) ([0a231a3](https://github.com/cedricziel/signaldb/commit/0a231a3bfbaa623da6122fc6d779ff9a28b30b80))
* **querier:** convert remaining legacy metric/log fixtures to the typed layout ([#1806](https://github.com/cedricziel/signaldb/issues/1806)) ([bc2c9ba](https://github.com/cedricziel/signaldb/commit/bc2c9ba14d85ad67e3e94fffe14c01dca3ce13ab))
* **querier:** cover fallback-attribute filters on the wide metrics table ([#1941](https://github.com/cedricziel/signaldb/issues/1941)) ([dc8a86a](https://github.com/cedricziel/signaldb/commit/dc8a86ac8124da5ee026daf89cc3607d14999317))
* **querier:** cover warm-index scan pruning end to end ([#1789](https://github.com/cedricziel/signaldb/issues/1789)) ([f7ba1cc](https://github.com/cedricziel/signaldb/commit/f7ba1ccc0434dbb86aa7ea6c1cf57600f603b040))
* **querier:** discriminating hive regression fixtures ([#2018](https://github.com/cedricziel/signaldb/issues/2018)) ([799940b](https://github.com/cedricziel/signaldb/commit/799940bfd84421a892a73bac32d91e2c4ae9824b))
* **querier:** drop the legacy-layout IR planner tests ([#1812](https://github.com/cedricziel/signaldb/issues/1812)) ([c5676f8](https://github.com/cedricziel/signaldb/commit/c5676f864e4f4fb42ee0604d7bbed06c0990acd9))
* **querier:** histogram validation, delta, reset and bounds-mismatch cases ([#1965](https://github.com/cedricziel/signaldb/issues/1965)) ([71ecbb2](https://github.com/cedricziel/signaldb/commit/71ecbb21a7bc4517ca7ed9bd90edc4420d0d6ea8))
* **querier:** make the promotion invariant exercise promoted reads ([#1870](https://github.com/cedricziel/signaldb/issues/1870)) ([d580309](https://github.com/cedricziel/signaldb/commit/d580309eb2f171a070f201b524cc23560e40a4eb))
* **querier:** pin sample edge cases for metric Series ([#2000](https://github.com/cedricziel/signaldb/issues/2000)) ([6e0a471](https://github.com/cedricziel/signaldb/commit/6e0a47146cf37c19e07117e71694f2fee0284456))
* **querier:** retire the legacy-versus-typed differential suites ([#1801](https://github.com/cedricziel/signaldb/issues/1801)) ([b8a5a4d](https://github.com/cedricziel/signaldb/commit/b8a5a4dda5bb83f1f381d730172f252de5c05eb5))
* **querier:** run metric tests on the wide metrics layout ([#1926](https://github.com/cedricziel/signaldb/issues/1926)) ([e851051](https://github.com/cedricziel/signaldb/commit/e851051e500805a0a2fb81491e05aea44e05b575))
* **querier:** vector matching edge cases and planner composition ([#1981](https://github.com/cedricziel/signaldb/issues/1981)) ([9c06f9c](https://github.com/cedricziel/signaldb/commit/9c06f9ccbaeb8cb3e19894e80916c654df5e1eb8))
* **querier:** vector matching over MemTable Series frames ([#1980](https://github.com/cedricziel/signaldb/issues/1980)) ([127a4e6](https://github.com/cedricziel/signaldb/commit/127a4e6f32610b411304e96451fe9f1982bf1ad0))


### Build System

* **deps:** bump object from 0.37.3 to 0.39.1 ([#2203](https://github.com/cedricziel/signaldb/issues/2203)) ([b6de77a](https://github.com/cedricziel/signaldb/commit/b6de77a4378388901382c517ed72e7a93c309c48))
* fix the beta test leg for cargo's unused-dependency lints ([#2047](https://github.com/cedricziel/signaldb/issues/2047)) ([6867d69](https://github.com/cedricziel/signaldb/commit/6867d69dceb26f2e55ccfac31e60ae42aad76418))
</details>

<details><summary>router: 0.5.0</summary>

## [0.5.0](https://github.com/cedricziel/signaldb/compare/router-v0.4.1...router-v0.5.0) (2026-10-08)


###   BREAKING CHANGES

* **traceql:** `Condition` has a new public `op` field, so code that constructs one must set it.
* `"from": "metrics_histogram"` is rejected as an unknown source. Use `"from": "metrics"`; add a `metric.type = histogram` filter where only histogram rows are wanted. `histogram_quantile` on `metrics` already reads histogram rows only.
* **tests-integration:** metric tables are recreated as metrics/metric_exemplars; pre-cutover metric data is dropped, not migrated.
* metric tables are recreated as metrics/metric_exemplars; pre-cutover metric data is dropped, not migrated.
* existing tables still in the legacy map<string,string> attribute layout are dropped and recreated in the typed layout the next time they are loaded; pre-cutover data in those tables is not migrated.

### Features

* **acceptor:** target the wide metrics table under the wide layout ([#1907](https://github.com/cedricziel/signaldb/issues/1907)) ([e6fb521](https://github.com/cedricziel/signaldb/commit/e6fb521164fa99ba4ebf987de022696576916cf9))
* **common:** merge discovery fields with the type authority's canonical types ([#2074](https://github.com/cedricziel/signaldb/issues/2074)) ([36b7266](https://github.com/cedricziel/signaldb/commit/36b7266ec9442a10aaec8b7eb45d654b6cb27ec7))
* cut attribute storage over to the typed layout ([#1791](https://github.com/cedricziel/signaldb/issues/1791)) ([79b1fff](https://github.com/cedricziel/signaldb/commit/79b1fff7184ee1b3a97dfaf6fbf2d994a6199b39))
* cut metrics over to the typed metrics layout ([#1928](https://github.com/cedricziel/signaldb/issues/1928)) ([e416d6f](https://github.com/cedricziel/signaldb/commit/e416d6f79029d36930b0b98a288517ac18a0b7ff))
* **evals:** build eval sets from traces ([#1847](https://github.com/cedricziel/signaldb/issues/1847)) ([dfe9e60](https://github.com/cedricziel/signaldb/commit/dfe9e608f9629abee1a542f5020f164d193b8faf))
* **evals:** eval sets pages, upload dialog and saving regressions in the UI ([#1871](https://github.com/cedricziel/signaldb/issues/1871)) ([cb05278](https://github.com/cedricziel/signaldb/commit/cb05278065450372e37f51fc149f4ac04a6ad6a7))
* **evals:** upload eval results and gate CI on them ([#1857](https://github.com/cedricziel/signaldb/issues/1857)) ([175155a](https://github.com/cedricziel/signaldb/commit/175155a3a2d958d59e16a05961206c0b09f1e79a))
* **processors:** diff Test panel output against the server's decoded input ([#1883](https://github.com/cedricziel/signaldb/issues/1883)) ([7711bc5](https://github.com/cedricziel/signaldb/commit/7711bc5bca22d010fb83766449d52ccdd44291e0))
* **querier:** add histogram_avg, histogram_stddev and histogram_stdvar ([#2187](https://github.com/cedricziel/signaldb/issues/2187)) ([626a417](https://github.com/cedricziel/signaldb/commit/626a41777f5393eb364a00b9fa211cd674f2eb09))
* **querier:** add the exemplars IR source over metric_exemplars ([#1946](https://github.com/cedricziel/signaldb/issues/1946)) ([5c2c348](https://github.com/cedricziel/signaldb/commit/5c2c3484fc7e4f25219ab05e0813ca0b9c4e3c0c))
* **querier:** bound a Query IR page to a live-tail window ([#2146](https://github.com/cedricziel/signaldb/issues/2146)) ([10421b0](https://github.com/cedricziel/signaldb/commit/10421b0ec2a787b62f515ba61b8e193787e5940b))
* **querier:** read IR metrics from the typed metrics layout ([#1903](https://github.com/cedricziel/signaldb/issues/1903)) ([2454e5c](https://github.com/cedricziel/signaldb/commit/2454e5c5a5bf176ebea7ebb0ebdadec4452c01b3))
* **query-ir:** add document-level page and tail with their validation ([#2133](https://github.com/cedricziel/signaldb/issues/2133)) ([32732f4](https://github.com/cedricziel/signaldb/commit/32732f49d64d32e1e42929ee9836e2e2b4cd3ab3))
* **query-ir:** add metric point streams and the Scalar relation at irVersion 10 ([#1969](https://github.com/cedricziel/signaldb/issues/1969)) ([2b529a2](https://github.com/cedricziel/signaldb/commit/2b529a28a91eacdc0b3cc8ae0d503580adcf05d8))
* **query-ir:** add the document step and the time/constant pseudo-sources ([#1970](https://github.com/cedricziel/signaldb/issues/1970)) ([4da9e65](https://github.com/cedricziel/signaldb/commit/4da9e657c71d2d47d9f221e9021fbc1878614ad2))
* **query-ir:** correlate to another signal with semi and anti joins (irVersion 11) ([#2052](https://github.com/cedricziel/signaldb/issues/2052)) ([ad0cd64](https://github.com/cedricziel/signaldb/commit/ad0cd644101af8f140dd7ae89fdc09f94dab9ca2))
* **query-ir:** differential flamegraph over a baseline window (irVersion 13) ([#2100](https://github.com/cedricziel/signaldb/issues/2100)) ([5e36ddb](https://github.com/cedricziel/signaldb/commit/5e36ddbea6b114c7c2077eca2ee28b9b0fa8089f))
* **query-ir:** trace result envelope (irVersion 12) ([#2063](https://github.com/cedricziel/signaldb/issues/2063)) ([934b6bc](https://github.com/cedricziel/signaldb/commit/934b6bccf5d5fdb12595c07191804091664ae145))
* remove the metrics_histogram IR source ([#1945](https://github.com/cedricziel/signaldb/issues/1945)) ([c16f82c](https://github.com/cedricziel/signaldb/commit/c16f82c100199ef0f297e366208c07d8242501c5))
* **router:** eval sets API for offline agent evals ([#1837](https://github.com/cedricziel/signaldb/issues/1837)) ([b3ce35a](https://github.com/cedricziel/signaldb/commit/b3ce35a7fb9671494e4d78cd29b2673f20205531))
* **router:** list discovery fields with their canonical authority type ([#2075](https://github.com/cedricziel/signaldb/issues/2075)) ([621aa46](https://github.com/cedricziel/signaldb/commit/621aa4674399a628470b73810e033963288a3866))
* **router:** live-tail Query IR rows and trace results (IR v15) ([#2148](https://github.com/cedricziel/signaldb/issues/2148)) ([57ac3c7](https://github.com/cedricziel/signaldb/commit/57ac3c74646037f2198b86f787da3b7a161d6ffd))
* **router:** paginate Query IR rows and trace results (IR v14) ([#2142](https://github.com/cedricziel/signaldb/issues/2142)) ([67023cc](https://github.com/cedricziel/signaldb/commit/67023cc0f71ab345f23d6278f9a917ee3d349941))
* **router:** plan a Query IR live-tail call from its cursor ([#2147](https://github.com/cedricziel/signaldb/issues/2147)) ([52a4eb9](https://github.com/cedricziel/signaldb/commit/52a4eb95c72a27aaa687a9c87822fcc4247a513b))
* **router:** plan a Query IR page from its cursor ([#2141](https://github.com/cedricziel/signaldb/issues/2141)) ([0b4a30b](https://github.com/cedricziel/signaldb/commit/0b4a30bda720145847e27294b582b5ee769ea4ee))
* **router:** publish the Query IR stage grammar as typed OpenAPI schemas ([#2088](https://github.com/cedricziel/signaldb/issues/2088)) ([55e79d8](https://github.com/cedricziel/signaldb/commit/55e79d8fdcb6b5703d1d66801477ae7c714ce6e3))
* **router:** publish UI login/logout and the full whoami response in OpenAPI ([#2110](https://github.com/cedricziel/signaldb/issues/2110)) ([664c8fe](https://github.com/cedricziel/signaldb/commit/664c8fe52a51505ef5fc46a63293e1e35a1a1913))
* **router:** report the oldest data a query's source holds ([#2191](https://github.com/cedricziel/signaldb/issues/2191)) ([7911eb5](https://github.com/cedricziel/signaldb/commit/7911eb512868a40075187e47fdce0303cf1507a0))
* **router:** report the retention that applies to a query ([#2182](https://github.com/cedricziel/signaldb/issues/2182)) ([91ca14d](https://github.com/cedricziel/signaldb/commit/91ca14dfc6f4478155ef9095c1b84e4078a6da90))
* **router:** run the Prometheus query endpoints on the Query IR ([#2040](https://github.com/cedricziel/signaldb/issues/2040)) ([aa66d18](https://github.com/cedricziel/signaldb/commit/aa66d1825b7165312114ec4e111e512e684f5e73))
* **router:** scalar result envelope and metric Series labels ([#2001](https://github.com/cedricziel/signaldb/issues/2001)) ([2d72dc1](https://github.com/cedricziel/signaldb/commit/2d72dc1b719ee880ad87101fc042d21465f3affc))
* **router:** type the Query IR request pipeline as IrStage ([#2095](https://github.com/cedricziel/signaldb/issues/2095)) ([414ed92](https://github.com/cedricziel/signaldb/commit/414ed92b136367e2650b1c8de8f0cd12250cce5b))
* **router:** warn match_incomplete_trace from the query report trailer ([#2090](https://github.com/cedricziel/signaldb/issues/2090)) ([7ee6f46](https://github.com/cedricziel/signaldb/commit/7ee6f464d3512df3a2c4fddc06f90302c3c55b16))
* **schema-registry:** accept definition/2 custom registry uploads ([#1823](https://github.com/cedricziel/signaldb/issues/1823)) ([a8c8196](https://github.com/cedricziel/signaldb/commit/a8c819648bd5778478be6e86241802ae4f6f880f))
* **traceql:** support !=, =~ and !~ and surface search errors over MCP ([#2178](https://github.com/cedricziel/signaldb/issues/2178)) ([ce929f5](https://github.com/cedricziel/signaldb/commit/ce929f5f321e5d826e2ba43edc67d7e26de67659))


### Bug Fixes

* **config:** merge a tenant's schema block over the global [schema] ([#2086](https://github.com/cedricziel/signaldb/issues/2086)) ([0832345](https://github.com/cedricziel/signaldb/commit/0832345521808cf865d627580dc1a0621692adcc))
* **discovery:** keep statistics coverage honest about what it bounds ([#2184](https://github.com/cedricziel/signaldb/issues/2184)) ([211cce5](https://github.com/cedricziel/signaldb/commit/211cce55e7d11d533d929f728b903004f605b03d))
* **querier:** address metric Series review findings ([#2003](https://github.com/cedricziel/signaldb/issues/2003)) ([e8a13d1](https://github.com/cedricziel/signaldb/commit/e8a13d12226e7a63162a6e2539cd4b7ee7b9785d))
* **querier:** keep the correlate trailer compatible across adjacent releases ([#2051](https://github.com/cedricziel/signaldb/issues/2051)) ([706f25e](https://github.com/cedricziel/signaldb/commit/706f25ed1c9d01b5d08ea6b368ff31cd5fc199f7))
* **query-ir:** keep the newest flamegraph profiles and reject inverted windows ([#2098](https://github.com/cedricziel/signaldb/issues/2098)) ([dda9bad](https://github.com/cedricziel/signaldb/commit/dda9bad2c86140ee9be861ee0e4cd5f2788cf8a9))
* **router:** harden the Prometheus query endpoints on the IR ([#2041](https://github.com/cedricziel/signaldb/issues/2041)) ([22a3369](https://github.com/cedricziel/signaldb/commit/22a33699346ac63517f0ef5b7f18a4980b9dd444))
* **router:** read data for sample:true and flag partial discovery statistics ([#2176](https://github.com/cedricziel/signaldb/issues/2176)) ([ab5dcf7](https://github.com/cedricziel/signaldb/commit/ab5dcf78f47567a6e2a8f8a10dcf5dfd06cdfed5))


### Performance Improvements

* **acceptor:** send WAL IPC bytes to the writer without re-encoding ([#2192](https://github.com/cedricziel/signaldb/issues/2192)) ([be7e5ee](https://github.com/cedricziel/signaldb/commit/be7e5ee93f9f539ed86adf476e03fe044ee00768)), closes [#942](https://github.com/cedricziel/signaldb/issues/942)
* **common:** share registry documents from SchemaResolver::get ([#1819](https://github.com/cedricziel/signaldb/issues/1819)) ([2fe61c1](https://github.com/cedricziel/signaldb/commit/2fe61c16af8956b49c4d223795c0549dc94727e7))
* **router:** decode querier results as they arrive ([#2165](https://github.com/cedricziel/signaldb/issues/2165)) ([60f55b7](https://github.com/cedricziel/signaldb/commit/60f55b7c05267a4f869b0d24c3ded8630fc92a0a)), closes [#938](https://github.com/cedricziel/signaldb/issues/938)


### Documentation

* describe PromQL execution through the Query IR ([#2042](https://github.com/cedricziel/signaldb/issues/2042)) ([3375b34](https://github.com/cedricziel/signaldb/commit/3375b344ff68b1c6f1b546bf36f7de00dd508357))


### Code Refactoring

* **common:** let ServiceBootstrap resolve the advertised address ([#2121](https://github.com/cedricziel/signaldb/issues/2121)) ([6bef83b](https://github.com/cedricziel/signaldb/commit/6bef83b68b873ea7407d0f92981665a2b4cb420e))
* drop the dead MetricsLayout switch and *_with_layout helpers ([#1961](https://github.com/cedricziel/signaldb/issues/1961)) ([4a98be3](https://github.com/cedricziel/signaldb/commit/4a98be3e108b2c67be847db23de0f768c1ad2a58))
* **querier:** define the metrics IR sources over the metrics table ([#1940](https://github.com/cedricziel/signaldb/issues/1940)) ([4a26403](https://github.com/cedricziel/signaldb/commit/4a26403a3e606dfdd69ad55842e359b08f4bea31))
* **querier:** report correlate bounds as a structured Flight trailer ([#2050](https://github.com/cedricziel/signaldb/issues/2050)) ([d538baf](https://github.com/cedricziel/signaldb/commit/d538baf416f8710b922b3c275a65873edd4cdd86))
* share typed attribute container helpers ([#1815](https://github.com/cedricziel/signaldb/issues/1815)) ([45be827](https://github.com/cedricziel/signaldb/commit/45be827a12365bca5b39443db66ae3f16764fbc8))


### Tests

* **compactor,router:** convert legacy attribute fixtures to the typed layout ([#1807](https://github.com/cedricziel/signaldb/issues/1807)) ([c9f06d1](https://github.com/cedricziel/signaldb/commit/c9f06d16ac6e6b1c0c610a4df40a8e67d02dea8b))
* **tests-integration:** add an end-to-end metrics cutover test ([#1929](https://github.com/cedricziel/signaldb/issues/1929)) ([bb47677](https://github.com/cedricziel/signaldb/commit/bb4767732f40a82ca1af1e4eab88a8dc11580829))


### Build System

* **deps:** bump object from 0.37.3 to 0.39.1 ([#2203](https://github.com/cedricziel/signaldb/issues/2203)) ([b6de77a](https://github.com/cedricziel/signaldb/commit/b6de77a4378388901382c517ed72e7a93c309c48))
* fix the beta test leg for cargo's unused-dependency lints ([#2047](https://github.com/cedricziel/signaldb/issues/2047)) ([6867d69](https://github.com/cedricziel/signaldb/commit/6867d69dceb26f2e55ccfac31e60ae42aad76418))
</details>

<details><summary>signaldb-bin: 0.5.0</summary>

## [0.5.0](https://github.com/cedricziel/signaldb/compare/signaldb-bin-v0.4.1...signaldb-bin-v0.5.0) (2026-10-08)


###   BREAKING CHANGES

* **querier:** a standalone querier with no `memory_limit_mb` is now bounded (set 0 to opt out), and a tenant running more than 8 concurrent queries gets RESOURCE_EXHAUSTED unless `max_concurrent_queries_per_tenant` is raised (0 = unlimited).

### Features

* **acceptor:** cap attributes per record at ingest ([#2185](https://github.com/cedricziel/signaldb/issues/2185)) ([56cc1f0](https://github.com/cedricziel/signaldb/commit/56cc1f0cad0aef215eb54639f187f55cd8055af5)), closes [#821](https://github.com/cedricziel/signaldb/issues/821)
* **acceptor:** warn OTLP senders about off-type attribute values ([#1834](https://github.com/cedricziel/signaldb/issues/1834)) ([1538139](https://github.com/cedricziel/signaldb/commit/1538139f06b5eac84158048f54283040b236185a))
* **querier:** bound memory and per-tenant concurrency by default ([#2161](https://github.com/cedricziel/signaldb/issues/2161)) ([93dc0d9](https://github.com/cedricziel/signaldb/commit/93dc0d9e60c84ddfc6fb876b09e40be4e7b305cc))
* **router:** paginate Query IR rows and trace results (IR v14) ([#2142](https://github.com/cedricziel/signaldb/issues/2142)) ([67023cc](https://github.com/cedricziel/signaldb/commit/67023cc0f71ab345f23d6278f9a917ee3d349941))
* **writer:** place typed attributes in every writer deployment ([#1790](https://github.com/cedricziel/signaldb/issues/1790)) ([49551e8](https://github.com/cedricziel/signaldb/commit/49551e8fd811514c222b1643ecbc2cca3c83495f))


### Bug Fixes

* **acceptor:** acknowledge an exporter's resend of an already-durable batch ([#1814](https://github.com/cedricziel/signaldb/issues/1814)) ([ff52ded](https://github.com/cedricziel/signaldb/commit/ff52deda64b5b803c4040cee0db249a6beb5d7f4))
* **signaldb-bin:** honour the *_ADVERTISE_ADDR overrides in the monolith ([#2109](https://github.com/cedricziel/signaldb/issues/2109)) ([de5de92](https://github.com/cedricziel/signaldb/commit/de5de922ee7ae88d5ee82f9fded3dac0caf38976)), closes [#1843](https://github.com/cedricziel/signaldb/issues/1843)
* **telemetry:** namespace bare log fields flagged by weaver live-check ([#1879](https://github.com/cedricziel/signaldb/issues/1879)) ([90dbf09](https://github.com/cedricziel/signaldb/commit/90dbf09c31f7181f4f97c4a0bdb6c79f0f78aca3)), closes [#912](https://github.com/cedricziel/signaldb/issues/912)


### Code Refactoring

* **common:** let ServiceBootstrap resolve the advertised address ([#2121](https://github.com/cedricziel/signaldb/issues/2121)) ([6bef83b](https://github.com/cedricziel/signaldb/commit/6bef83b68b873ea7407d0f92981665a2b4cb420e))


### Build System

* **deps:** bump object from 0.37.3 to 0.39.1 ([#2203](https://github.com/cedricziel/signaldb/issues/2203)) ([b6de77a](https://github.com/cedricziel/signaldb/commit/b6de77a4378388901382c517ed72e7a93c309c48))
* fix the beta test leg for cargo's unused-dependency lints ([#2047](https://github.com/cedricziel/signaldb/issues/2047)) ([6867d69](https://github.com/cedricziel/signaldb/commit/6867d69dceb26f2e55ccfac31e60ae42aad76418))
</details>

<details><summary>signaldb-cli: 0.5.0</summary>

## [0.5.0](https://github.com/cedricziel/signaldb/compare/signaldb-cli-v0.4.1...signaldb-cli-v0.5.0) (2026-10-08)


###   BREAKING CHANGES

* **mcp-server:** discover_attributes and discover_metrics return the Query IR metadata envelope (logical dotted names, levels, cost) instead of a Tempo tag list, a Loki/Prometheus {status,data} array, or Pyroscope {names}. scope narrows by attribute level rather than routing to the Tempo v2 endpoints, and tag values need a declared set, a statistics sketch, or sample: true.
* **signaldb-cli:** discover attributes and discover metrics print the Query IR metadata envelope (logical dotted names, levels, cost) instead of a Tempo tag list, a Loki/Prometheus {status,data} array, or the PromQL __name__ values. --scope narrows by attribute level rather than routing to the Tempo v2 endpoints, and tag values need a declared set, a statistics sketch, or --sample.
* **tests-integration:** metric tables are recreated as metrics/metric_exemplars; pre-cutover metric data is dropped, not migrated.
* metric tables are recreated as metrics/metric_exemplars; pre-cutover metric data is dropped, not migrated.

### Features

* **cli,mcp:** follow a Query IR live tail through the IR endpoint ([#2150](https://github.com/cedricziel/signaldb/issues/2150)) ([1219d9d](https://github.com/cedricziel/signaldb/commit/1219d9d971107f485ffb7a4737bda901eceaedc0))
* **cli,mcp:** page Query IR results through the IR endpoint ([#2144](https://github.com/cedricziel/signaldb/issues/2144)) ([9fc147d](https://github.com/cedricziel/signaldb/commit/9fc147d416a5329a31040af566424f40db3e5f6c))
* cut metrics over to the typed metrics layout ([#1928](https://github.com/cedricziel/signaldb/issues/1928)) ([e416d6f](https://github.com/cedricziel/signaldb/commit/e416d6f79029d36930b0b98a288517ac18a0b7ff))
* **evals:** build eval sets from traces ([#1847](https://github.com/cedricziel/signaldb/issues/1847)) ([dfe9e60](https://github.com/cedricziel/signaldb/commit/dfe9e608f9629abee1a542f5020f164d193b8faf))
* **evals:** eval sets pages, upload dialog and saving regressions in the UI ([#1871](https://github.com/cedricziel/signaldb/issues/1871)) ([cb05278](https://github.com/cedricziel/signaldb/commit/cb05278065450372e37f51fc149f4ac04a6ad6a7))
* **evals:** list and compare eval runs from MCP and the CLI ([#1866](https://github.com/cedricziel/signaldb/issues/1866)) ([665f458](https://github.com/cedricziel/signaldb/commit/665f4588808aad9ae567df358c7732f70881aec9))
* **evals:** upload eval results and gate CI on them ([#1857](https://github.com/cedricziel/signaldb/issues/1857)) ([175155a](https://github.com/cedricziel/signaldb/commit/175155a3a2d958d59e16a05961206c0b09f1e79a))
* **mcp-server:** discover attributes and metrics through the Query IR ([#2077](https://github.com/cedricziel/signaldb/issues/2077)) ([37d58ea](https://github.com/cedricziel/signaldb/commit/37d58eac6e5ac2b3766b324e494f0c4901812a5a))
* **processors:** diff Test panel output against the server's decoded input ([#1883](https://github.com/cedricziel/signaldb/issues/1883)) ([7711bc5](https://github.com/cedricziel/signaldb/commit/7711bc5bca22d010fb83766449d52ccdd44291e0))
* **query-ir:** differential flamegraph over a baseline window (irVersion 13) ([#2100](https://github.com/cedricziel/signaldb/issues/2100)) ([5e36ddb](https://github.com/cedricziel/signaldb/commit/5e36ddbea6b114c7c2077eca2ee28b9b0fa8089f))
* **query-ir:** trace result envelope (irVersion 12) ([#2063](https://github.com/cedricziel/signaldb/issues/2063)) ([934b6bc](https://github.com/cedricziel/signaldb/commit/934b6bccf5d5fdb12595c07191804091664ae145))
* recognise AI agents as the gen_ai.agent entity ([#1803](https://github.com/cedricziel/signaldb/issues/1803)) ([e0fcaa0](https://github.com/cedricziel/signaldb/commit/e0fcaa0b3cd8133e23e0ccc8f9e28e43986c41f5))
* **router:** eval sets API for offline agent evals ([#1837](https://github.com/cedricziel/signaldb/issues/1837)) ([b3ce35a](https://github.com/cedricziel/signaldb/commit/b3ce35a7fb9671494e4d78cd29b2673f20205531))
* **router:** live-tail Query IR rows and trace results (IR v15) ([#2148](https://github.com/cedricziel/signaldb/issues/2148)) ([57ac3c7](https://github.com/cedricziel/signaldb/commit/57ac3c74646037f2198b86f787da3b7a161d6ffd))
* **router:** paginate Query IR rows and trace results (IR v14) ([#2142](https://github.com/cedricziel/signaldb/issues/2142)) ([67023cc](https://github.com/cedricziel/signaldb/commit/67023cc0f71ab345f23d6278f9a917ee3d349941))
* **router:** report the oldest data a query's source holds ([#2191](https://github.com/cedricziel/signaldb/issues/2191)) ([7911eb5](https://github.com/cedricziel/signaldb/commit/7911eb512868a40075187e47fdce0303cf1507a0))
* **router:** report the retention that applies to a query ([#2182](https://github.com/cedricziel/signaldb/issues/2182)) ([91ca14d](https://github.com/cedricziel/signaldb/commit/91ca14dfc6f4478155ef9095c1b84e4078a6da90))
* **router:** scalar result envelope and metric Series labels ([#2001](https://github.com/cedricziel/signaldb/issues/2001)) ([2d72dc1](https://github.com/cedricziel/signaldb/commit/2d72dc1b719ee880ad87101fc042d21465f3affc))
* **schema-registry:** accept definition/2 custom registry uploads ([#1823](https://github.com/cedricziel/signaldb/issues/1823)) ([a8c8196](https://github.com/cedricziel/signaldb/commit/a8c819648bd5778478be6e86241802ae4f6f880f))
* **signaldb-cli:** discover attributes and metrics through the Query IR ([#2076](https://github.com/cedricziel/signaldb/issues/2076)) ([f398305](https://github.com/cedricziel/signaldb/commit/f398305ed42d5833150e2e19c5f0c7bb90d411d5))


### Bug Fixes

* **router:** read data for sample:true and flag partial discovery statistics ([#2176](https://github.com/cedricziel/signaldb/issues/2176)) ([ab5dcf7](https://github.com/cedricziel/signaldb/commit/ab5dcf78f47567a6e2a8f8a10dcf5dfd06cdfed5))


### Tests

* **tests-integration:** add an end-to-end metrics cutover test ([#1929](https://github.com/cedricziel/signaldb/issues/1929)) ([bb47677](https://github.com/cedricziel/signaldb/commit/bb4767732f40a82ca1af1e4eab88a8dc11580829))


### Build System

* **deps:** bump object from 0.37.3 to 0.39.1 ([#2203](https://github.com/cedricziel/signaldb/issues/2203)) ([b6de77a](https://github.com/cedricziel/signaldb/commit/b6de77a4378388901382c517ed72e7a93c309c48))
* fix the beta test leg for cargo's unused-dependency lints ([#2047](https://github.com/cedricziel/signaldb/issues/2047)) ([6867d69](https://github.com/cedricziel/signaldb/commit/6867d69dceb26f2e55ccfac31e60ae42aad76418))
</details>

<details><summary>writer: 0.5.0</summary>

## [0.5.0](https://github.com/cedricziel/signaldb/compare/writer-v0.4.1...writer-v0.5.0) (2026-10-08)


###   BREAKING CHANGES

* **tests-integration:** metric tables are recreated as metrics/metric_exemplars; pre-cutover metric data is dropped, not migrated.
* metric tables are recreated as metrics/metric_exemplars; pre-cutover metric data is dropped, not migrated.
* remove the legacy attribute map write path ([#1793](https://github.com/cedricziel/signaldb/issues/1793))
* drop the legacy attr_tokens column ([#1794](https://github.com/cedricziel/signaldb/issues/1794))
* existing tables still in the legacy map<string,string> attribute layout are dropped and recreated in the typed layout the next time they are loaded; pre-cutover data in those tables is not migrated.

### Features

* **acceptor:** target the wide metrics table under the wide layout ([#1907](https://github.com/cedricziel/signaldb/issues/1907)) ([e6fb521](https://github.com/cedricziel/signaldb/commit/e6fb521164fa99ba4ebf987de022696576916cf9))
* **common:** declare the typed metrics and metric_exemplars schemas ([#1886](https://github.com/cedricziel/signaldb/issues/1886)) ([9095c9f](https://github.com/cedricziel/signaldb/commit/9095c9f252f6bfead8ea76d7ee8f2c996caa69bd))
* cut attribute storage over to the typed layout ([#1791](https://github.com/cedricziel/signaldb/issues/1791)) ([79b1fff](https://github.com/cedricziel/signaldb/commit/79b1fff7184ee1b3a97dfaf6fbf2d994a6199b39))
* cut metrics over to the typed metrics layout ([#1928](https://github.com/cedricziel/signaldb/issues/1928)) ([e416d6f](https://github.com/cedricziel/signaldb/commit/e416d6f79029d36930b0b98a288517ac18a0b7ff))
* **writer:** commit metric exemplars alongside their metrics batch ([#1895](https://github.com/cedricziel/signaldb/issues/1895)) ([5536644](https://github.com/cedricziel/signaldb/commit/55366442e01832a8f331f37797bc54aab543234f))
* **writer:** drop legacy metric tables from the reconciler under the wide layout ([#1908](https://github.com/cedricziel/signaldb/issues/1908)) ([ed8a311](https://github.com/cedricziel/signaldb/commit/ed8a311d9d124e7a754f06c9503c61e75ed61896))
* **writer:** place typed attributes in every writer deployment ([#1790](https://github.com/cedricziel/signaldb/issues/1790)) ([49551e8](https://github.com/cedricziel/signaldb/commit/49551e8fd811514c222b1643ecbc2cca3c83495f))
* **writer:** surface off-type attribute values and type-pin conflicts ([#1829](https://github.com/cedricziel/signaldb/issues/1829)) ([b4bbfb4](https://github.com/cedricziel/signaldb/commit/b4bbfb464e9cb3e96c5afe82cc543b9f5beaf401))
* **writer:** transform wire exemplars into metric_exemplars rows ([#1894](https://github.com/cedricziel/signaldb/issues/1894)) ([27ac405](https://github.com/cedricziel/signaldb/commit/27ac4054d78fa3464ac953c4682ec5030d421ed2))
* **writer:** transform wire metrics into the typed metrics layout ([#1893](https://github.com/cedricziel/signaldb/issues/1893)) ([ef96961](https://github.com/cedricziel/signaldb/commit/ef969619aa00fb944cdafcfbedbc6a47a73c28fd))
* **writer:** write warm-index tokens for opted-in typed tables ([#1783](https://github.com/cedricziel/signaldb/issues/1783)) ([c2c1dba](https://github.com/cedricziel/signaldb/commit/c2c1dbab6de9d9175bd4f06615d556f67ed4fe82))


### Bug Fixes

* **acceptor:** acknowledge an exporter's resend of an already-durable batch ([#1814](https://github.com/cedricziel/signaldb/issues/1814)) ([ff52ded](https://github.com/cedricziel/signaldb/commit/ff52deda64b5b803c4040cee0db249a6beb5d7f4))
* **acceptor:** dedup client resends across acceptor replicas and restarts ([#1821](https://github.com/cedricziel/signaldb/issues/1821)) ([98cf40e](https://github.com/cedricziel/signaldb/commit/98cf40e5e6bc7d97f3b43564591df2039b9c8070))
* **compactor:** advertise COMPACTOR_ADVERTISE_ADDR instead of the bind address ([#2107](https://github.com/cedricziel/signaldb/issues/2107)) ([2b8adb7](https://github.com/cedricziel/signaldb/commit/2b8adb7f23df02ee44a7d7f72dde53bcdd436254)), closes [#1844](https://github.com/cedricziel/signaldb/issues/1844)
* **config:** merge a tenant's schema block over the global [schema] ([#2086](https://github.com/cedricziel/signaldb/issues/2086)) ([0832345](https://github.com/cedricziel/signaldb/commit/0832345521808cf865d627580dc1a0621692adcc))
* **querier:** resolve label columns by their origin-key doc ([#2183](https://github.com/cedricziel/signaldb/issues/2183)) ([73768e9](https://github.com/cedricziel/signaldb/commit/73768e96e645b50157c04864dac913637f2d1068))
* **telemetry:** namespace bare log fields flagged by weaver live-check ([#1879](https://github.com/cedricziel/signaldb/issues/1879)) ([90dbf09](https://github.com/cedricziel/signaldb/commit/90dbf09c31f7181f4f97c4a0bdb6c79f0f78aca3)), closes [#912](https://github.com/cedricziel/signaldb/issues/912)
* **writer:** keep skipped exemplar ids in the metric_exemplars marker ([#1948](https://github.com/cedricziel/signaldb/issues/1948)) ([a36f3e6](https://github.com/cedricziel/signaldb/commit/a36f3e697552a72386c081668db50706f556c30c))
* **writer:** retry the legacy metric table purge on converged datasets ([#1949](https://github.com/cedricziel/signaldb/issues/1949)) ([1ec8d3f](https://github.com/cedricziel/signaldb/commit/1ec8d3f9a87f165e2cf7e69bfbe2b4bcd145a1a5))


### Performance Improvements

* **acceptor:** send WAL IPC bytes to the writer without re-encoding ([#2192](https://github.com/cedricziel/signaldb/issues/2192)) ([be7e5ee](https://github.com/cedricziel/signaldb/commit/be7e5ee93f9f539ed86adf476e03fe044ee00768)), closes [#942](https://github.com/cedricziel/signaldb/issues/942)


### Code Refactoring

* **common:** let ServiceBootstrap resolve the advertised address ([#2121](https://github.com/cedricziel/signaldb/issues/2121)) ([6bef83b](https://github.com/cedricziel/signaldb/commit/6bef83b68b873ea7407d0f92981665a2b4cb420e))
* drop the dead MetricsLayout switch and *_with_layout helpers ([#1961](https://github.com/cedricziel/signaldb/issues/1961)) ([4a98be3](https://github.com/cedricziel/signaldb/commit/4a98be3e108b2c67be847db23de0f768c1ad2a58))
* drop the legacy attr_tokens column ([#1794](https://github.com/cedricziel/signaldb/issues/1794)) ([a4b84a7](https://github.com/cedricziel/signaldb/commit/a4b84a7b7128e822524b5379036ded18873f4fa9))
* remove the legacy attribute map write path ([#1793](https://github.com/cedricziel/signaldb/issues/1793)) ([9962ba9](https://github.com/cedricziel/signaldb/commit/9962ba962ca3da95107442dd0d7140cb0b2fd98e))
* **writer:** delete the gauge and sum metric transforms ([#1935](https://github.com/cedricziel/signaldb/issues/1935)) ([e920fc1](https://github.com/cedricziel/signaldb/commit/e920fc154792bddce29659c0ceb341590cbbf408))
* **writer:** delete the gauge schema builder and move its fixtures onto the wire format ([#1934](https://github.com/cedricziel/signaldb/issues/1934)) ([bc74e50](https://github.com/cedricziel/signaldb/commit/bc74e50459199b3a2a38456efe845ec417e97bc8))
* **writer:** delete the histogram, exponential-histogram, and summary metric transforms ([#1932](https://github.com/cedricziel/signaldb/issues/1932)) ([f7276e2](https://github.com/cedricziel/signaldb/commit/f7276e2832de355cc77912d2f683fe6e7252f4c2))
* **writer:** delete the sum, histogram, exponential-histogram, and summary schema builders ([#1933](https://github.com/cedricziel/signaldb/issues/1933)) ([cd762cf](https://github.com/cedricziel/signaldb/commit/cd762cf8e93b068ee4dbbc97203523fb7e79bcec))


### Tests

* derive per-tenant table counts and switch metrics fixtures to wire-format batches ([#1927](https://github.com/cedricziel/signaldb/issues/1927)) ([dbeddc5](https://github.com/cedricziel/signaldb/commit/dbeddc5b78665446fbc7dc2c151816a3bd21feb6))
* move metric fixtures off the legacy per-type tables ([#1966](https://github.com/cedricziel/signaldb/issues/1966)) ([4f239c7](https://github.com/cedricziel/signaldb/commit/4f239c7da68a4a7db76667044a724b19fe691127))
* **tests-integration:** add an end-to-end metrics cutover test ([#1929](https://github.com/cedricziel/signaldb/issues/1929)) ([bb47677](https://github.com/cedricziel/signaldb/commit/bb4767732f40a82ca1af1e4eab88a8dc11580829))


### Build System

* **deps:** bump object from 0.37.3 to 0.39.1 ([#2203](https://github.com/cedricziel/signaldb/issues/2203)) ([b6de77a](https://github.com/cedricziel/signaldb/commit/b6de77a4378388901382c517ed72e7a93c309c48))
* fix the beta test leg for cargo's unused-dependency lints ([#2047](https://github.com/cedricziel/signaldb/issues/2047)) ([6867d69](https://github.com/cedricziel/signaldb/commit/6867d69dceb26f2e55ccfac31e60ae42aad76418))


### Continuous Integration

* run every Criterion bench once on core PRs ([#2167](https://github.com/cedricziel/signaldb/issues/2167)) ([918f9c5](https://github.com/cedricziel/signaldb/commit/918f9c558ab655cc4b71071ce981d9ba5ed6ec8a))
</details>

---
This PR was generated with [Release Please](https://github.com/googleapis/release-please). See [documentation](https://github.com/googleapis/release-please#release-please).