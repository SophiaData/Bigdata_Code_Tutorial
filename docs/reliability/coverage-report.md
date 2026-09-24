# 娴嬭瘯瑕嗙洊鐜囨姤鍛?

> 鐢熸垚鏃堕棿锛?026-06-29锛?*2026-09-24 閲嶆柊娴嬮噺**锛?
> 宸ュ叿锛欽aCoCo Maven Plugin 0.8.12
> 闂ㄦ锛歚jacoco.line.min=0.15` / `jacoco.branch.min=0.25`锛堟杞紡涓嬮檺锛岄槻姝㈠€掗€€锛?

---

## 0. 閲嶈鏇存锛氭鍓嶇殑瑕嗙洊鐜囨暟瀛楁槸鏃犳晥鐨?

鏈姤鍛婂師鍏堣褰曠殑 `cdc-mysql-sync 10.1%`銆乣flink-demo 100%`銆乣cdc-paimon-sync 0%` **骞堕潪鐪熷疄娴嬮噺缁撴灉**銆?

鍘熷洜锛歋urefire 鐨?`<argLine>` 閲岀‖缂栫爜浜?`--add-opens`锛岃繖浼?*鏁翠綋瑕嗙洊** JaCoCo 閫氳繃 `argLine` 灞炴€ф敞鍏ョ殑 `-javaagent`锛屽鑷磋鐩栫巼 agent 浠庢湭鎸傝浇銆傛瀯寤烘棩蹇椾腑鐨勮〃鐜版槸锛?

```
[INFO] Skipping JaCoCo execution due to missing execution data file.
```

鍗筹細**娴嬭瘯姝ｅ父璺戙€佸叏閮ㄩ€氳繃锛屼絾瑕嗙洊鐜囨暟鎹竴鐩存槸绌虹殑**锛宍check` 鐩爣涔熷洜涓烘棤鏁版嵁鑰岄潤榛樿烦杩囷紙闂ㄦ褰㈠悓铏氳锛夈€?

淇锛歚<argLine>@{argLine} --add-opens=...</argLine>` 鈥斺€?鐢ㄥ睘鎬ф彃鍊兼妸 JaCoCo 鐨?agent 鎷煎洖鏉ャ€備慨澶嶅悗 `target/jacoco.exec` 姝ｅ父鐢熸垚锛堢害 39KB锛夈€?

CI 渚ц繕鏈変竴涓嫭绔嬬己闄凤細鏀堕泦瑕嗙洊鐜囩殑姝ラ鏄?
`./mvnw ... verify -Djacoco.skip=false -DskipTests`
鈥斺€?甯︿簡 `-DskipTests`锛屽悓鏍锋案杩滄嬁涓嶅埌鏁版嵁銆傚凡鏀逛负鍦ㄨ窇鍗曟祴鏃朵竴骞舵敹闆嗭紝鍐嶇敤 `jacoco:report` 鐢熸垚鎶ュ憡銆?

---

## 涓€銆佸悇妯″潡瑕嗙洊鐜囷紙2026-09-24 瀹炴祴锛?

| 妯″潡 | LINE 瑕嗙洊 | BRANCH 瑕嗙洊 | 璇存槑 |
|---|---|---|---|
| `flink-compat` | **79.5%** (35/44) | 鈥?| 灏忔ā鍧楋紝鍏煎灞傛祴璇曞厖鍒?|
| `cdc-mysql-sync` | **32.2%** (331/1028) | 28.3% (115/406) | 宸ュ叿绫昏鐩栬緝濂斤紝涓绘祦绋嬩粛鍋忚杽 |
| `flink-demo` | **27.7%** (486/1752) | 28.5% (111/390) | 绀轰緥绫诲锛屾祴璇曢泦涓湪灏戞暟妯″潡 |
| `cdc-paimon-sync` | **15.5%** (34/220) | 36.0% (27/75) | 鏈€浣庯紝浣嗗垎鏀鐩栧弽鑰屼笉宸?|

---

## 浜屻€侀棬妲涚瓥鐣ワ細妫樿疆鑰岄潪鐩爣

闂ㄦ鍊艰鍦?*褰撳墠瀹炴祴鍊肩暐涓嬫柟**锛屼綔鐢ㄦ槸**鎷︿綇鍊掗€€**锛堜緥濡傛柊绫昏惤鍦板嵈娌￠厤娴嬭瘯锛夛紝鑰屼笉鏄瀹氭彁鍗囩洰鏍囷細

```xml
<jacoco.line.min>0.15</jacoco.line.min>
<jacoco.branch.min>0.25</jacoco.branch.min>
```

**涓轰粈涔堢敤缁熶竴鍊艰€屼笉鏄瘡妯″潡涓€涓?*锛欽aCoCo 鐨?`BUNDLE` 瑙勫垯鎸夋ā鍧楃敓鏁堬紝鑰屽悇妯″潡瑕嗙洊鐜囩浉宸?5 鍊嶏紙15.5% ~ 79.5%锛夈€傜粺涓€鍙栦笅闄愶紙15%锛夊彲淇濊瘉褰撳墠鎵€鏈夋ā鍧楅兘鑳介€氳繃锛屼箣鍚庡彧鑳戒笂璋冦€傛煇涓ā鍧楄鐩栫巼鏄捐憲鎻愬崌鍚庯紝鍙湪鑷繁鐨?POM 閲岃鐩?`jacoco.line.min`銆?

宸插疄娴嬮獙璇侀棬妲?*纭疄鐢熸晥**锛氭妸闂ㄦ涓存椂璁句负 0.90 鍚庢瀯寤哄け璐ュ苟缁欏嚭

```
Rule violated for bundle flink-compat: lines covered ratio is 0.79, but expected minimum is 0.90
```

---

## 涓夈€佸浣曞鐜?

```bash
# 1) 璺戝崟娴嬪苟鏀堕泦瑕嗙洊鐜囷紙娉ㄦ剰涓嶈兘甯?-DskipTests锛?
./mvnw -pl cdc-mysql-sync -am test -Djacoco.skip=false \
  -Dtest='!*IT,!*E2E,!*IntegrationTest,!*FlinkSqlWDSTest'

# 2) 鐢熸垚 HTML/XML 鎶ュ憡
./mvnw -pl cdc-mysql-sync -am jacoco:report -Djacoco.skip=false

# 3) 鎶ュ憡浣嶇疆
# cdc-mysql-sync/target/site/jacoco/index.html
```

CI 鐨?`lint-and-unit` job 鐜板凡鍖呭惈杩欎笁姝ワ紝骞舵妸鎶ュ憡浣滀负 `jacoco-<module>` 鍒跺搧涓婁紶锛堜繚鐣?7 澶╋級銆?

---

## 鍥涖€佸悗缁柟鍚戯紙鎸夋€т环姣旀帓搴忥級

1. `cdc-paimon-sync` 鐨?LINE 瑕嗙洊鏈€浣庯紙15.5%锛夛紝涓斿垎鏀鐩栵紙36%锛夋槑鏄鹃珮浜庤瑕嗙洊 鈥斺€?璇存槑娴嬪埌鐨勫垎鏀泦涓湪灏戦噺鏂规硶閲岋紝涓绘祦绋嬪熀鏈湭瑕嗙洊
2. `cdc-mysql-sync` 鐨?`SchemaEvolver`銆乣FlinkSqlWDS` 鏄悓姝ラ摼璺殑鏍稿績锛屼粛鏄富瑕佺己鍙?
3. 闂ㄦ鍙殢瑕嗙洊鐜囨彁鍗囬€愭涓婅皟锛?5% 鈫?25% 鈫?35%

> 娉細JaCoCo 鍦?*娌℃湁鎵ц鏁版嵁鏃朵細璺宠繃妫€鏌?*锛屽洜姝ら棬妲涘彧鍦?`-Djacoco.skip=false` 鐪熸璺戣繃娴嬭瘯鍚庢墠鏈夌害鏉熷姏銆?

---

## 五、已知限制

1. **JaCoCo 无数据时跳过检查**：若 `-Djacoco.skip=false` 未真正执行测试，`check` 与 `report` 都会输出 "Skipping JaCoCo execution due to missing execution data file" 并以成功结束 —— 门槛在这种情况下不具约束力
2. **不区分 main/test**：JaCoCo 默认同时统计 main 与 test 代码；上表数字已按模块 bundle 汇总
3. **集成测试未计入**：上表只统计单测（`!*IT,!*E2E`）覆盖；`*IT` 需 Docker，在 CI 的独立 job 中运行，未合并进覆盖率数据
4. **未上传到外部服务**：报告以 CI 制品形式（`jacoco-<module>`）保存 7 天，尚未接入 Codecov/SonarQube
