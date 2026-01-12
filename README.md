# Geo Matcher 流水线

面向“高德 POI + 40 万条快递原始地址”的批量经纬度匹配方案，提供数据清洗、候选召回、特征打分与结果导出的一体化脚本，适合作为内部工具或离线任务接入。

## 功能亮点

- **灵活配置**：输入/输出路径与字段映射全部通过 `config.yaml` 控制，便于对接不同 Excel/CSV。
- **地址标准化**：结巴分词 + 拼音 + 正则规则提取门牌、道路关键信息，保证召回前的数据一致性。
- **候选召回**：默认构建倒排索引进行 Token 匹配，可扩展向量召回或接入外部搜索服务。
- **特征打分**：利用 `rapidfuzz` 计算编辑距离，结合 Token 覆盖率与门牌一致性给出综合得分，支持阈值过滤。
- **批量导出**：输出匹配结果、得分、候选缓存，便于人工复核或后续建模。

## 环境准备

1. **Conda 安装（推荐）**
   ```bash
   # 只示例关键依赖，可按 docs/re.txt 逐条安装
   conda install pandas=2.2 openpyxl=3.1 pydantic=2.8 rapidfuzz=3.9 jieba=0.42 pypinyin=0.49 typer=0.12 pyyaml=6.0 loguru=0.7 tqdm=4.66
   ```
   如某些包在默认渠道不可用，可切换到 `conda-forge`。

2. **安装项目（必需）**
   ```bash
   python -m pip install -e . --no-deps
   ```
   若要开启向量召回功能，再执行 `pip install -e .[vector]`。

3. **数据配置**
   - 复制 `config.example.yaml` 为 `config.yaml`；
   - 按实际文件路径填写 `poi_file`、`address_file`、`output_file`；
   - 确保字段映射与 Excel 列名一致。

## 使用方式

```bash
geo-matcher config.yaml
```

运行后脚本会依次完成：

1. 加载 POI 与快递地址；
2. 完成地址标准化、构建倒排索引；
3. 对每条地址召回候选、计算得分；
4. 将 `MatchResult` 写入配置指定的 Excel。

日志默认使用 `INFO` 级别，可在 `config.yaml` 的 `runtime.log_level` 中调整。

## 工作流示意

```
Excel 读入 → 地址清洗/分词 → 构建倒排索引 → 候选召回 → rapidfuzz 特征打分 → 结果导出
```

核心模块：

| 文件 | 作用 |
| ---- | ---- |
| `src/geo_matcher/config.py` | 解析 YAML 配置、校验字段 |
| `src/geo_matcher/address_cleaner.py` | Excel 地址拆分与清洗逻辑 |
| `src/geo_matcher/normalizer.py` | 地址文本标准化、提取门牌 |
| `src/geo_matcher/indexers.py` | 建立倒排索引 |
| `src/geo_matcher/retrievers.py` | 候选召回逻辑（可扩展） |
| `src/geo_matcher/scorer.py` | 调用 rapidfuzz 计算编辑距离并综合打分 |
| `src/geo_matcher/pipeline.py` | 主数据流：加载→召回→得分→导出 |
| `src/geo_matcher/cli.py` | 命令行入口 `geo-matcher config.yaml` |

## 输入数据示例

当前 `config.yaml` 约定：POI 数据为 UTF-8 CSV，字段以空格仅为演示，实际文件中为英文逗号分隔，包含以下列（表头需与示例一致）：

| 列名 | 含义 |
| --- | --- |
| `id` | POI 唯一编码，对应 `columns.poi.poi_id` |
| `name` | POI 名称 |
| `type`/`typecode`/`bigType`/`midType`/`smallType` | 分类信息，默认只读取 `type` |
| `address` | 详细地址（街道、门牌号） |
| `pcode`/`pname`/`citycode`/`cityname`/`adcode`/`adname` | 省市区编码+名称 |
| `lon_gcj02` `lat_gcj02` `lon_wgs84` `lat_wgs84` | 坐标，可任选一组与 `columns.poi.longitude/latitude` 对齐 |

```
B01C30J4ZG 奔马汽配城 汽车服务;汽车配件销售;汽车配件销售 10800 宣化街477号 0451-82526666 230000 黑龙江省 451 哈尔滨市 230103 南岗区 汽车服务 汽车配件销售 汽车配件销售 126.668667 45.768254 126.6626423 45.76626984
```

快递地址数据来自 Excel（示例 sheet：`kx_jtu`），需包含以下列：

| 列名 | 含义 |
| --- | --- |
| `receive_province_name` | 收件省份 |
| `receive_city_name` | 收件城市 |
| `receive_district_name` | 收件区县 |
| `receive_mailing_address` | 详细地址（街道+门牌+店名等），同时映射到 `raw_address` 与 `street` |

```
黑龙江省 哈尔滨市 道里区 西四道街33号你好酒店(哈尔滨中央大街店)
```

## 地址清洗工具

如需仅对 Excel 中的原始地址列进行拆分，可使用新增命令：

```bash
geo-clean -i data/raw_addresses.xlsx -c 地址 -o output/cleaned.xlsx
```

- `-c/--column`：要处理的列名，默认为“地址”；
- `-o/--output`：输出文件路径，若省略则写入与输入同目录的 `_clean` 文件。

工具会输出以下列：省份、地市、区县、街道/乡镇、道路名称、门牌号、小区/屯名称、楼栋、单元、户室、备注。例如：

| 原始地址 | 省份 | 地市 | 区县 | 街道/乡镇 | 小区/屯名称 | 楼栋 | 单元 | 户室 | 备注 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 黑龙江省哈尔滨市松北区松北街道北岸润和城一期6栋2单元1201（不放丰巢谢谢）【配送请拨打18445786875-5835】 | 黑龙江省 | 哈尔滨市 | 松北区 | 松北街道 | 北岸润和城一期 | 6栋 | 2单元 | 1201 | 不放丰巢谢谢；配送请拨打18445786875-5835 |

## 常见问题

- **`FileNotFoundError: data\poi.xlsx`**：检查 `config.yaml` 中的路径是否真实存在，必要时使用绝对路径。
- **`pkg_resources is deprecated` 警告**：由 `jieba` 依赖触发，可忽略，不影响运行。
- **Conda 环境迁移**：在旧机器运行 `conda env export --from-history > environment.yaml`，在新机器执行 `conda env create -f environment.yaml`。
