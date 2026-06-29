# MetroPT-3 虚拟机测试执行清单

版本：v1.0  
日期：2026-06-01  
适用目录：`/home/common/tmp/pycharm_Design`  
集群配置：`config/metropt_quality.cluster.yaml`

## 1. 测试目标

本清单用于在三节点虚拟机集群中验证 MetroPT-3 项目链路：

1. 平台基础服务可用。
2. MetroPT 原始数据已上传到本地临时目录和 HDFS。
3. 离线链路能跑到 ODS、DWD、DWS。
4. Hive/Iceberg 发布和 BI 视图能创建并查询。
5. 数据分析和 baseline 建模能基于 DWS 产物运行。
6. Kafka replay、Flink 实时聚合、Redis KPI、DLQ 能形成小闭环。
7. 每个失败点都能定位到日志、命令和组件。

## 2. 执行规则

- 先跑基础平台检查，再跑项目任务。
- 先跑离线到 DWS，再跑分析和实时。
- 失败时不要跳到下游步骤；先保留日志并反馈。
- 不要临时重装 Hadoop、Hive、Spark、Kafka、Flink 等平台底座。
- Kafka 副本数、Flink connector、Iceberg catalog、Doris 同步策略如需降级，先记录现象再决策。
- Doris 端口判断不要只看端口号；`8040` 在当前集群可能是 YARN NodeManager，不要误杀。

## 3. Windows 侧数据上传

如果虚拟机上还没有原始 CSV，先在 Windows PowerShell 中执行：

```powershell
scp "<repo-root>\datas\MetroPT3_AirCompressor.csv" common@192.168.88.101:/home/common/tmp/metropt_quality/
scp "<repo-root>\datas\Data Description_Metro.pdf" common@192.168.88.101:/home/common/tmp/metropt_quality/
```

如果目录不存在，先在 `hadoop1` 中执行：

```bash
mkdir -p /home/common/tmp/metropt_quality
```

## 4. VM 基础环境检查

在 `hadoop1` 执行：

```bash
cd /home/common/tmp/pycharm_Design
source /etc/profile.d/bigdata.sh
source ~/.bashrc
export METROPT_CONFIG=/home/common/tmp/pycharm_Design/config/metropt_quality.cluster.yaml

hostname
date
type -a python python3
python - <<'PY'
import sys
print(sys.executable)
PY
python --version
command -v spark-submit
spark-submit --version | head -n 20
echo "$METROPT_CONFIG"
test -f "$METROPT_CONFIG" && echo "config ok"
```

验收标准：

- 当前目录是 `/home/common/tmp/pycharm_Design`。
- `METROPT_CONFIG` 指向 cluster 配置。
- `spark-submit` 可执行。

失败时反馈：

```bash
pwd
ls -lh
echo "$PATH"
echo "$JAVA_HOME"
type -a python python3
python - <<'PY'
import sys
print(sys.executable)
PY
command -v spark-submit
```

## 5. 平台基础服务检查

### 5.1 HDFS / YARN

```bash
jps -l
hdfs dfs -ls /
hdfs dfsadmin -report | head -n 80
yarn node -list
yarn scheduler -status default 2>/dev/null || true
```

验收标准：

- NameNode、DataNode、ResourceManager、NodeManager 正常。
- `hdfs dfs -ls /` 能返回目录。
- YARN 至少看到可用 NodeManager。
- 如果 YARN 最大 container 只有 `1024 MB`，当前 `config/metropt_quality.cluster.yaml` 已按单 executor `640m + 384m overhead` 适配；不要在未调整 YARN 前把 executor 改回 6GB。

### 5.2 Hive Metastore / HiveServer2

```bash
ss -lntp | egrep ':9083|:10000' || true
beeline -u jdbc:hive2://hadoop1:10000 -e "SHOW DATABASES;"
```

验收标准：

- `9083` 有 Hive Metastore。
- `10000` 有 HiveServer2。
- Beeline 能执行 `SHOW DATABASES`。

### 5.3 Kafka / Redis / Flink

```bash
/export/server/kafka/bin/kafka-topics.sh --bootstrap-server 192.168.88.101:9092 --list
redis-cli -h 192.168.88.101 -p 6379 ping
export FLINK_HOME=/export/server/flink
export HIVE_CONF_DIR=/export/server/hive/conf
test -d "$FLINK_HOME/lib" && ls -1 "$FLINK_HOME/lib" | egrep 'kafka|hive|iceberg|json|connector' || true
/export/server/flink/bin/flink list
```

验收标准：

- Kafka topic 命令可执行。
- Redis 返回 `PONG`。
- Flink lib 中能看到 Kafka/Hive/JSON 相关 connector。
- Flink list 命令可执行。

## 6. 数据上传到 HDFS

在 `hadoop1` 执行：

```bash
ls -lh /home/common/tmp/metropt_quality/MetroPT3_AirCompressor.csv
ls -lh /home/common/tmp/metropt_quality/Data\ Description_Metro.pdf 2>/dev/null || true

hdfs dfs -mkdir -p /lakehouse/projects/metropt_quality/raw
hdfs dfs -put -f /home/common/tmp/metropt_quality/MetroPT3_AirCompressor.csv /lakehouse/projects/metropt_quality/raw/MetroPT3_AirCompressor.csv
hdfs dfs -put -f "/home/common/tmp/metropt_quality/Data Description_Metro.pdf" /lakehouse/projects/metropt_quality/raw/Data_Description_Metro.pdf 2>/dev/null || true

hdfs dfs -ls -h /lakehouse/projects/metropt_quality/raw
```

验收标准：

- HDFS 中存在 `MetroPT3_AirCompressor.csv`。
- 文件大小约为 200MB 级别。

## 7. 离线链路测试

### 7.1 单步预检

```bash
cd /home/common/tmp/pycharm_Design
source /etc/profile.d/bigdata.sh
export METROPT_CONFIG=/home/common/tmp/pycharm_Design/config/metropt_quality.cluster.yaml

spark-submit src/00_metropt_preflight.py
```

验收标准：

- 输出 `MetroPT preflight passed.`。
- 能识别 7 个模拟传感器和 8 个离散传感器。
- 不应因为 HDFS CSV 第一列显示 `_c0` 而失败。

### 7.2 跑到 DWS

先只跑到 `04_metropt_kpi_calc.py`，用于验证 ODS/DWD/DWS 主链路：

```bash
python src/run_metropt_offline.py --stop-after 04_metropt_kpi_calc.py
```

验收标准：

- runner 输出 `preflight_executor: spark-submit`。
- `00` 到 `04` 全部 return_code 为 `0`。
- DWD 行数应等于 `1,516,948 * 15 = 22,754,220`。
- DWS overall/window/sensor 都非空。

查看日志：

```bash
ls -1 data/metropt_quality/logs | tail
RUN_ID=$(ls -1 data/metropt_quality/logs | tail -n 1)
cat data/metropt_quality/logs/$RUN_ID/offline_run_summary.tsv
tail -n 120 data/metropt_quality/logs/$RUN_ID/04_metropt_kpi_calc.log
```

检查 HDFS 输出：

```bash
hdfs dfs -du -h /lakehouse/projects/metropt_quality/ods
hdfs dfs -du -h /lakehouse/projects/metropt_quality/dwd
hdfs dfs -du -h /lakehouse/projects/metropt_quality/dws
hdfs dfs -ls -h /lakehouse/projects/metropt_quality/dws/window_kpi | head
```

### 7.3 发布 Hive / Iceberg / BI 视图

DWS 成功后执行：

```bash
python src/run_metropt_offline.py --start-at 05_metropt_to_hive_iceberg.py
```

验收标准：

- `05_metropt_to_hive_iceberg.py` 成功写 Hive 表。
- 如果 Iceberg catalog 正常，Iceberg 表也成功写入。
- `06_metropt_hive_views.py` 创建 BI 视图成功。

Hive 对象和样例验收：

```bash
beeline -u jdbc:hive2://hadoop1:10000 -e "
USE metropt_quality;
SHOW TABLES;
SELECT * FROM ods_metropt_readings LIMIT 5;
SELECT * FROM dwd_metropt_sensor_long LIMIT 5;
SELECT * FROM vw_pbi_metropt_window_kpi LIMIT 5;
SELECT * FROM vw_pbi_metropt_sensor_kpi LIMIT 5;
"
```

Hive `COUNT(*)` 会触发 Hive-on-MR。2026-06-05 已修正 hadoop1 的 Hive 专用 JDK8 配置：HiveServer2、MR AM、map、reduce container 均使用 JDK8，并关闭 `mapreduce.jvm.add-opens-as-default`。行数复验优先使用普通 Beeline：

```bash
export JAVA_HOME=/export/server/jdk8
/export/server/hive/bin/beeline \
  -u "jdbc:hive2://hadoop1:10000/default" \
  -n common \
  --showHeader=false \
  --outputformat=tsv2 \
  -e "USE metropt_quality;
SELECT 'ods_metropt_readings', COUNT(*) FROM ods_metropt_readings;
SELECT 'dwd_metropt_sensor_long', COUNT(*) FROM dwd_metropt_sensor_long;
SELECT 'dws_metropt_window_kpi', COUNT(*) FROM dws_metropt_window_kpi;
SELECT 'dws_metropt_sensor_kpi', COUNT(*) FROM dws_metropt_sensor_kpi;"
```

2026-06-05 已验证的期望结果：

```text
ods_metropt_readings        1516948
dwd_metropt_sensor_long     22754220
dws_metropt_window_kpi      269991
dws_metropt_sensor_kpi      15
```

如果后续普通 Beeline 再次出现 `InaccessibleObjectException`、`NoSuchFieldException` 或 `Unrecognized option: --add-opens`，先不要改业务代码，改用回退脚本确认是否为配置漂移：

```bash
cd /home/common/tmp/pycharm_Design
bin/metropt_hive_mr_count_check.sh --mode offline
```

Trino 验收：

```bash
trino --server http://hadoop1:8080 --execute "SHOW CATALOGS"
trino --server http://hadoop1:8080 --execute "SELECT node_id,http_uri,node_version,coordinator,state FROM system.runtime.nodes"
trino --server http://hadoop1:8080 --execute "SHOW SCHEMAS FROM iceberg"
trino --server http://hadoop1:8080 --execute "SHOW TABLES FROM iceberg.metropt_quality_iceberg"

trino --server http://hadoop1:8080 --execute "SELECT COUNT(*) FROM iceberg.metropt_quality_iceberg.ods_metropt_readings"
trino --server http://hadoop1:8080 --execute "SELECT COUNT(*) FROM iceberg.metropt_quality_iceberg.dws_metropt_window_kpi"
trino --server http://hadoop1:8080 --execute "SELECT * FROM iceberg.metropt_quality_iceberg.dws_metropt_sensor_kpi LIMIT 5"
```

如果 Iceberg 失败但 Hive 表成功，保留失败日志，先不要反复重跑全链路。

## 8. 数据分析与建模测试

### 8.1 Python 依赖检查

```bash
python - <<'PY'
import pandas, numpy, matplotlib, seaborn, sklearn, pyarrow, yaml
print("analysis deps ok")
print("pandas", pandas.__version__)
print("numpy", numpy.__version__)
print("sklearn", sklearn.__version__)
PY
```

如果失败，反馈缺失包名和当前 Python 路径：

```bash
type -a python python3
python - <<'PY'
import sys
print(sys.executable)
PY
python --version
python -m pip list | egrep 'pandas|numpy|matplotlib|seaborn|scikit|sklearn|pyarrow|pyspark' || true
```

验收口径：

- `type -a python` 第一条应显示 `alias python='/usr/bin/python3'`，或 `sys.executable` 输出 `/usr/bin/python3`。
- 如果 `/export/server/jdk17/bin/python` 排在第一位，不要继续安装依赖，先按 `通用大数据流程配置.md` 的环境变量章节修复 `PATH` 和 `~/.bashrc`。

### 8.2 输入完整性检查

```bash
python analysis/run_metropt_analysis.py --stop-after 00_validate_analysis_inputs.py
```

验收标准：

- cluster 配置下 runner 输出 `step_executor: spark-submit`。
- `00_validate_analysis_inputs.py` return_code 为 `0`。
- 报告显示 ODS/DWD/DWS 均存在。

### 8.3 完整分析和 baseline

```bash
python analysis/run_metropt_analysis.py
```

验收标准：

- `01_data_quality_analysis.py` 成功。
- `02_multidim_analysis.py` 成功。
- `03_model_baseline.py` 成功或明确说明训练集只有一个类别而跳过。
- 产物写入：
  - `data/metropt_quality/analysis/reports`
  - `data/metropt_quality/analysis/figures`
  - `data/metropt_quality/analysis/models`
  - `data/metropt_quality/analysis/logs`

查看日志：

```bash
RUN_ID=$(ls -1 data/metropt_quality/analysis/logs | tail -n 1)
cat data/metropt_quality/analysis/logs/$RUN_ID/analysis_run_summary.tsv
tail -n 120 data/metropt_quality/analysis/logs/$RUN_ID/03_model_baseline.log
find data/metropt_quality/analysis/reports -maxdepth 1 -type f -print
find data/metropt_quality/analysis/figures -maxdepth 1 -type f -print
find data/metropt_quality/analysis/models -maxdepth 1 -type f -print
```

## 9. 实时链路测试

### 9.1 创建 Kafka topic

```bash
/export/server/kafka/bin/kafka-topics.sh \
  --bootstrap-server 192.168.88.101:9092 \
  --create \
  --if-not-exists \
  --topic metropt.ods.compressor.reading.v1 \
  --partitions 3 \
  --replication-factor 3

/export/server/kafka/bin/kafka-topics.sh \
  --bootstrap-server 192.168.88.101:9092 \
  --create \
  --if-not-exists \
  --topic metropt.ods.compressor.reading.dlq.v1 \
  --partitions 3 \
  --replication-factor 3

/export/server/kafka/bin/kafka-topics.sh \
  --bootstrap-server 192.168.88.101:9092 \
  --list | grep metropt
```

如果 replication factor 3 失败，先反馈 broker 状态，不要直接降为 1：

```bash
/export/server/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server 192.168.88.101:9092 | head -n 80
jps -l | grep kafka || true
```

### 9.2 Replay dry-run

```bash
python streaming/metropt_replay_to_kafka.py \
  --config /home/common/tmp/pycharm_Design/config/metropt_quality.cluster.yaml \
  --dry-run \
  --print-sample 3 \
  --max-events 3
```

验收标准：

- 打印 3 条 JSON。
- 字段包含 `event_id`、`event_time`、`operating_state`、`is_failure_window` 和 15 个传感器字段。

### 9.3 启动 Flink 实时作业

建议新开一个终端执行，或后台运行：

```bash
export FLINK_HOME=/export/server/flink
export HIVE_CONF_DIR=/export/server/hive/conf
export METROPT_CONFIG=/home/common/tmp/pycharm_Design/config/metropt_quality.cluster.yaml

mkdir -p data/metropt_quality/realtime_logs
/export/server/venv/flink120/bin/python - <<'PY'
import importlib.util
for mod in ("pkg_resources", "pyflink", "redis", "yaml"):
    if importlib.util.find_spec(mod) is None:
        raise SystemExit(f"missing {mod}")
print("pyflink venv deps ok")
PY

/export/server/venv/flink120/bin/python streaming/01_flink_metropt_kafka_to_hive.py --startup-mode earliest-offset \
  > data/metropt_quality/realtime_logs/flink_metropt_realtime.log 2>&1 &
echo $! > data/metropt_quality/realtime_logs/flink_metropt_realtime.pid
sleep 15
tail -n 120 data/metropt_quality/realtime_logs/flink_metropt_realtime.log

PID=$(cat data/metropt_quality/realtime_logs/flink_metropt_realtime.pid)
ps -fp "$PID" || true
/export/server/flink/bin/flink list
```

验收标准：

- 输出 `MetroPT Flink 作业已提交`。
- 如果出现 `缺少 Hive dialect ParserFactory，改用 default dialect + connector='hive'`，属于可接受降级路径；继续看作业是否提交成功。
- 如果日志只有 fallback 提示一行，先不要判失败；检查 `ps -fp $(cat data/metropt_quality/realtime_logs/flink_metropt_realtime.pid)` 和 `/export/server/flink/bin/flink list`。
- 如果出现 `HiveCatalog currently only supports timestamp of precision 9`，说明 VM 代码还没有同步 `TIMESTAMP(9)` fallback 版本，先同步 `streaming/01_flink_metropt_kafka_to_hive.py` 后重试。
- 如果出现 `Streaming write to partitioned hive table ... without providing a commit policy`，说明 VM 代码还没有同步 Hive sink partition commit policy 版本，先同步 `streaming/01_flink_metropt_kafka_to_hive.py` 后重试。
- 没有 connector、Hive conf、Redis 依赖、Kafka bootstrap 格式错误。

### 9.4 小批量发送 Kafka

```bash
python streaming/metropt_replay_to_kafka.py \
  --config /home/common/tmp/pycharm_Design/config/metropt_quality.cluster.yaml \
  --rate 100 \
  --batch-size 500 \
  --max-events 10000
```

Kafka 消费检查：

```bash
/export/server/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server 192.168.88.101:9092 \
  --topic metropt.ods.compressor.reading.v1 \
  --from-beginning \
  --max-messages 5 \
  --timeout-ms 10000
```

Flink 作业检查：

```bash
/export/server/flink/bin/flink list
tail -n 120 data/metropt_quality/realtime_logs/flink_metropt_realtime.log
```

Hive 实时表检查：

```bash
beeline -u jdbc:hive2://hadoop1:10000 -e "
USE metropt_quality;
SHOW TABLES LIKE '*realtime*';
DESCRIBE FORMATTED ods_metropt_realtime_readings;
SHOW PARTITIONS ods_metropt_realtime_readings;
SELECT * FROM ods_metropt_realtime_readings LIMIT 5;
SELECT * FROM dws_metropt_realtime_kpi_1min LIMIT 10;
"
```

`SELECT COUNT(*)` 会触发 Hive-on-MR。需要做实时表 COUNT 时，使用当前已修正的普通 Beeline 路径；若失败，再用 7.3 的回退脚本判断是否为配置漂移：

```sql
USE metropt_quality;
SELECT COUNT(*) AS ods_metropt_realtime_readings_count FROM ods_metropt_realtime_readings;
SELECT COUNT(*) AS dws_metropt_realtime_kpi_1min_count FROM dws_metropt_realtime_kpi_1min;
```

Redis 检查：

```bash
redis-cli -h 192.168.88.101 -p 6379 --scan --pattern 'metropt:kpi:1m*' | head
KEY=$(redis-cli -h 192.168.88.101 -p 6379 --scan --pattern 'metropt:kpi:1m*' | head -n 1)
test -n "$KEY" && redis-cli -h 192.168.88.101 -p 6379 HGETALL "$KEY"
```

DLQ 检查：

```bash
/export/server/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server 192.168.88.101:9092 \
  --topic metropt.ods.compressor.reading.dlq.v1 \
  --from-beginning \
  --max-messages 5 \
  --timeout-ms 10000
```

正常数据没有进入 DLQ 也可以接受。若要强制测试 DLQ，可在确认 Flink 作业运行后发送一条缺字段事件：

```bash
echo '{"event_id":null,"raw_index":null}' | /export/server/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server 192.168.88.101:9092 \
  --topic metropt.ods.compressor.reading.v1

sleep 10

/export/server/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server 192.168.88.101:9092 \
  --topic metropt.ods.compressor.reading.dlq.v1 \
  --from-beginning \
  --max-messages 1 \
  --timeout-ms 10000
```

验收口径：

- 正常 replay 后 DLQ 为 0 条是通过。
- 强制坏消息注入后，DLQ 应至少能消费到 1 条异常事件。

## 10. Doris / Trino / 调度补充验收

### 10.1 Doris 健康检查

```bash
mysql -h 192.168.88.101 -P 9030 -uroot -e "SHOW FRONTENDS;"
mysql -h 192.168.88.101 -P 9030 -uroot -e "SHOW BACKENDS;"
```

验收标准：

- `SHOW BACKENDS` 中 BE 应为 `Alive=true`。
- 如看到 `8040`，先用 `/proc/<pid>/cmdline` 确认是否为 YARN NodeManager，不要直接 kill。

### 10.2 DolphinScheduler 最小调度验收

第一版只调度离线链路：

```bash
python src/run_metropt_offline.py --stop-after 04_metropt_kpi_calc.py
```

在 DolphinScheduler 中创建 Shell 任务执行上述命令，验收：

- 工作流实例成功。
- 能看到 stdout/stderr。
- 失败时能定位到 runner 日志目录。

## 11. 失败时反馈模板

如果任何步骤失败，请按下面格式反馈，不要只发最后一行报错：

```text
阶段：
执行节点：
执行命令：
是否使用 cluster 配置：
失败脚本：
return_code：
关键报错：
```

必须附带以下输出：

```bash
pwd
echo "$METROPT_CONFIG"
type -a python python3
python - <<'PY'
import sys
print(sys.executable)
PY
python --version
command -v spark-submit

# 如果是离线失败
RUN_ID=$(ls -1 data/metropt_quality/logs | tail -n 1)
cat data/metropt_quality/logs/$RUN_ID/offline_run_summary.tsv
tail -n 160 data/metropt_quality/logs/$RUN_ID/<失败脚本去掉.py>.log

# 如果是分析失败
RUN_ID=$(ls -1 data/metropt_quality/analysis/logs | tail -n 1)
cat data/metropt_quality/analysis/logs/$RUN_ID/analysis_run_summary.tsv
tail -n 160 data/metropt_quality/analysis/logs/$RUN_ID/<失败脚本去掉.py>.log

# 如果是实时失败
tail -n 160 data/metropt_quality/realtime_logs/flink_metropt_realtime.log
/export/server/flink/bin/flink list
/export/server/kafka/bin/kafka-topics.sh --bootstrap-server 192.168.88.101:9092 --list | grep metropt
```

把 `<失败脚本去掉.py>` 替换成实际日志名，例如：

- `00_metropt_preflight`
- `04_metropt_kpi_calc`
- `03_model_baseline`

## 12. 通过标准汇总

- `spark-submit src/00_metropt_preflight.py` 通过。
- `python src/run_metropt_offline.py --stop-after 04_metropt_kpi_calc.py` 通过。
- ODS 行数约等于 `1,516,948`。
- DWD 行数等于 `22,754,220`。
- DWS overall/window/sensor 非空。
- Hive 表和 BI 视图可查。
- 分析报告、图表、模型结果生成。
- Kafka 主 topic 有消息。
- Flink 实时作业启动并消费。
- Hive 实时表有数据。
- Redis 有 `metropt:kpi:1m` key。
- DLQ 能查询，强制坏消息测试时能收到异常事件。

## 13. 已知错误：YARN 单容器内存上限过低

### 现象

`spark-submit src/00_metropt_preflight.py` 初始化 SparkContext 时报错：

```text
Required executor memory (6144 MB), overhead (512 MB) is above the max threshold (1024 MB)
```

### 判断

这不是 MetroPT 数据、HDFS 路径或 PySpark 导入问题。Spark 已经读取到 cluster 配置并准备提交 YARN，但当前 YARN 单个 container 最大只允许 `1024 MB`，而旧配置申请了 `6GB` executor 和 `512MB` overhead。

### 当前修正

项目 cluster 配置已按当前 YARN 上限调低：

```yaml
spark.executor.instances: 3
spark.executor.cores: 1
spark.executor.memory: "640m"
spark.executor.memoryOverhead: "384m"
spark.driver.memory: "1g"
spark.sql.shuffle.partitions: 32
```

同步最新代码后重新执行：

```bash
cd /home/common/tmp/pycharm_Design
source /etc/profile.d/bigdata.sh
export METROPT_CONFIG=/home/common/tmp/pycharm_Design/config/metropt_quality.cluster.yaml
spark-submit src/00_metropt_preflight.py
```

### 如果仍失败，反馈这些输出

```bash
grep -R "yarn.scheduler.maximum-allocation-mb\|yarn.nodemanager.resource.memory-mb" /export/server/hadoop/etc/hadoop/yarn-site.xml
yarn node -list
yarn scheduler -status default 2>/dev/null || true
cat config/metropt_quality.cluster.yaml | sed -n '25,45p'
```

如果后续想恢复 `6g` executor，必须先在 Hadoop/YARN 配置中提高 `yarn.scheduler.maximum-allocation-mb` 和 `yarn.nodemanager.resource.memory-mb`，同步三台并重启 YARN；否则 Spark 会继续在提交阶段失败。
