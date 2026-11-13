#!/usr/bin/env bash
# 完整演示脚本：导入三批 -> 撤销坏批（期间并发导入）-> 查看/下载结果
set -euo pipefail
BASE="${BASE:-http://127.0.0.1:8000}"
DIR="$(cd "$(dirname "$0")" && pwd)/demo_data"

echo "① 导入批次 1（正常）/ 2（仪器整批故障）/ 3（后续修正）"
for f in batch1_ok.csv batch2_bad_instrument.csv batch3_corrected.csv; do
  curl -s -F "file=@$DIR/$f" "$BASE/api/upload"; echo
done

echo; echo "② 当前结果（同键取最新批次，坏批值暂时生效）"
curl -s "$BASE/api/results" | python3 -m json.tool

echo; echo "③ 请求撤销批次 2（后台影子重建，约数秒，可在页面观察进度）"
curl -s -X POST -H 'Content-Type: application/json' -d '{"batch_id":2}' "$BASE/api/revoke"; echo

echo "④ 重建期间继续导入批次 4（立即生效，切换时自动合并，不被迟到计算覆盖）"
tmp="$(mktemp -d)/batch4.csv"
printf '站点,采样日,检测项目,结果\n城东站点,2026-11-01,PM2.5,34.5\n新南站,2026-11-03,CO,1.2\n' > "$tmp"
curl -s -F "file=@$tmp" "$BASE/api/upload"; echo

echo; echo "⑤ 等待撤销任务完成"
for _ in $(seq 1 60); do
  line="$(curl -s "$BASE/api/jobs" | python3 -c "import json,sys;j=json.load(sys.stdin)[0];print(j['status'],j['keys_done'],'/',j['keys_total'])")"
  echo "   $line"
  case "$line" in done*|failed*) break;; esac
  sleep 0.5
done

echo; echo "⑥ 撤销后的正式结果：坏批独有键已删除，其余键取最大未撤销批次"
curl -s "$BASE/api/results" | python3 -m json.tool
echo "⑦ 下载当前结果 CSV： $BASE/api/results.csv"
