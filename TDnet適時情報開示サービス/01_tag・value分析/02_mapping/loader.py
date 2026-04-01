# --- obsidian_property ---
# scr名: 【自動】
# 概要: mappingfileを読込んで売上,営利,経常,純利益のXBRLのタグ(コンセプト名)と指標名のマッピング表を得る
# 処理grp: -
# 処理順番: 0
# mermaid: 
# tags: ["tdnet", "download"]
# aliases: ["loader.py"]
# created: 2026-03-31
# updated: 【自動】
# folder:【自動】
# file:【自動】
# cssclasses: python_script
# --- obsidian_property ---

# --- 概要 ---
# [!abstract] 概要：mappingfileを読込んで売上,営利,経常,純利益のXBRLのタグ(コンセプト名)と指標名のマッピング表を得る
# mappingfileを指定↓↓↓↓↓↓
# MAPPING_YAML_FILE = "mapping.yaml"
# mappingfileを指定↑↑↑↑↑↑
# --- 概要 ---


from __future__ import annotations

from pathlib import Path
from typing import Any
import yaml
import pandas as pd

# 設定変数:mappingfile
# MAPPING_YAML_FILE = "mapping.yaml"    # 英語版
MAPPING_YAML_FILE = "mapping.xlsx"   # 日本語版


def load_mapping(excel_path: str | Path) -> dict[str, Any]:
    """
    mapping.xlsx を読み込み、使いやすい辞書に整形して返す。

    Excelファイルの形式:
    日本語項目名 | XBRLタグ | コメント
    売上       | NetSales | 純売上高（日本基準）
    売上       | Revenue  | 収益（IFRS）
    ...

    戻り値:
    {
        "raw": 元のDataFrame,
        "metric_to_concepts": {
            "売上": ["NetSales", "Revenue", "OperatingRevenue"],
            ...
        },
        "concept_to_metric": {
            "NetSales": "売上",
            "Revenue": "売上",
            "OperatingRevenue": "売上",
            "OperatingIncome": "営業利益",
            ...
        }
    }
    """
    excel_path = Path(excel_path)

    if not excel_path.exists():
        raise FileNotFoundError(f"Excelファイルが見つかりません: {excel_path}")

    # Excelファイルを読み込み
    df = pd.read_excel(excel_path)
    
    # 必要な列が存在するか確認
    required_columns = ["日本語項目名", "XBRLタグ"]
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Excelファイルに必要な列がありません: {missing_columns}")

    metric_to_concepts: dict[str, list[str]] = {}
    concept_to_metric: dict[str, str] = {}

    for _, row in df.iterrows():
        metric = str(row["日本語項目名"]).strip()
        concept = str(row["XBRLタグ"]).strip()
        
        if not metric or not concept:
            continue
            
        # concept_to_metricに重複チェック
        if concept in concept_to_metric:
            old_metric = concept_to_metric[concept]
            if old_metric != metric:
                raise ValueError(
                    f"XBRLタグ '{concept}' が重複しています。"
                    f" すでに '{old_metric}' に割り当て済みなのに、"
                    f" '{metric}' にも定義されています。"
                )
            continue  # 同じmetricへの重複はスキップ

        # 登録
        concept_to_metric[concept] = metric
        
        if metric not in metric_to_concepts:
            metric_to_concepts[metric] = []
        metric_to_concepts[metric].append(concept)

    return {
        "raw": df,
        "metric_to_concepts": metric_to_concepts,
        "concept_to_metric": concept_to_metric,
    }


def get_metric(concept_name: str, mapping: dict[str, Any]) -> str | None:
    """
    concept名から metric を返す。
    見つからなければ None を返す。
    """
    if not concept_name:
        return None

    concept_to_metric = mapping.get("concept_to_metric", {})
    return concept_to_metric.get(concept_name)


def main() -> None:
    excel_path = __file__.replace("loader.py", MAPPING_YAML_FILE)
    mapping = load_mapping(excel_path)

    print("=== metric_to_concepts ===")
    for metric, concepts in mapping["metric_to_concepts"].items():
        print(f"{metric}: {concepts}")

    print()
    print("=== concept_to_metric ===")
    test_concepts = [
        "NetSales",
        "Revenue",
        "OperatingRevenue",
        "OperatingIncome",
        "OrdinaryIncome",
        "Profit",
        "ProfitAttributableToOwnersOfParent",
        "UnknownConcept",
    ]
    for concept in test_concepts:
        print(f"{concept} -> {get_metric(concept, mapping)}")


if __name__ == "__main__":
    main()