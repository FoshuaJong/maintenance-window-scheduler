import json
import os
import pickle
import shutil
from pathlib import Path
from datetime import date
import pandas as pd


def load_config(config_file: str = "config.json") -> dict:
    config_path = Path(config_file)
    if not config_path.exists():
        raise FileNotFoundError(f"Config file {config_file} not found")

    with open(config_path, "r") as f:
        config = json.load(f)

    public_holidays = []
    for holiday in config["public_holidays"]["holidays"]:
        year, month, day = holiday["date"].split("-")
        public_holidays.append(date(int(year), int(month), int(day)))

    config["public_holidays"] = public_holidays
    return config


def create_df_impute(
    df_historical_path: str, df_relational_path: str, df_all_path: str
) -> pd.DataFrame:
    df_historical = pd.read_excel(df_historical_path)
    df_relational = pd.read_excel(df_relational_path)
    df_all = pd.read_excel(df_all_path)

    df_all_impute = df_all[["Asset"]]
    df_all_impute = df_all_impute.join(df_historical.set_index("Asset"), on="Asset")
    df_all_impute = df_all_impute.join(df_relational.set_index("Asset"), on="Asset")

    return df_all_impute


def convert_batches_to_mvs(
    seasons: list[str] = ["Summer", "Autumn", "Winter", "Spring"],
) -> None:
    for batch_season in seasons:
        df_batch = pd.read_excel(f"outputs/05_season_batches_{batch_season}.xlsx")

        all_descendants = []
        non_root_assets = []
        candidate_roots = []

        for origin in df_batch["Asset"]:
            if origin in all_descendants:
                non_root_assets.append(origin)
            else:
                candidate_roots.append(origin)
                queue = [origin]

                while queue:
                    asset = queue.pop(0)
                    if asset != origin:
                        all_descendants.append(asset)

                    children = df_batch.loc[
                        df_batch["Asset"] == asset, "Bundled Assets"
                    ].values

                    try:
                        if children.size > 0:
                            children = children[0].split(", ")
                            for child in children:
                                if (
                                    child not in queue
                                    and child not in all_descendants
                                    and child != origin
                                ):
                                    queue.append(child)
                    except AttributeError:
                        pass

        root_assets = []
        for origin in candidate_roots:
            if origin in all_descendants:
                non_root_assets.append(origin)
            else:
                root_assets.append(origin)

        df_historical = pd.read_excel("data/historical_durations.xlsx")
        mvs = {}

        for parent in root_assets:
            group = []
            conflicting_assets = []
            group_duration = 1
            queue = [parent]

            while queue:
                asset = queue.pop(0)
                group.append(asset)

                children = df_batch.loc[
                    df_batch["Asset"] == asset, "Bundled Assets"
                ].values

                try:
                    if children.size > 0:
                        children = children[0].split(", ")
                        for child in children:
                            if child not in queue and child not in group and child != parent:
                                queue.append(child)
                except AttributeError:
                    pass

            for member in group:
                critical = df_batch.loc[
                    df_batch["Asset"] == member, "Conflicting Assets"
                ].values
                try:
                    if member in non_root_assets or member in root_assets:
                        duration = (
                            df_historical.loc[
                                df_historical["Asset"] == member, "HistMedDuration"
                            ]
                            .fillna(1)
                            .values[0]
                        )
                        if duration > group_duration:
                            group_duration = int(duration)
                except IndexError:
                    pass
                try:
                    if isinstance(critical[0], str):
                        for crit in critical[0].split(", "):
                            if crit not in group:
                                conflicting_assets.append(crit)
                except IndexError:
                    pass

            mvs[parent] = {
                "group": group,
                "duration": group_duration,
                "Conflicting Assets": conflicting_assets,
            }

        with open(f"outputs/06_mvs_groups_{batch_season}.pkl", "wb") as f:
            pickle.dump(mvs, f)


def preprocess_data(config: dict) -> pd.DataFrame:
    source_path = config["asset_requests_filepath"]
    backup_path = source_path.replace(".xlsx", "_backup.xlsx")

    if not os.path.exists(backup_path):
        shutil.copy(source_path, backup_path)

    try:
        df = pd.read_excel(backup_path)
    except Exception as e:
        print(f"Error reading file: {e}")
        return

    def concatenate_unique(series):
        values = series.dropna().astype(str)
        values = values[values.str.strip() != ""]
        unique_values = []
        for string in values:
            if "," in str(string):
                parts = [part.strip() for part in str(string).split(",")]
                unique_values.extend(parts)
            else:
                unique_values.append(str(string).strip())
        return ",".join(unique_values) if unique_values else ""

    columns_to_concat = [col for col in df.columns if col != "Asset"]
    grouped = df.groupby("Asset")[columns_to_concat].agg(concatenate_unique)
    df_cleaned = grouped.reset_index()
    df_cleaned.to_excel(f"{config['asset_requests_filepath']}", index=False)

    return df_cleaned
