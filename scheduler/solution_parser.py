import calendar
import os
import pickle
from datetime import date, datetime

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from matplotlib.ticker import MaxNLocator

from scheduler.model import daterange, solve_batch


def split_solution_to_seasonal_batches(df_all_impute, solution, seasons, config):
    O = df_all_impute["Asset"]
    M = [calendar.month_abbr[i] for i in range(1, 13)]

    df_soln = df_all_impute.copy()
    df_soln["Assigned Month"] = ""
    for i, o in enumerate(O):
        for m in M:
            if solution[o, m] == 1:
                df_soln.loc[df_soln.index[i], "Assigned Month"] = int(
                    datetime.strptime(m, "%b").month
                )

    df_soln.loc[df_soln["Assigned Month"] >= 0, "Season"] = "Summer"
    df_soln.loc[df_soln["Assigned Month"] >= 3, "Season"] = "Autumn"
    df_soln.loc[df_soln["Assigned Month"] >= 6, "Season"] = "Winter"
    df_soln.loc[df_soln["Assigned Month"] >= 9, "Season"] = "Spring"
    df_soln.loc[df_soln["Assigned Month"] >= 12, "Season"] = "Summer"

    for batch_season in seasons:
        df_batch = df_soln[df_soln["Season"] == batch_season]
        df_batch.to_excel(
            f"outputs/05_season_batches_{batch_season}.xlsx", index=False
        )

    return df_soln


def save_data(path, z, c, batch_name, O, D):
    if not os.path.exists(path):
        os.makedirs(path)

    outage_day_pairs = [(o, d) for o in O for d in D if z[(o, d)] > 0]
    solution = pd.DataFrame(columns=["Asset", "Day", "isOut", "isStart"])
    solution["Asset"] = [pair[0] for pair in outage_day_pairs]
    solution["Day"] = [pair[1] for pair in outage_day_pairs]
    solution["isOut"] = [z[pair[0], pair[1]] for pair in outage_day_pairs]
    solution["isStart"] = [c[pair[0], pair[1]] for pair in outage_day_pairs]
    solution["cumsum_out"] = solution.groupby(["Day"])["isOut"].cumsum()

    filename = f"{path}{batch_name.split('.')[0]}.xlsx"
    solution.to_excel(filename, index=False)


def plot_solution(config, solution, start_year, season, method="dense", save_path=None):
    start_date = date(start_year, 7, 1)
    end_date = date(start_year + 1, 7, 1)

    D = []
    for single_date in daterange(start_date, end_date):
        month = single_date.month
        if season == "Summer" and (month == 1 or month == 2 or month == 12):
            D.append(single_date)
        elif season == "Autumn" and (month == 3 or month == 4 or month == 5):
            D.append(single_date)
        elif season == "Winter" and (month == 6 or month == 7 or month == 8):
            D.append(single_date)
        elif season == "Spring" and (month == 9 or month == 10 or month == 11):
            D.append(single_date)
        elif season == "FULL":
            D.append(single_date)

    fig, ax = plt.subplots(figsize=(50, 10))
    ax.yaxis.set_major_locator(MaxNLocator(integer=True))

    for d in D:
        if d.weekday() > 4:
            plt.axvline(x=d, lw=30, color="lightgrey", label="axvline - full height")
        if d in config["public_holidays"]:
            plt.axvline(x=d, lw=30, color="silver", label="axvline - full height")

    if method == "dense":
        sns.lineplot(
            data=solution,
            x="Day",
            y="cumsum_out",
            hue="Asset",
            marker="o",
            markersize=15,
            linewidth=10,
            errorbar=None,
        )
        for i, txt in enumerate(solution["Asset"]):
            if solution["isStart"][i] == 1:
                plt.text(
                    solution["Day"][i],
                    solution["cumsum_out"][i] + 0.2,
                    solution["Asset"][i],
                    fontsize=10,
                )
    else:
        ax.set_ylim([min(solution["Outage"]) - 0.9, max(solution["Outage"]) + 0.9])
        sns.lineplot(
            data=solution, x="Day", y="Outage", hue="Asset", marker="o", errorbar=None
        )
        for i, txt in enumerate(solution["Asset"]):
            if solution["isStart"][i] == 1:
                plt.annotate(txt, (solution["Day"][i], solution["Outage"][i]))

    ax.xaxis.set_major_locator(mdates.DayLocator(interval=7))
    ax.set_title(
        f"Asset Maintenance Schedule — {season} {start_year}/{start_year + 1}",
        fontsize=16,
        pad=12,
    )
    ax.set_xlabel("Date")
    ax.set_ylabel("Concurrent outages (cumulative)" if method == "dense" else "Outage index")
    plt.legend(loc="upper left", bbox_to_anchor=(1, 1))
    ax.get_legend().remove()
    if save_path is not None:
        fig.savefig(save_path, bbox_inches="tight", dpi=150)
    plt.show()


def solve_assets_for_each_season(seasons, config):
    for batch_season in seasons:
        with open(f"outputs/06_mvs_groups_{batch_season}.pkl", "rb") as f:
            df_batch = pickle.load(f)
        z, c, _, _, _, _, O, D = solve_batch(
            df_batch,
            config["public_holidays"],
            start_year=config["aop_year"],
            season=batch_season,
        )
        save_data("outputs/", z, c, f"07_day_schedule_{batch_season}", O, D)


def read_and_plot_batch_season_solutions(seasons, config, output_dir=None):
    for batch_season in seasons:
        batch_sln = pd.read_excel(f"outputs/07_day_schedule_{batch_season}.xlsx")
        print(batch_season)
        if output_dir is not None:
            import os
            os.makedirs(output_dir, exist_ok=True)
            save_path = os.path.join(
                output_dir,
                f"schedule_{batch_season}_{config['aop_year']}.png",
            )
        else:
            save_path = None
        plot_solution(
            config,
            batch_sln,
            start_year=config["aop_year"],
            season=batch_season,
            method="dense",
            save_path=save_path,
        )


def create_full_season_solutions(seasons, config):
    df_requests = pd.read_excel(config["asset_requests_filepath"])

    for batch_season in seasons:
        batch_sln = pd.read_excel(f"outputs/07_day_schedule_{batch_season}.xlsx")
        df_groupby = batch_sln.groupby(["Asset"])
        df_groupby = df_groupby.agg(date_min=("Day", "min"), date_max=("Day", "max"))
        df_groupby["Planned Start Date"] = df_groupby["date_min"]
        df_groupby["Planned Finish Date"] = df_groupby["date_max"]
        df_groupby = df_groupby[
            ["Planned Start Date", "Planned Finish Date"]
        ].reset_index()

        df_joined = df_groupby.join(
            df_requests.loc[
                :, ~df_requests.columns.isin(["Planned Start Date", "Planned Finish Date"])
            ].set_index("Asset"),
            on="Asset",
        )

        df_joined["Planned Start Time"] = "7:30"
        df_joined["Planned Finish Time"] = "18:30"
        df_joined["Time Frame"] = "Daily"
        df_joined.loc[
            df_joined["Planned Finish Date"] - df_joined["Planned Start Date"]
            > pd.Timedelta(days=1),
            ["Time Frame"],
        ] = "Continuous"

        df_lean = df_joined.copy()
        df_to_insert = pd.DataFrame()

        with open(f"outputs/06_mvs_groups_{batch_season}.pkl", "rb") as f:
            df_batch = pickle.load(f)

        for i in range(0, df_lean.shape[0]):
            current_outage = df_lean.iloc[i]
            group_work = [current_outage["Asset"]]

            for groupie in df_batch[current_outage["Asset"]]["group"][1:]:
                if groupie in list(df_requests["Asset"]):
                    groupie_series = df_requests.loc[
                        df_requests["Asset"] == groupie
                    ].copy()
                    groupie_series["Planned Start Date"] = current_outage["Planned Start Date"]
                    groupie_series["Planned Finish Date"] = current_outage["Planned Finish Date"]
                    groupie_series["Planned Start Time"] = current_outage["Planned Start Time"]
                    groupie_series["Planned Finish Time"] = current_outage["Planned Finish Time"]
                    groupie_series["Time Frame"] = current_outage["Time Frame"]
                    group_work.append(groupie)
                    df_to_insert = pd.concat(
                        [df_to_insert, groupie_series], ignore_index=True
                    )

            for groupie in df_batch[current_outage["Asset"]]["group"][1:]:
                if groupie not in group_work:
                    groupie_row = current_outage.copy()
                    groupie_row["Asset"] = groupie
                    groupie_df = groupie_row.to_frame().T
                    groupie_df["Description of Work"] = (
                        f"Affected equipment {' & '.join(group_work)}"
                    )
                    df_to_insert = pd.concat(
                        [df_to_insert, groupie_df], ignore_index=True
                    )

        df_full = pd.concat([df_lean, df_to_insert], ignore_index=True)
        df_full = df_full.sort_values(by=["Planned Start Date", "Description of Work"])
        df_full.to_excel(
            f"outputs/08b_season_schedule_{batch_season}.xlsx", index=False
        )


def merge_season_solutions(seasons, config):
    df_full_sln = pd.DataFrame()
    for batch_season in seasons:
        season_sln = pd.read_excel(f"outputs/08b_season_schedule_{batch_season}.xlsx")
        df_full_sln = pd.concat([df_full_sln, season_sln], ignore_index=True)

    df_full_sln = df_full_sln.sort_values(
        by=["Planned Start Date", "Description of Work"]
    )
    df_full_sln["Planned Start Date"] = pd.to_datetime(
        df_full_sln["Planned Start Date"]
    ).dt.strftime("%d/%m/%Y")
    df_full_sln["Planned Finish Date"] = pd.to_datetime(
        df_full_sln["Planned Finish Date"]
    ).dt.strftime("%d/%m/%Y")

    aop_year = config["aop_year"]
    aop_year_next = config["aop_year"] + 1
    df_full_sln.to_excel(
        f"outputs/09_draft_aop_{aop_year}_to_{aop_year_next}.xlsx",
        index=False,
    )
    return df_full_sln


def postprocess_data(config):
    aop_year = config["aop_year"]
    aop_year_next = config["aop_year"] + 1
    df_full_sln = pd.read_excel(
        f"outputs/09_draft_aop_{aop_year}_to_{aop_year_next}.xlsx"
    )

    if "ASSET" in df_full_sln.columns:
        return df_full_sln

    transformed_df = pd.DataFrame(
        {
            "ASSET": df_full_sln["Asset"],
            "PLANNED START DATE": df_full_sln["Planned Start Date"],
            "PLANNED START TIME": df_full_sln["Planned Start Time"],
            "PLANNED FINISH DATE": df_full_sln["Planned Finish Date"],
            "PLANNED FINISH TIME": df_full_sln["Planned Finish Time"],
            "NATURE": "RS",
            "TIME FRAME": df_full_sln["Time Frame"],
            "DESCRIPTION OF WORK": df_full_sln["Description of Work"],
            "REQUEST REASONS": df_full_sln["Request Reason"],
            "JOB MANAGER": df_full_sln["Job Manager"],
            "WINDOW OWNERS": df_full_sln["Window Owner"],
            "REQUESTOR": "",
            "MAXIMO SCHEDULING DETAILS": df_full_sln["Work Plan Details"],
        }
    )

    reason_mapping = {
        "APJ": "Project",
        "SEL": "Project",
        "CAP": "Project",
        "PDM": "Station Maintenance",
        "PDM-L": "Station Maintenance",
        "PM": "Station Maintenance",
        "PM Forecast": "Station Maintenance",
    }

    def map_request_reasons(value):
        if pd.isna(value) or value == "":
            return ""
        reasons = [r.strip() for r in str(value).split(",") if r.strip()]
        mapped = [reason_mapping.get(r, r) for r in reasons]
        return ", ".join(dict.fromkeys(mapped))

    transformed_df["REQUEST REASONS"] = transformed_df["REQUEST REASONS"].apply(
        map_request_reasons
    )

    transformed_df.to_excel(
        f"outputs/09_draft_aop_{aop_year}_to_{aop_year_next}.xlsx",
        index=False,
    )
    return transformed_df
