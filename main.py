from scheduler import (
    load_config,
    preprocess_data,
    create_df_impute,
    distribute_months,
    split_solution_to_seasonal_batches,
    convert_batches_to_mvs,
    solve_assets_for_each_season,
    create_full_season_solutions,
    merge_season_solutions,
    postprocess_data,
)


def run_pipeline():
    seasons = ["Summer", "Autumn", "Winter", "Spring"]

    print("=" * 40)
    print("Load config and data")
    print("=" * 40)
    config = load_config()
    print(f"Asset requests filepath: {config['asset_requests_filepath']}")
    print(f"AOP year: {config['aop_year']}")
    print(f"Number of holidays: {len(config['public_holidays'])}")
    print("First few holidays:")
    for holiday in config["public_holidays"][:3]:
        print(f"  {holiday}")

    print("=" * 40)
    print("Pre-process data - remove duplicates, impute missing values")
    print("=" * 40)
    preprocess_data(config)

    print("=" * 40)
    print("Build imputed asset dataset")
    print("=" * 40)
    df_all_impute = create_df_impute(
        config["historical_durations_filepath"],
        config["relational_dependencies_filepath"],
        config["asset_requests_filepath"],
    )
    print(df_all_impute.head())

    print("=" * 40)
    print("Distribute assets to months and split to seasonal batches")
    print("=" * 40)
    solution = distribute_months(df_all_impute)
    split_solution_to_seasonal_batches(df_all_impute, solution, seasons, config)

    print("=" * 40)
    print("Convert batches to minimum viable sets")
    print("=" * 40)
    convert_batches_to_mvs(seasons)

    print("=" * 40)
    print("Solve day-level schedule for each season")
    print("=" * 40)
    solve_assets_for_each_season(seasons, config)

    # read_and_plot_batch_season_solutions(seasons, config)

    print("=" * 40)
    print("Create full season solutions")
    print("=" * 40)
    create_full_season_solutions(seasons, config)

    print("=" * 40)
    print("Merge season solutions")
    print("=" * 40)
    merge_season_solutions(seasons, config)

    print("=" * 40)
    print("Post-process data")
    print("=" * 40)
    postprocess_data(config)


if __name__ == "__main__":
    run_pipeline()
