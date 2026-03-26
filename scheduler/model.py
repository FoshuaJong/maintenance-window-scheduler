import calendar
from datetime import date, datetime, timedelta
from typing import Generator

import pandas as pd
from ortools.sat.python import cp_model
from tqdm import tqdm


def daterange(start_date: date, end_date: date) -> Generator[date, None, None]:
    """Yield each date from start_date up to (but not including) end_date."""
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)


def distribute_months(data: pd.DataFrame) -> dict:
    """Assign each asset to exactly one calendar month using CP-SAT.

    Minimises the number of non-preferred month assignments jointly with
    workload variance across months (load-balancing term delta).

    Returns a dict keyed by (asset, month_abbr) → 0/1 solution values.
    """
    model = cp_model.CpModel()

    # Sets
    months = [calendar.month_abbr[i] for i in range(1, 13)]
    assets = data["Asset"]
    n_assets = len(assets)

    preferred_months = {
        data["Asset"].iloc[i]: data["Historical and Preferred Months"].iloc[i]
        for i in range(n_assets)
    }

    # Variables
    month_assignment = {
        (a, m): model.NewBoolVar(f"Outage {a} starts in month {m}")
        for a in assets
        for m in months
    }
    monthly_count = {
        m: model.NewIntVar(0, n_assets, f"How many outages are concurrent on month {m}")
        for m in months
    }
    preference_penalty = {(a, m): 0 for a in assets for m in months}

    # Constraints
    # Must run in one of the provided periods
    for a in assets:
        for m in months:
            try:
                if m not in preferred_months[a]:
                    preference_penalty[a, m] += 1
            except TypeError:
                pass
    for a in assets:
        model.Add(sum(month_assignment[a, m] for m in months) == 1)

    # Concurrency limits
    for m in months:
        model.Add(monthly_count[m] == sum(month_assignment[a, m] for a in assets))

    # Groups
    for i, a in enumerate(assets):
        try:
            for groupie in data["Bundled Assets"].iloc[i].split(", "):
                if groupie in list(assets):
                    for m in months:
                        model.AddImplication(
                            month_assignment[a, m], month_assignment[groupie, m]
                        )
        except AttributeError:
            pass

    # Objective Function
    month_avg = n_assets // 12
    delta = model.NewIntVar(0, n_assets, "")
    for m in months:
        model.Add(monthly_count[m] <= month_avg + delta)
        model.Add(monthly_count[m] >= month_avg - delta)
    penalty = model.NewIntVar(0, n_assets, "")
    model.Add(
        penalty
        == sum(month_assignment[a, m] * preference_penalty[a, m] for a in assets for m in months)
    )

    model.Minimize(penalty + delta)

    # Solve
    now = datetime.now()
    print(now.strftime("%H:%M:%S"), "Solving")

    solver = cp_model.CpSolver()
    status = solver.Solve(model)

    solution = {}
    if status == cp_model.OPTIMAL or status == cp_model.FEASIBLE:
        print("optimal" if status == cp_model.OPTIMAL else "feasible")
        for a in assets:
            for m in months:
                solution[a, m] = solver.Value(month_assignment[a, m])
    else:
        print("No solution found.")
        print(solver.SolutionInfo())

    print("\nAdvanced usage:")
    print("Problem solved in %f seconds" % solver.WallTime())

    return solution


def solve_batch(
    data: dict,
    public_holidays: list,
    start_year: int,
    season: str = "summer",
) -> tuple:
    """Schedule MVS groups across working days for a single season using CP-SAT.

    Assigns a contiguous start date to each asset group within the season window,
    respecting conflict constraints and minimising workload variance across working
    days jointly with peak simultaneous starts.

    Returns an 8-tuple:
        running_vals  -- dict (asset, day) → 1 if asset is out for work on that day
        start_vals    -- dict (asset, day) → 1 if asset outage starts on that day
        status        -- CP-SAT solver status code
        n_assets      -- number of asset groups scheduled
        n_days        -- total calendar days in the planning year
        wall_time     -- solver wall-clock time in seconds
        assets        -- ordered list of asset group keys
        days          -- list of date objects in the season window
    """
    model = cp_model.CpModel()

    n_assets = len(data)

    print("Number of Outages: %i Season: %s" % (n_assets, season))

    # Sets
    assets = list(data)

    start_date = date(start_year, 7, 1)
    end_date = date(start_year + 1, 7, 1)
    n_days = int((end_date - start_date).days)

    days = []
    for single_date in daterange(start_date, end_date):
        month = single_date.month
        if season == "Summer" and (month == 1 or month == 2 or month == 12):
            days.append(single_date)
        elif season == "Autumn" and (month == 3 or month == 4 or month == 5):
            days.append(single_date)
        elif season == "Winter" and (month == 6 or month == 7 or month == 8):
            days.append(single_date)
        elif season == "Spring" and (month == 9 or month == 10 or month == 11):
            days.append(single_date)
        elif season == "ALL":
            days.append(single_date)

    asset_duration = {a: data[a]["duration"] for a in assets}

    # Variables
    start_var = {
        (a, d): model.NewBoolVar(f"Outage {a} starts on day {d}") for a in assets for d in days
    }
    running_var = {
        (a, d): model.NewBoolVar(f"Outage {a} runs on day {d}") for a in assets for d in days
    }
    daily_count = {
        d: model.NewIntVar(0, n_assets, f"How many outages are concurrent on day {d}")
        for d in days
    }
    daily_starts = {
        d: model.NewIntVar(0, n_assets, f"How many outages start on day {d}")
        for d in days
    }

    # Constraints
    # An outage can start at most once
    print("Adding constraints: An outage can start at most once")
    for a in tqdm(assets):
        model.AddExactlyOne(start_var[a, d] for d in days)

    # An outage must run for its full duration
    print(
        "Adding constraints: An outage must run for its full duration in a single uninterrupted period"
    )
    for a in tqdm(assets):
        # 1
        for d in days:
            for t in range(0, asset_duration[a]):
                try:
                    model.AddImplication(
                        start_var[a, d], running_var[a, d + timedelta(t)]
                    )
                except KeyError:
                    pass
        # 2
        model.Add(sum(running_var[a, d] for d in days) == asset_duration[a])
        # 3
        for d in days:
            if d + timedelta(asset_duration[a] - 1) not in days:
                model.Add(start_var[a, d] == 0)

    # Concurrency limits
    print("Adding constraints: Constrain number of outages run concurrently per day")
    for d in tqdm(days):
        model.Add(daily_count[d] == sum(running_var[a, d] for a in assets))
        model.Add(daily_starts[d] == sum(start_var[a, d] for a in assets))

    # Clashes
    print("Adding constraints: Conflicting Assets cannot run concurrently")
    for a1 in tqdm(assets):
        try:
            for clasher in data[a1]["Conflicting Assets"]:
                for a2 in assets:
                    if clasher in data[a2]["group"]:
                        for d in days:
                            model.Add(running_var[a2, d] == 0).OnlyEnforceIf(
                                running_var[a1, d]
                            )
        except AttributeError:
            pass

    # Objective Function
    n_workdays = len([d for d in days if d.weekday() < 5 and d not in public_holidays])

    concurrent_workdays_avg = sum(asset_duration[a] for a in assets) // n_workdays
    delta = model.NewIntVar(0, n_assets, "")
    for d in days:
        if d.weekday() < 5 and d not in public_holidays:
            model.Add(daily_count[d] <= concurrent_workdays_avg + delta)
            model.Add(daily_count[d] >= concurrent_workdays_avg - delta)
        else:
            model.Add(daily_count[d] == 0)

    max_concurrent_start = model.NewIntVar(0, n_assets, "")
    model.AddMaxEquality(max_concurrent_start, [daily_starts[d] for d in days])

    model.Minimize(delta + max_concurrent_start)

    # Solve
    now = datetime.now()
    print(now.strftime("%H:%M:%S"), "Solving")

    solver = cp_model.CpSolver()
    status = solver.Solve(model)

    running_vals = {}
    start_vals = {}
    if status == cp_model.OPTIMAL or status == cp_model.FEASIBLE:
        print("optimal" if status == cp_model.OPTIMAL else "feasible")
        print(solver.ObjectiveValue())
        for a in assets:
            for d in days:
                running_vals[a, d] = solver.Value(running_var[a, d])
                start_vals[a, d] = solver.Value(start_var[a, d])
    else:
        print("No solution found.")
        print(solver.SolutionInfo())

    print("Advanced usage:")
    print("Problem solved in %f seconds\n" % solver.WallTime())

    return running_vals, start_vals, status, n_assets, n_days, solver.WallTime(), assets, days
