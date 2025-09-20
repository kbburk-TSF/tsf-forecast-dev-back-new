import pandas as pd

def _ensure_daily_index(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date")
    idx = pd.date_range(df["date"].min(), df["date"].max(), freq="D")
    df2 = df.set_index("date").reindex(idx)
    df2.index.name = "date"
    df2["value"] = df2["value"].astype(float)
    return df2.reset_index().rename(columns={"index": "date"})

def forecast_seasonal_naive_dow(history: pd.DataFrame, h: int, lookback_weeks: int = 8) -> pd.DataFrame:
    df = _ensure_daily_index(history[["date","value"]])
    df["dow"] = df["date"].dt.dayofweek
    res = []
    last_date = df["date"].max()
    for i in range(1, h+1):
        target_date = last_date + pd.Timedelta(days=i)
        dow = target_date.dayofweek
        recent = df[(df["dow"] == dow) & (df["date"] <= last_date)].tail(lookback_weeks)
        val = float(recent["value"].mean()) if not recent.empty else float(df["value"].mean())
        res.append({"date": target_date.date(), "value": val})
    return pd.DataFrame(res)

def forecast_ewma(history: pd.DataFrame, h: int, span: int = 14) -> pd.DataFrame:
    df = _ensure_daily_index(history[["date","value"]])
    smoothed = df["value"].ewm(span=span, adjust=False).mean().iloc[-1]
    future_dates = [df["date"].max() + pd.Timedelta(days=i) for i in range(1, h+1)]
    return pd.DataFrame({"date": [d.date() for d in future_dates], "value": [float(smoothed)]*h})
