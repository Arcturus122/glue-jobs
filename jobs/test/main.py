import yfinance as yf

tickers = {
    "Brent": "BZ=F",
    "WTI": "CL=F",
    "Gold": "GC=F",
    "Silver": "SI=F",
    "Copper": "HG=F",
    "Henry Hub": "NG=F",
    "TTF": "TTF=F",
    "Corn": "ZC=F",
    "Soybeans": "ZS=F",
    "Wheat": "ZW=F",
    "Cotton": "CT=F",
    "Sugar": "SB=F",
    "Coffee": "KC=F",
    "Cocoa": "CC=F",
    "Orange Juice": "OJ=F",
    "Lumber": "LBS=F",
    "Live Cattle": "LE=F",
    "Feeder Cattle": "GF=F",
    "Lean Hogs": "HE=F",
    "Zinc": "ZN=F",
    "Aluminum": "ALI=F",
}

if __name__ == "__main__":
    data = yf.download(list(tickers.values()), period="max", interval="1d")
    data = (
        data.unstack(level=0)
        .to_frame()
        .reset_index()
        .rename(
            columns={
                "level_0": "metric",
                0: "value",
                "level_1": "ticker",
                "Date": "date",
            }
        )
        .set_index(["date", "ticker", "metric"])
        .sort_index()
    )
    data.dropna(inplace=True)
