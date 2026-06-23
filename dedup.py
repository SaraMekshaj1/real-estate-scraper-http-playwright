import pandas as pd


df = pd.read_csv("output/results.csv", encoding="utf-8")

df.duplicated(subset=["property_id"]).sum()
print("Before:", len(df))
df = df.drop_duplicates(subset=["property_id"])
print("After:", len(df))

df.to_csv(
    "output/results2.csv",
    index=False,
    encoding="utf-8-sig"
)