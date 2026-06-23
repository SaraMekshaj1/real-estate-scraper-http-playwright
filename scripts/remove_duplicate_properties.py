import pandas as pd

df = pd.read_csv("output/results.csv", encoding="utf-8")

dup_sum=df.duplicated(subset=["property_id"]).sum()

print("duplicates nr",dup_sum)

print("Before:", len(df))
df = df.drop_duplicates(subset=["property_id"])
print("After:", len(df))

df.to_csv(
   "output/results_deduplicated.csv",
   index=False,
   encoding="utf-8-sig"
)