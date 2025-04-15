import pandas as pd

#  bvn, chn, account no (uid)
registrars_data = pd.read_csv("tshold.csv")

# obtain duplicate bvn
registrars_bvn_duplicate = registrars_data[registrars_data.duplicated(subset="bvn", keep=False) == True ]

# group duplicates by bvn & customer id
analysis = registrars_bvn_duplicate.groupby(["bvn","customerid"]).size()


# print(analysis)
analysis.to_csv("Entity_Duplicate_Customer_Analysis.csv")