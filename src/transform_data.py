import pandas as pd


dataprev = pd.read_csv("Insumos/previous_application.csv")
dataprev = dataprev.rename(columns={"SK_ID_PREV ": "SK_ID_PREV"})
print(dataprev.shape)
