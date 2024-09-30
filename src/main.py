import pandas as pd
import load_data as ld

if __name__ == "__main__":

    data = pd.read_csv("Insumos/application_data.csv")
    dataprev = pd.read_csv("Insumos/previous_application.csv")
    dataprev = dataprev.rename(columns={"SK_ID_PREV ": "SK_ID_PREV"})

    conn = ld.connect_to_postgres()
    cur = conn.cursor()

    # ld.add_to_postgresql(cur, data)
    # ld.add_prev_to_postgresql(cur, dataprev)

    conn.commit()
    cur.close()
    conn.close()
