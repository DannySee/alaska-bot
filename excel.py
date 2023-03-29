import os
import pandas as pd


def create_excel(vendors):
    filename = 'Alaska VADAM Ties.xlsx'
    writer = pd.ExcelWriter(filename)
    df = pd.DataFrame(vendors, columns=['vendors'])
    df.to_excel(writer, sheet_name='VADAM Requests', index=False)

    writer.save()
    return os.path.join(os.getcwd() , filename)
