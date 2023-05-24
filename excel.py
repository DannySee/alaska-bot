import os
import pandas as pd


def create_excel(vendors):
    filename = 'C:\\Temp\\outgoing_mail\\Alaska VADAM Ties.xlsx'
    writer = pd.ExcelWriter(filename)
    df = pd.DataFrame(vendors, columns=['vendors'])
    df.to_excel(writer, sheet_name='VADAM Requests', index=False)

    writer.save()
    return filename
