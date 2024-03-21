import pandas as pd

def read_df(file):
    file = request.files.get("file[]")
    filename = file.filename
    file_extension = filename.partition(".")[-1]
    if file_extension == "csv":
        data = pd.read_csv(file)
    elif file_extension in ["xls", "xlsx"]:
        data = pd.read_excel(file)