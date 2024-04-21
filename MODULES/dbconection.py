
def get_conection():

    # Config db connection
    connect_info = (
        "Driver={ODBC Driver 18 for SQL Server};"
        "Server=tcp:budgefy.database.windows.net,1433;Database=budgefy;"
        "Uid=rique;"
        "Pwd={Vai2023!};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;Driver={ODBC Driver 18 for SQL Server};"
        "Server=tcp:budgefy.database.windows.net,1433;"
        "Database=budgefy;"
        "Uid=rique;Pwd={Vai2023!};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )

    return connect_info
