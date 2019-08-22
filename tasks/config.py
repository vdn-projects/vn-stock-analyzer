import os

db_username = "admin"
db_psw = "B@chan1987"
use_virtual_screen = False
# conn_string = f"host=127.0.0.1 dbname=vietnam_stock user={db_username} password={db_psw}"
conn_string = f"host=127.0.0.1 dbname=vietnam_stock user={db_username} password={db_psw}"
download_path = "./data/download"  # os.getcwd()
initial_load_path = "./data/initial_load"
