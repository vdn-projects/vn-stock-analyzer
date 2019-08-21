import os

db_username = "vietnamstock_usr1"
db_psw = "stock@2019"
use_virtual_screen = False
# conn_string = f"host=127.0.0.1 dbname=vietnam_stock user={db_username} password={db_psw}"
conn_string = f"host=67.99.68.250 dbname=vietnam_stock user={db_username} password={db_psw}"
download_path = "./data/download"  # os.getcwd()
initial_load_path = "./data/initial_load"
