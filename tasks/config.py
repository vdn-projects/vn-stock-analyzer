import os

# db_username = "admin"
# db_psw = "B@chan1987"
db_username = "vietnamstock_usr1"
db_psw = "stock@2019"

use_virtual_screen = False
# conn_string = f"host=127.0.0.1 dbname=vietnam_stock user={db_username} password={db_psw}"
# conn_string = f"host=localhost dbname=vietnam_stock user={db_username} password={db_psw}"
conn_string = f"host=167.99.68.250 dbname=vietnam_stock user={db_username} password={db_psw}"
# download_path = "/usr/local/airflow/dags"  # os.getcwd()
download_path = os.path.join(
    os.getcwd(), "dags/vn_stock/tasks/data/download")  # os.getcwd()
initial_load_path = "./data/initial_load"
