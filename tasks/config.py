import os

db_username = "admin"
db_psw = "B@chan1987"
use_virtual_screen = False
# conn_string = f"host=127.0.0.1 dbname=vietnam_stock user={db_username} password={db_psw}"
conn_string = f"host=vietnam_stock dbname=vietnam_stock user={db_username} password={db_psw}"
# download_path = "/usr/local/airflow/dags"  # os.getcwd()
download_path = os.path.join(
    os.getcwd(), "dags/vn_stock/tasks/data/download")  # os.getcwd()
initial_load_path = "./data/initial_load"


url_ticker = "https://www.vndirect.com.vn/portal/thong-tin-co-phieu/nhap-ma-chung-khoan.shtml?request_locale=en_GB"
