import schedule
import time
from script import run_stock_job

from datetime import datetime

def basic_job():
    print("Job started at:", datetime.now())


# Run every minute
schedule.every().day.at("10:30").do(basic_job)
schedule.every().day.at("10:30").do(run_stock_job)


while True:
    schedule.run_pending()
    time.sleep(1)