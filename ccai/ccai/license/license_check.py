import datetime

def check_license():
    now = datetime.datetime.now()
    if now.year > 2023:
        return False, "License expired. Please contact MindGraph Technologies PTE LTD."
    return True, "License valid."