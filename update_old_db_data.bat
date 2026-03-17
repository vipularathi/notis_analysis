@echo off
echo Updating old db data
start http://localhost:8700
py update_old_db_data_app.py