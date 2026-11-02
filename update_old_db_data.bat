@echo off
echo Updating old db data
start http://localhost:8787
venv\Scripts\python.exe update_old_db_data_app.py