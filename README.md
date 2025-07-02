# notis_analysis

BOD Check 1=> The app is scheduled to start at 0830hrs on every working day. If it doesn’t start automatically, start it from either of the following:-
1.	Schedular – Run Notis App Start Frontend & Backend
2.	Windows Explorer –
a.	Run "D:\notis_analysis\notis_backend.bat"
b.	Run "D:\notis_analysis\notis_app.bat"

BOD Check 2=> Ensure that yesterday’s volatility file is present in the folder - "D:\notis_analysis\nse_fo_voltality_file". If yesterday’s volatility file is not present, manually download it from NSE website. Follow the given guideline:-
1.	Go to https://www.nseindia.com/all-reports-derivatives
2.	Scroll down to find previous day’s reports and download the Daily Volatility file.
3.	Cut and paste the downloaded volatility file in "D:\notis_analysis\nse_fo_voltality_file"

**Note: If the volatility file is missing then exposure would not be displayed.**

=====================================================================
EOD Check 1=> The final EOD file is made at 1635hrs, it is added to the schedular. In case it doesn’t start automatically, start it by either of the following ways:-
1.	Schedular - Run notis
2.	Windows Explorer – 
a.	Run "D:\notis_analysis\main.bat"
b.	Run "D:\notis_analysis\get_all_bse_trade.bat"

EOD Check 2=> The system is scheduled to restart automatically at 1700hrs everyday. Make sure to log into the system after restart. The code to download the volatility file is scheduled to run at 2000hrs.

**Note: If the system is not logged in, the code to download the volatility file would not work.**
