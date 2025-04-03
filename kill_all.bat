:: Kill all python.exe processes
for /f "tokens=2 delims=," %%i in ('tasklist /fi "imagename eq python.exe" /fo csv /nh') do (taskkill /pid %%i /t /f)

:: Kill all node.exe processes
for /f "tokens=2 delims=," %%i in ('tasklist /fi "imagename eq node.exe" /fo csv /nh') do (taskkill /pid %%i /t /f)

:: Kill all cmd.exe processes
for /f "tokens=2 delims=," %%i in ('tasklist /fi "imagename eq cmd.exe" /fo csv /nh') do (taskkill /pid %%i /t /f)
