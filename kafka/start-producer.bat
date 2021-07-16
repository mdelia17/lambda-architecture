@ECHO OFF

setlocal enabledelayedexpansion
FOR /L %%A IN (0,1,99) DO (
    for /f "delims=" %%x in ('powershell get-date -format "{dd-MM-yyyy_HH-mm-ss}"') do @set date=%%x
    docker cp dataset/data%%A.csv kafka-connect-prod://tmp/kafka-connect-prod/network-data/!date!.csv
    ECHO copied data%%A.csv to !date!.csv
    timeout /t 10 /nobreak
)
