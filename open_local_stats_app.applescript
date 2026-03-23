set appPath to POSIX path of (path to me)
set launcherPath to quoted form of (appPath & "Contents/Resources/open_local_stats_app.command")
do shell script "chmod +x " & launcherPath & " && " & launcherPath & " >/tmp/xhs_local_stats_app.launch.log 2>&1 &"
