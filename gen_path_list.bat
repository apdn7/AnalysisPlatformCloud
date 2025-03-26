@echo off
set log_file=_original_path_list.log
powershell " Get-ChildItem -Recurse . -file | Resolve-Path -Relative | Where-Object { $_ -notmatch '__pycache__|.data$|.log$|cache|webassets-cache|.idea|gen_path_list*|.env$' } | Out-File -Encoding ASCII %log_file%"
