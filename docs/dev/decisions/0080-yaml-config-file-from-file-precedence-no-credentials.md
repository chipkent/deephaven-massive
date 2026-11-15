# 80. Settings can come from a YAML file

Date: 2026-10-31

Status: accepted

Settings can come from a YAML file through `MassiveConfig.from_file`. Code overrides the file, which
overrides the defaults; credentials are refused in the file; every value goes through one explicit
converter.
