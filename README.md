# Huawei strorage exporter

Metrics will be printed at STDOUT.

## Use with exporter_exporter
```
modules:
  huawei_storage:
    method: exec
    timeout: 10s
    exec:
      command: oceanstore_exporter.py
      args: [-c, config.ini, -t, <target>]
```
