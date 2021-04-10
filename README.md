# sqltail
Command line tool to tail a SQL query

```
Usage: sqltail [OPTIONS]

Options:
  --version                       Show the version and exit.
  --host TEXT
  --port TEXT
  --user TEXT
  --password TEXT
  --database TEXT
  --config-file TEXT
  --timeout FLOAT
  --interval INTEGER
  --timezone TEXT
  --template TEXT                 json field template, or @FILENAME in cwd or
                                  ~/.sqltail

  --get-template                  output json field template
  --get-columns
  --suffix TEXT                   append SUFFIX to db name (defaults to _log)
  -r, --retry / -R, --no-retry    retry on database connection failures
  -t, --table TEXT                table name
  -c, --columns TEXT              comma delimited list of output column names
  -f, --filters TEXT              list of filter conditions
  -o, --output-format TEXT        output format
  -l, --log-level [DEBUG|INFO|WARNING|ERROR|CRITICAL]
  --help                          Show this message and exit.
```
