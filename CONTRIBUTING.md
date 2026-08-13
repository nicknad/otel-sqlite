# Contributing

1. Fork the repository and create a focused branch.
2. Make the smallest change that solves the problem.
3. Run the relevant checks:

```bash
make fmt
make test
make lint
```

If protobuf sources change, run `make generate` and commit the generated code.
Include tests for behavior changes and update the relevant documentation.

Open a pull request at <https://codeberg.org/nicknad/otel-sqlite> with a short
summary, test results, and any operational or migration impact.
