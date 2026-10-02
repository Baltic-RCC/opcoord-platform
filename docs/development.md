# Development

## Setup

The project uses [uv](https://docs.astral.sh/uv/) and Python 3.11+.

```bash
uv sync
cp config/.env.example config/.env   # then fill in the connection settings
```

## Running a worker locally

The workers import their own modules as top-level names (`import settings`,
`import builders`), so run them with both the repository root and the worker
folder on the path. From the repository root:

=== "bash"

    ```bash
    PYTHONPATH=.:card_publicator uv run python card_publicator/worker.py
    ```

=== "PowerShell"

    ```powershell
    $env:PYTHONPATH = ".;card_publicator"; uv run python card_publicator/worker.py
    ```

Replace `card_publicator` with `card_retriever` or `business_data_exchange`
for the other workers. The Docker images set `PYTHONPATH=/app` and use
`/app` (the worker folder's parent) as the working directory.

To build a card from a local XML file without RabbitMQ, use the
[card publicator test harness](workers/card-publicator.md#local-test-harness).

## Docker images

`Dockerfile` at the root builds the shared base image with the Python
dependencies. Each worker folder has its own `Dockerfile` on top of it. The
GitHub Actions workflows build and push both:

| Workflow | Builds |
|---|---|
| `ci-base-image.yml` | `opcoord-base` |
| `ci-dev.yml` | The three worker images, tagged `dev-<yyyymmddHHMM>` and `latest` on pushes to `main`, and with the version on `v*` tags |

## Documentation

This site is built with [MkDocs](https://www.mkdocs.org/) and the
[Material](https://squidfunk.github.io/mkdocs-material/) theme. Its tools are
in the `docs` dependency group, so they are not installed in the Docker images.

```bash
uv run --group docs mkdocs serve          # live preview on http://127.0.0.1:8000
uv run --group docs mkdocs build --strict # what CI runs
```

- Pages are Markdown files in `docs/`. Add new pages to `nav` in `mkdocs.yml`.
- The API reference pages are generated from docstrings by
  [mkdocstrings](https://mkdocstrings.github.io/). A `::: module.name` line in
  a page renders that module. Docstrings use the Google style.
- Do not put internal hostnames, credentials or secret paths in the docs;
  this repository and its site are public.
