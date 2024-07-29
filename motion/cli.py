import typer
from typing import Optional
from pathlib import Path

from motion.runner import DSLRunner

app = typer.Typer()


@app.command()
def run(
    yaml_file: Path = typer.Argument(
        ..., help="Path to the YAML file containing the pipeline configuration"
    ),
    max_threads: Optional[int] = typer.Option(
        None, help="Maximum number of threads to use for parallel operations"
    ),
):
    """
    Run the configuration specified in the YAML file.
    """
    try:
        runner = DSLRunner(str(yaml_file), max_threads=max_threads)
        runner.run()
    except Exception as e:
        typer.echo(f"An error occurred: {str(e)}", err=True)
        raise typer.Exit(code=1)


if __name__ == "__main__":
    app()