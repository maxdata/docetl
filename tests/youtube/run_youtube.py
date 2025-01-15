import json
from docetl.runner import DSLRunner


def run_youtube_pipeline(config_file, tmp_path):
    
    # Create and run the DSLRunner
    runner = DSLRunner.from_yaml(str(config_file))
    total_cost = runner.load_run_save()

    # Check if the output file was created
    output_path = tmp_path / "output.json"
    assert output_path.exists(), "Output file was not created"

    # Load and check the output
    with open(output_path, "r") as f:
        output_data = json.load(f)

    # Check if the cost was calculated and is greater than 0
    assert total_cost > 0, "Total cost was not calculated or is 0"

    print(f"Pipeline executed successfully. Total cost: ${total_cost:.2f}")
    print(f"Output: {output_data}")


if __name__ == "__main__":
    config_file = "tests/youtube/youtube_extraction_pipeline.yaml"
    tmp_path = "tests/youtube/tmp"
    run_youtube_pipeline(config_file, tmp_path)
