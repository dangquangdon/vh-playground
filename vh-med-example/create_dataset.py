import valohai
import json
import os
import shutil
from datetime import datetime

valohai.prepare(
    step="create-dataset",
    default_inputs={
        "patient_results": None  # Will be provided by pipeline
    },
)


def create_dataset():
    execution_id = os.getenv("VH_EXECUTION_ID", "dev")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    dataset_version = f"processed_batch_{timestamp}"

    with valohai.metadata.logger() as logger:
        logger.log("status", "Starting dataset creation")
        processed_files = 0

        # Process all result files from patient processing
        for result_file in valohai.inputs("patient_results").paths():
            filename = os.path.basename(result_file)
            logger.log("processing_file", filename)

            # Copy the file to outputs
            output_path = valohai.outputs().path(filename)
            shutil.copy2(result_file, output_path)

            # Create metadata sidecar file for dataset creation
            metadata = {
                "valohai.dataset-versions": [
                    {
                        "uri": f"dataset://processed-medical-images/{dataset_version}",
                        "metadata": {
                            "processing_date": datetime.now().isoformat(),
                            "source_execution": execution_id,
                            "original_filename": filename,
                            "patient_id": filename.split("_")[0]
                            if "_" in filename
                            else "unknown",
                        },
                    }
                ]
            }

            # Save metadata sidecar file
            metadata_path = valohai.outputs().path(f"{filename}.metadata.json")
            with open(metadata_path, "w") as f:
                json.dump(metadata, f, indent=2)

            # Live upload both files
            valohai.outputs().live_upload(filename)
            valohai.outputs().live_upload(f"{filename}.metadata.json")

            processed_files += 1
            logger.log("files_processed", processed_files)

        # Create summary metadata
        summary_metadata = {
            "dataset_version": dataset_version,
            "creation_date": datetime.now().isoformat(),
            "source_execution": execution_id,
            "total_files_processed": processed_files,
        }

        # Save summary metadata
        summary_path = valohai.outputs().path("dataset_summary.json")
        with open(summary_path, "w") as f:
            json.dump(summary_metadata, f, indent=2)
        valohai.outputs().live_upload("dataset_summary.json")

        # Create metadata sidecar for summary
        summary_metadata_content = {
            "valohai.dataset-versions": [
                {
                    "uri": f"dataset://processed-medical-images/{dataset_version}",
                    "metadata": summary_metadata,
                }
            ]
        }

        summary_metadata_path = valohai.outputs().path(
            "dataset_summary.json.metadata.json"
        )
        with open(summary_metadata_path, "w") as f:
            json.dump(summary_metadata_content, f, indent=2)
        valohai.outputs().live_upload("dataset_summary.json.metadata.json")


if __name__ == "__main__":
    create_dataset()
