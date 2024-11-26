import valohai
import pandas as pd
import numpy as np
import json
import os
from datetime import datetime

input_files = {
    "brain_scans": "dataset://brain-scans/v1",
    "patient_metadata": "dataset://patient-metadata/v1",
    "scan_parameters": "dataset://scan-parameters/v1",
}

valohai.prepare(step="medical-image-analysis", default_inputs=input_files)


def process_images():
    execution_id = os.getenv("VH_EXECUTION_ID", "dev")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    dataset_version = f"processed_batch_{timestamp}"

    with valohai.metadata.logger() as logger:
        execution_metadata = {
            "processed_files": 0,
            "processing_date": datetime.now().isoformat(),
            "file_summary": [],
            "total_confidence_score": 0,
        }

        # Create a dictionary to store all outputs across patients
        all_outputs = {}

        for scan_file in valohai.inputs("brain_scans").paths():
            logger.log("processing_file", os.path.basename(scan_file))
            patient_id = os.path.basename(scan_file).split("_")[0]
            logger.log("patient_id", patient_id)

            file_metadata = {
                "patient_id": patient_id,
                "original_file": os.path.basename(scan_file),
                "processing_timestamp": datetime.now().isoformat(),
                "outputs": [],
            }

            # Create patient-specific output filenames
            patient_outputs = {
                f"{patient_id}_volumetric_analysis_detailed.csv": pd.DataFrame(
                    np.random.randn(100, 5)
                ),
                f"{patient_id}_segmentation_results_v2.nii.gz": np.random.bytes(1000),
                f"{patient_id}_tumor_detection_report.json": {"confidence": 0.95},
                f"{patient_id}_brain_region_measurements.xlsx": pd.DataFrame(
                    np.random.randn(50, 8)
                ),
                f"{patient_id}_quality_metrics.txt": "QA Score: 0.98",
                f"{patient_id}_processed_scan.dcm": np.random.bytes(2000),
            }

            # Add patient outputs to the complete output dictionary
            all_outputs.update(patient_outputs)

            confidence_score = 0.95
            execution_metadata["total_confidence_score"] += confidence_score
            file_metadata["confidence_score"] = confidence_score
            logger.log("confidence_score", confidence_score)

            # Save patient-specific metadata
            execution_metadata["file_summary"].append(file_metadata)
            execution_metadata["processed_files"] += 1
            logger.log("files_processed", execution_metadata["processed_files"])

        # Now save all outputs after processing all patients
        for filename, content in all_outputs.items():
            output_path = valohai.outputs().path(filename)

            try:
                # Save the main file
                if isinstance(content, pd.DataFrame):
                    content.to_csv(output_path)
                elif isinstance(content, dict):
                    with open(output_path, "w") as f:
                        json.dump(content, f)
                elif isinstance(content, str):
                    with open(output_path, "w") as f:
                        f.write(content)
                elif isinstance(content, bytes):
                    with open(output_path, "wb") as f:
                        f.write(content)

                # Create metadata sidecar file for dataset creation
                metadata = {
                    "valohai.dataset-versions": [
                        {
                            "uri": f"dataset://processed-medical-images/{dataset_version}",
                            "metadata": {
                                "processing_date": datetime.now().isoformat(),
                                "source_execution": execution_id,
                                "patient_id": filename.split("_")[
                                    0
                                ],  # Extract patient ID from filename
                            },
                        }
                    ]
                }

                # Save metadata sidecar file
                metadata_path = valohai.outputs().path(f"{filename}.metadata.json")
                with open(metadata_path, "w") as f:
                    json.dump(metadata, f)

                # Live upload both files
                valohai.outputs().live_upload(filename)
                valohai.outputs().live_upload(f"{filename}.metadata.json")

                logger.log("output_file", filename)
                logger.log("output_size", os.path.getsize(output_path))

            except Exception as e:
                error_msg = f"Error saving {filename}: {str(e)}"
                print(error_msg)
                logger.log("error", error_msg)
                raise e

        # Calculate final metrics
        if execution_metadata["processed_files"] > 0:
            avg_confidence = (
                execution_metadata["total_confidence_score"]
                / execution_metadata["processed_files"]
            )
            execution_metadata["average_confidence"] = avg_confidence
            logger.log("average_confidence", avg_confidence)

        # Save execution metadata
        metadata_path = valohai.outputs().path("execution_metadata.json")
        with open(metadata_path, "w") as f:
            json.dump(execution_metadata, f, indent=2)
        valohai.outputs().live_upload("execution_metadata.json")

        # Add execution metadata to the dataset
        exec_metadata = {
            "valohai.dataset-versions": [
                {
                    "uri": f"dataset://processed-medical-images/{dataset_version}",
                    "metadata": {
                        "processing_date": datetime.now().isoformat(),
                        "source_execution": execution_id,
                        "total_files_processed": execution_metadata["processed_files"],
                        "average_confidence": execution_metadata.get(
                            "average_confidence", 0
                        ),
                    },
                }
            ]
        }

        exec_metadata_path = valohai.outputs().path(
            "execution_metadata.json.metadata.json"
        )
        with open(exec_metadata_path, "w") as f:
            json.dump(exec_metadata, f)
        valohai.outputs().live_upload("execution_metadata.json.metadata.json")


if __name__ == "__main__":
    print("Starting medical image processing...")
    try:
        process_images()
        print("Processing completed successfully")
    except Exception as e:
        print(f"Error during processing: {str(e)}")
        raise e
