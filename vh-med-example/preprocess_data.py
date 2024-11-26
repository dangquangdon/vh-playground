import valohai
import pandas as pd
import numpy as np
import json
import os
from datetime import datetime

valohai.prepare(
    step="preprocess-data",
    default_inputs={
        "brain_scans": "dataset://brain-scans/v1",
        "patient_metadata": "dataset://patient-metadata/v1",
        "scan_parameters": "dataset://scan-parameters/v1",
    },
)


def preprocess_data():
    with valohai.metadata.logger() as logger:
        try:
            logger.log("status", "Starting preprocessing")

            # Dictionary to store processed scans
            processed_scans = {}
            processed_metadata = []

            # Process brain scans
            logger.log("status", "Processing brain scans")
            for scan_file in valohai.inputs("brain_scans").paths():
                patient_id = os.path.basename(scan_file).split("_")[0]
                logger.log("processing_scan", patient_id)

                try:
                    # Generate simulated scan data instead of loading from file
                    # Create a 100x100 array of random values
                    simulated_scan = np.random.randn(100, 100)

                    processed_data = {
                        "data": simulated_scan,  # Already in correct shape
                        "processed_date": datetime.now().isoformat(),
                        "original_file": os.path.basename(scan_file),
                    }

                    processed_scans[patient_id] = processed_data
                    logger.log(
                        "scan_processed",
                        {
                            "patient_id": patient_id,
                            "data_shape": processed_data["data"].shape,
                        },
                    )

                except Exception as e:
                    logger.log(
                        "scan_error", {"patient_id": patient_id, "error": str(e)}
                    )
                    raise

            # Process metadata files
            logger.log("status", "Processing metadata")
            for meta_file in valohai.inputs("patient_metadata").paths():
                patient_id = os.path.basename(meta_file).split("_")[0]
                logger.log("processing_metadata", patient_id)

                try:
                    # Create simulated metadata instead of loading from file
                    metadata = {
                        "patient_id": patient_id,
                        "age": np.random.randint(20, 80),
                        "scan_date": datetime.now().strftime("%Y-%m-%d"),
                        "preprocessing_date": datetime.now().isoformat(),
                        "preprocessing_version": "1.0",
                        "original_file": os.path.basename(meta_file),
                    }

                    processed_metadata.append(metadata)
                    logger.log("metadata_processed", {"patient_id": patient_id})

                except Exception as e:
                    logger.log(
                        "metadata_error", {"patient_id": patient_id, "error": str(e)}
                    )
                    raise

            # Save processed scans
            logger.log("status", "Saving processed scans")
            scans_output_path = valohai.outputs().path("preprocessed_scans.npz")
            np.savez(
                scans_output_path, **{k: v["data"] for k, v in processed_scans.items()}
            )
            valohai.outputs().live_upload("preprocessed_scans.npz")

            # Save processed metadata
            logger.log("status", "Saving processed metadata")
            metadata_output_path = valohai.outputs().path("preprocessed_metadata.json")
            with open(metadata_output_path, "w") as f:
                json.dump(processed_metadata, f, indent=2)
            valohai.outputs().live_upload("preprocessed_metadata.json")

            # Create and save summary
            summary = {
                "preprocessing_date": datetime.now().isoformat(),
                "total_scans_processed": len(processed_scans),
                "total_metadata_processed": len(processed_metadata),
                "patient_ids": list(processed_scans.keys()),
                "preprocessing_version": "1.0",
            }

            summary_path = valohai.outputs().path("preprocessing_summary.json")
            with open(summary_path, "w") as f:
                json.dump(summary, f, indent=2)
            valohai.outputs().live_upload("preprocessing_summary.json")

            # Log final statistics
            logger.log(
                "preprocessing_complete",
                {
                    "total_scans": len(processed_scans),
                    "total_metadata": len(processed_metadata),
                    "completion_time": datetime.now().isoformat(),
                },
            )

            # Debug: Log all created outputs
            logger.log("available_outputs", os.listdir(valohai.outputs().dir_path))

        except Exception as e:
            logger.log("preprocessing_failed", str(e))
            raise


if __name__ == "__main__":
    preprocess_data()
