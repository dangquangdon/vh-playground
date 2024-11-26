import valohai
import pandas as pd
import numpy as np
import json
import os
from datetime import datetime

valohai.prepare(
    step="process-single-patient",
    default_parameters={"patient_id": "patient001"},
    default_inputs={
        "preprocessed_scans": None,  # Will be provided by pipeline
        "preprocessed_metadata": None,  # Will be provided by pipeline
        "scan_parameters": "dataset://scan-parameters/v1",
    },
)


def process_single_patient():
    patient_id = valohai.parameters("patient_id").value

    with valohai.metadata.logger() as logger:
        logger.log("processing_patient", patient_id)

        metadata_file = valohai.inputs("preprocessed_metadata").path()
        with open(metadata_file, "r") as f:
            metadata = json.load(f)

        # Get this patient's data
        patient_metadata = next(
            (m for m in metadata if m["patient_id"] == patient_id), None
        )

        if patient_metadata is None:
            raise ValueError(f"No metadata found for patient {patient_id}")

        # Generate outputs for this patient
        outputs = {
            f"{patient_id}_volumetric_analysis.csv": pd.DataFrame(
                np.random.randn(100, 5)
            ),
            f"{patient_id}_segmentation.nii.gz": np.random.bytes(1000),
            f"{patient_id}_report.json": {
                "confidence": 0.95,
                "processing_date": datetime.now().isoformat(),
                "patient_metadata": patient_metadata,
            },
        }

        # Save all outputs
        for filename, content in outputs.items():
            output_path = valohai.outputs().path(filename)

            try:
                if isinstance(content, pd.DataFrame):
                    content.to_csv(output_path)
                elif isinstance(content, dict):
                    with open(output_path, "w") as f:
                        json.dump(content, f, indent=2)
                elif isinstance(content, bytes):
                    with open(output_path, "wb") as f:
                        f.write(content)
                elif isinstance(content, str):
                    with open(output_path, "w") as f:
                        f.write(content)

                valohai.outputs().live_upload(filename)
                logger.log("output_file", filename)

            except Exception as e:
                logger.log("error", str(e))
                raise e


if __name__ == "__main__":
    process_single_patient()
