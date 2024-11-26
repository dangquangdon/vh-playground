import os
import numpy as np
import json
import yaml
from datetime import datetime


def create_directory_structure():
    dirs = ["data/brain_mri_scans", "data/patient_records", "data/scanning_protocols"]
    for dir_path in dirs:
        os.makedirs(dir_path, exist_ok=True)
    return dirs[0], dirs[1], dirs[2]


def generate_dummy_dicom():
    return np.random.bytes(1000)


def generate_patient_metadata(patient_id):
    return {
        "patient_id": patient_id,
        "scan_date": datetime.now().strftime("%Y-%m-%d"),
        "age": np.random.randint(20, 80),
        "gender": np.random.choice(["M", "F"]),
        "medical_history": {
            "previous_conditions": ["condition_" + str(i) for i in range(2)],
            "medications": ["med_" + str(i) for i in range(3)],
        },
    }


def generate_scan_protocol(version):
    return {
        "protocol_version": version,
        "machine_settings": {
            "magnetic_field_strength": 3.0,
            "slice_thickness": 1.5,
            "repetition_time": 2000,
            "echo_time": 30,
        },
        "sequence_parameters": {
            "orientation": "axial",
            "field_of_view": 240,
            "matrix_size": "256x256",
        },
    }


def main():
    scans_dir, records_dir, protocols_dir = create_directory_structure()

    print("Generating sample files...")

    for i in range(1, 11):
        patient_id = f"patient{i:03d}"

        scan_path = os.path.join(scans_dir, f"{patient_id}_t1_scan.dcm")
        with open(scan_path, "wb") as f:
            f.write(generate_dummy_dicom())
        print(f"Created: {scan_path}")

        metadata_path = os.path.join(records_dir, f"{patient_id}_metadata.json")
        with open(metadata_path, "w") as f:
            json.dump(generate_patient_metadata(patient_id), f, indent=2)
        print(f"Created: {metadata_path}")

    for version in ["v1", "v2"]:
        protocol_path = os.path.join(protocols_dir, f"mri_protocol_{version}.yaml")
        with open(protocol_path, "w") as f:
            yaml.dump(generate_scan_protocol(version), f)
        print(f"Created: {protocol_path}")

    print("\nAll files generated successfully!")


if __name__ == "__main__":
    main()
