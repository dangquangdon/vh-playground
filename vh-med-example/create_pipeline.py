import json


def create_patient_pipeline():
    # Define the patient IDs
    patient_ids = [f"patient{str(i).zfill(3)}" for i in range(1, 11)]

    # Create a pipeline node for each patient
    patient_nodes = []
    for patient_id in patient_ids:
        node = {
            "step": "process-single-patient",
            "name": f"Process-{patient_id}",
            "parameters": {"patient_id": patient_id},
        }
        patient_nodes.append(node)

    return patient_nodes


if __name__ == "__main__":
    nodes = create_patient_pipeline()
    print(json.dumps(nodes, indent=2))
