import valohai
import shutil
from valohai.paths import get_outputs_path

if __name__ == "__main__":
    input_faces = valohai.inputs("faces").paths()
    output_dir = get_outputs_path()
    for file in input_faces:
        print(f"Copying {file} to {output_dir}")
        shutil.copy(file, output_dir)
    