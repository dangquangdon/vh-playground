import valohai

input_file = valohai.inputs('large_file').paths()
print("INPUT: ")
print(input_file)
with open(input_file, 'r') as f:
    print("reading...")
    print(f.readlines())