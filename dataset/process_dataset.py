import valohai

if __name__ == "__main__":
    inputs = valohai.inputs("dataset").paths()
    print("INPUT")
    [print(input) for input in inputs]
    print("OK")