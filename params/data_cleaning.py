import valohai


def main():
    null_look_up_cols = valohai.parameters("null_lookup_columns").value
    duplicate_lookup_cols = valohai.parameters("duplicate_lookup_columns").value
    print(null_look_up_cols, duplicate_lookup_cols)

if __name__ == "__main__":
    main()
