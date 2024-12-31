import os


def write_list_to_a_txt_file(filename, lst):
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename, 'w') as file:
        for item in lst:
            file.write(f"{item}\n")


def read_list_from_a_txt_file(filename):
    with open(filename, 'r') as file:
        return [line.strip() for line in file.readlines()]


def find_and_replace_line_in_file(filepath, target_line, replacement_line):
    # Read the file into a list of lines
    with open(filepath, 'r') as file:
        lines = file.readlines()

    # Replace the target line with the replacement line
    lines = [replacement_line + "\n" if line.strip() == target_line else line for line in lines]

    # Write the updated lines back to the file
    with open(filepath, 'w') as file:
        file.writelines(lines)


def get_all_files(folder_path, extension=".txt"):
    text_files = []
    for root, _, files in os.walk(folder_path):
        for file in files:
            if file.endswith(extension):
                text_files.append(os.path.join(root, file))
    return text_files


def chunk_the_list(input_list, chunk_size=5):
    return [input_list[i:i + chunk_size] for i in range(0, len(input_list), chunk_size)]
