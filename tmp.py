import os


def get_file_order_priority(filepath):
    """
    Return a priority number for ordering files.
    Lower numbers will be printed first.
    """
    if filepath.endswith('.py') and 'webhook.py' in filepath:
        return 7  # webhook.py comes after README
    elif filepath.endswith('.sql'):
        return 1  # SQL files first
    elif filepath.endswith('.c'):
        return 2  # C files second
    elif filepath.endswith('.control'):
        return 3  # control files third
    elif 'Makefile' in filepath:
        return 4  # Makefile fourth
    elif 'README' in filepath:
        return 5  # README fifth
    elif filepath.endswith('requirements.txt'):
        return 8  # requirements.txt last
    elif filepath.endswith('.py'):
        return 6  # other Python files
    return 9  # everything else


def should_include_file(filepath):
    """
    Determine if a file should be included in the output
    """
    # Exclude git-related files and directories
    if '.git' in filepath:
        return False

    # Exclude pycache directories
    if '__pycache__' in filepath:
        return False

    # Exclude temporary files
    if filepath.endswith('.pyc') or filepath.endswith('tmp.py'):
        return False

    # List of relevant file extensions we want to include
    valid_extensions = [
        '.py', '.sql', '.md', '.txt', '.c',
        'Dockerfile', 'Makefile', '.control'
    ]

    # Check if the file has a valid extension or is a known file we want
    return any(filepath.endswith(ext) for ext in valid_extensions)


def print_directory_contents(directory):
    """
    Walk through a directory and print all filenames and their contents
    in markdown format, excluding unwanted files and ordering by priority
    """
    all_files = []

    # First collect all valid files
    for root, dirs, files in os.walk(directory):
        for file in files:
            file_path = os.path.join(root, file)
            if should_include_file(file_path):
                all_files.append(file_path)

    # Sort files by priority and then alphabetically within each priority level
    all_files.sort(key=lambda x: (get_file_order_priority(x), x.lower()))

    # Print files in order
    for file_path in all_files:
        # Print the filename as a markdown header
        print(f"\n# {os.path.basename(file_path)}\n")

        try:
            # Try to read the file contents
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read().strip()
                # Print the contents in a markdown code block
                print("```")
                print(content)
                print("```\n")
        except Exception as e:
            print(f"Error reading file: {e}\n")


# Example usage
directory = "."  # Current directory
print_directory_contents(directory)