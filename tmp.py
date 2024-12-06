import os


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
    in markdown format, excluding unwanted files
    """
    for root, dirs, files in os.walk(directory):
        # Sort files for consistent output
        files.sort()

        for file in files:
            # Get the full file path
            file_path = os.path.join(root, file)

            if should_include_file(file_path):
                # Print the filename as a markdown header
                print(f"\n# {file}\n")

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