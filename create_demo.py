
import os
import time

def create_dummy_files():
    os.makedirs("demo_source", exist_ok=True)
    os.makedirs("demo_dest", exist_ok=True)
    
    files = [
        ("note.txt", "This is a test note."),
        ("image.jpg", b"\xFF\xD8\xFF\xE0\x00\x10JFIF" + b"\x00" * 20), # Fake JPG header
        ("data.csv", "id,name\n1,Test"),
        ("project/2024/JAN/12345/report.pdf", b"%PDF-1.4\n..."), # Fake PDF
    ]

    for path, content in files:
        full_path = os.path.join("demo_source", path)
        os.makedirs(os.path.dirname(full_path), exist_ok=True)
        mode = "wb" if isinstance(content, bytes) else "w"
        with open(full_path, mode) as f:
            f.write(content)
        print(f"Created {full_path}")

if __name__ == "__main__":
    create_dummy_files()
