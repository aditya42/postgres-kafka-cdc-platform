from __future__ import annotations

import requests

BASE = "http://localhost:8081"


def main() -> None:
    subjects = requests.get(f"{BASE}/subjects", timeout=10).json()
    if not subjects:
        print("No schemas registered yet. Generate CDC events first.")
        return
    for subject in sorted(subjects):
        versions = requests.get(f"{BASE}/subjects/{subject}/versions", timeout=10).json()
        mode = requests.get(f"{BASE}/config/{subject}", timeout=10)
        compatibility = mode.json().get("compatibilityLevel") if mode.ok else "global"
        print(f"{subject:50s} versions={versions} compatibility={compatibility}")


if __name__ == "__main__":
    main()
