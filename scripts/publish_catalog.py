"""Pull/generate catalog & users data, then commit and push to GitHub for teammates."""

import subprocess
import sys
from pathlib import Path

# Paths
REPO_ROOT = Path(__file__).resolve().parents[1]
PYTHON = sys.executable


def run_cmd(cmd: list[str], desc: str) -> None:
    print(f"\n--> {desc}...")
    result = subprocess.run(cmd, cwd=REPO_ROOT, text=True, capture_output=True)
    if result.returncode != 0:
        print(f"[LỖI] {desc} thất bại:")
        print(result.stderr)
        sys.exit(result.returncode)
    if result.stdout.strip():
        print(result.stdout.strip())


def main() -> None:
    print("=" * 60)
    print(" BỘ CÔNG CỤ ĐỒNG BỘ DATA SẢN PHẨM & NGƯỜI DÙNG LÊN GITHUB ")
    print("=" * 60)

    # 1. Trích xuất / kiểm tra Catalog 1.000 sản phẩm
    run_cmd(
        [
            PYTHON,
            "-m",
            "recommender.ingestion.catalog",
            "--limit",
            "1000",
            "--output",
            "data/bronze/products/products.jsonl",
            "--seed",
            "42",
        ],
        "1. Trích xuất 1.000 sản phẩm thật Best Buy vào data/bronze/products/",
    )

    # 2. Sinh / kiểm tra 1.000 User Profiles
    run_cmd(
        [
            PYTHON,
            "-m",
            "recommender.ingestion.users",
            "--limit",
            "1000",
            "--output",
            "data/bronze/users/users.jsonl",
            "--seed",
            "42",
        ],
        "2. Khởi tạo 1.000 hồ sơ người dùng vào data/bronze/users/",
    )

    # 3. Git stage, commit & push
    run_cmd(
        [
            "git",
            "add",
            ".gitignore",
            "data/raw/products_bestbuy_1000.json",
            "data/raw/users_1000.json",
            "data/bronze/products/",
            "data/bronze/users/",
            "scripts/publish_catalog.py",
        ],
        "3. Đưa file dữ liệu vào Git staging",
    )

    # Commit nếu có thay đổi
    status = subprocess.run(
        ["git", "status", "--porcelain"],
        cwd=REPO_ROOT,
        text=True,
        capture_output=True,
    ).stdout.strip()
    if status:
        run_cmd(
            [
                "git",
                "commit",
                "-m",
                "data(catalog): publish 1000 Best Buy products and users for team",
            ],
            "4. Tạo Git commit",
        )
    else:
        print("\n--> Không có thay đổi mới cần commit.")

    # Push to origin
    run_cmd(["git", "push", "origin", "main"], "5. Đẩy (push) dữ liệu lên GitHub")

    print("\n" + "=" * 60)
    print(" HOÀN TẤT ĐẨY DỮ LIỆU LÊN GITHUB! ")
    print(" Teammate của bạn (Phan Văn Pháp) chỉ cần mở terminal và gõ:")
    print("     git pull origin main")
    print(" là sẽ có ngay lập tức toàn bộ 1.000 sản phẩm và 1.000 users!")
    print("=" * 60)


if __name__ == "__main__":
    main()
