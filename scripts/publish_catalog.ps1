# Tự động trích xuất sản phẩm, người dùng và đẩy lên GitHub cho đồng đội
$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = Split-Path -Parent $scriptDir
Set-Location $repoRoot

$python = ".\.venv\Scripts\python.exe"

Write-Host "============================================================" -ForegroundColor Cyan
Write-Host " DONG BO DATA SAN PHAM & NGUOI DUNG LEN GITHUB " -ForegroundColor Cyan
Write-Host "============================================================" -ForegroundColor Cyan

Write-Host "`n--> 1. Trich xuat 1.000 san pham Best Buy..." -ForegroundColor Yellow
& $python -m recommender.ingestion.catalog --limit 1000 --output data/bronze/products/products.jsonl --seed 42

Write-Host "`n--> 2. Khoi tao 1.000 ho so nguoi dung..." -ForegroundColor Yellow
& $python -m recommender.ingestion.users --limit 1000 --output data/bronze/users/users.jsonl --seed 42

Write-Host "`n--> 3. Dua vao Git staging..." -ForegroundColor Yellow
git add .gitignore data/raw/products_bestbuy_1000.json data/raw/users_1000.json data/bronze/products/ data/bronze/users/ scripts/publish_catalog.py scripts/publish_catalog.ps1

$changes = git status --porcelain
if ($changes) {
    Write-Host "`n--> 4. Tao Git commit..." -ForegroundColor Yellow
    git commit -m "data(catalog): publish 1000 Best Buy products and users for team"
} else {
    Write-Host "`n--> Khong co thay doi moi can commit." -ForegroundColor Green
}

Write-Host "`n--> 5. Day (push) len GitHub origin/main..." -ForegroundColor Yellow
git push origin main

Write-Host "`n============================================================" -ForegroundColor Green
Write-Host " HOAN TAT DAY DATA LEN GITHUB! " -ForegroundColor Green
Write-Host " Teammate cua ban (Phan Van Phap) chi can go lenh:" -ForegroundColor Green
Write-Host "     git pull origin main" -ForegroundColor White
Write-Host " la se co day du toan bo 1.000 san pham va 1.000 users tren may!" -ForegroundColor Green
Write-Host "============================================================" -ForegroundColor Green
