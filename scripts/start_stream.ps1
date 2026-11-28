# Chạy luồng sự kiện hành vi người dùng qua Kafka và MinIO
param (
    [int]$Events = 1000,
    [float]$Rate = 20,
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = Split-Path -Parent $scriptDir
Set-Location $repoRoot

$python = ".\.venv\Scripts\python.exe"
$catalog = "data/bronze/products/products.jsonl"
$users = "data/bronze/users/users.jsonl"

Write-Host "=== Chạy Streaming Simulator ===" -ForegroundColor Cyan
Write-Host "Catalog: $catalog"
Write-Host "Users:   $users"
Write-Host "Events:  $Events (Tốc độ: $Rate events/giây)"

if ($DryRun) {
    Write-Host "Chế độ Dry-Run: in trực tiếp ra console..." -ForegroundColor Yellow
    & $python -m recommender.streaming.producer --catalog $catalog --users $users --dry-run --max-events $Events --rate $Rate
    exit 0
}

# Kiểm tra cổng Kafka 9092
$kafkaReady = $false
try {
    $tcp = New-Object System.Net.Sockets.TcpClient
    $tcp.Connect("127.0.0.1", 9092)
    $tcp.Close()
    $kafkaReady = $true
} catch {
    $kafkaReady = $false
}

if (-not $kafkaReady) {
    Write-Host "[CẢNH BÁO] Không kết nối được Kafka tại 127.0.0.1:9092!" -ForegroundColor Yellow
    Write-Host "Hãy mở Docker Desktop trên máy rồi chạy lệnh: docker compose up -d" -ForegroundColor Yellow
    Write-Host "Đang chuyển sang chế độ Dry-Run để chạy thử nghiệm..." -ForegroundColor Green
    & $python -m recommender.streaming.producer --catalog $catalog --users $users --dry-run --max-events 20 --rate 10
    exit 0
}

Write-Host "Kafka đã sẵn sàng! Bắt đầu phát stream sự kiện..." -ForegroundColor Green
& $python -m recommender.streaming.producer --catalog $catalog --users $users --bootstrap-servers 127.0.0.1:9092 --topic product-events --max-events $Events --rate $Rate

