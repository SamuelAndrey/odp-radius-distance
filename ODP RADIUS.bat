@echo off
REM Menampilkan informasi program
echo =============================================
echo         Program Perhitungan Jarak ODP        
echo =============================================
echo.

REM Pindah ke direktori program
cd /d D:\jarak-odp

REM Pastikan file data1.csv dan data2.csv ada di direktori
if not exist "data1.csv" (
    echo [ERROR] File data1.csv tidak ditemukan.
    pause
    exit /b
)
if not exist "data2.csv" (
    echo [ERROR] File data2.csv tidak ditemukan.
    pause
    exit /b
)

REM Cek apakah Go terinstall
go version >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Program Go tidak terinstall di komputer ini.
    echo Silakan install Go dari https://go.dev/dl/.
    pause
    exit /b
)

REM Meminta input radius maksimal
set /p radiusInput=Masukkan jarak maksimal (dalam meter):

REM Jalankan program Go dengan radius sebagai argumen
echo Sedang memproses data...
go run main.go %radiusInput%

REM Cek apakah file output berhasil dibuat
if exist "nearest_points.csv" (
    echo.
    echo [SUCCESS] Proses selesai. File output bernama nearest_points.csv berhasil dibuat.
    echo.
) else (
    echo.
    echo [ERROR] Proses gagal. Periksa kembali file input atau radius maksimal.
    echo.
)

REM Menunggu pengguna menutup terminal
pause
