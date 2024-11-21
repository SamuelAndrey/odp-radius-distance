package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"math"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Point struct {
	Name      string
	Latitude  float64
	Longitude float64
}

// Fungsi utama
func main() {
	// Catat waktu mulai eksekusi
	startTime := time.Now()

	// Maksimalkan penggunaan CPU
	numCPU := runtime.NumCPU()
	runtime.GOMAXPROCS(numCPU) // Gunakan semua core CPU yang tersedia
	fmt.Printf("Menggunakan %d core CPU\n", numCPU)

	// Nama file input dan output
	data1File := "data1.csv"
	data2File := "data2.csv"
	outputFile := "nearest_points.csv"

	// Ambil radius maksimal dari argumen baris perintah
	if len(os.Args) < 2 {
		fmt.Println("Usage: go run main.go <radius>")
		return
	}
	maxRadius, err := strconv.ParseFloat(os.Args[1], 64)
	if err != nil {
		fmt.Println("Gagal membaca input radius:", err)
		return
	}

	// Membaca data input
	data1, err := loadCSVWithAutoSeparator(data1File)
	if err != nil {
		fmt.Println("Gagal membaca file data1.csv:", err)
		return
	}

	data2, err := loadCSVWithAutoSeparator(data2File)
	if err != nil {
		fmt.Println("Gagal membaca file data2.csv:", err)
		return
	}

	// Membuat channel untuk hasil dengan kapasitas buffer
	results := make(chan []string, len(data1))

	// Worker pool untuk memproses data secara paralel
	var wg sync.WaitGroup
	numWorkers := 8
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go worker(data1, data2, maxRadius, results, &wg)
	}

	// Tunggu semua worker selesai
	go func() {
		wg.Wait()
		close(results) // Tutup channel setelah semua worker selesai
	}()

	// Tulis hasil ke file output
	if err := writeResults(outputFile, results); err != nil {
		fmt.Println("Gagal menulis file output:", err)
		return
	}

	// Catat waktu selesai eksekusi
	endTime := time.Now()

	// Hitung lama waktu eksekusi
	duration := endTime.Sub(startTime)

	// Tampilkan informasi hasil
	fmt.Printf("Proses selesai. File output tersimpan di %s.\n", outputFile)
	fmt.Printf("Lama waktu eksekusi: %s\n", duration)
}

// Fungsi untuk membaca file CSV dengan deteksi separator otomatis
func loadCSVWithAutoSeparator(filePath string) ([]Point, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	// Deteksi separator dari beberapa baris pertama
	buf := make([]byte, 1024)
	n, err := file.Read(buf)
	if err != nil && err != io.EOF {
		return nil, err
	}
	content := string(buf[:n])

	var separator rune
	if strings.Contains(content, ";") {
		separator = ';'
	} else {
		separator = ','
	}

	// Reset file pointer ke awal
	_, err = file.Seek(0, io.SeekStart)
	if err != nil {
		return nil, err
	}

	// Membaca file dengan separator yang terdeteksi
	reader := csv.NewReader(file)
	reader.Comma = separator
	rows, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}

	points := make([]Point, len(rows)-1)
	for i, row := range rows[1:] {
		lat, _ := strconv.ParseFloat(row[1], 64)
		lon, _ := strconv.ParseFloat(row[2], 64)
		points[i] = Point{Name: row[0], Latitude: lat, Longitude: lon}
	}

	return points, nil
}

// Worker untuk memproses data secara paralel
func worker(data1, data2 []Point, maxRadius float64, results chan []string, wg *sync.WaitGroup) {
	defer wg.Done() // Pastikan WaitGroup selesai

	for _, p1 := range data1 {
		nearest, distance := findNearestWithinRadius(p1, data2, maxRadius)
		if nearest != nil {
			results <- []string{
				p1.Name,
				fmt.Sprintf("%.6f", p1.Latitude),
				fmt.Sprintf("%.6f", p1.Longitude),
				nearest.Name,
				fmt.Sprintf("%.6f", nearest.Latitude),
				fmt.Sprintf("%.6f", nearest.Longitude),
				fmt.Sprintf("%.2f", distance),
			}
		}
	}
}

// Fungsi untuk mencari titik terdekat dalam radius maksimal
func findNearestWithinRadius(p1 Point, data2 []Point, maxRadius float64) (*Point, float64) {
	var nearest *Point
	minDistance := math.MaxFloat64
	for _, p2 := range data2 {
		distance := haversine(p1.Latitude, p1.Longitude, p2.Latitude, p2.Longitude)
		if distance <= maxRadius && distance < minDistance {
			minDistance = distance
			nearest = &p2
		}
	}
	return nearest, minDistance
}

// Fungsi Haversine untuk menghitung jarak
func haversine(lat1, lon1, lat2, lon2 float64) float64 {
	const R = 6371000 // Radius bumi dalam meter
	lat1Rad, lon1Rad := lat1*math.Pi/180, lon1*math.Pi/180
	lat2Rad, lon2Rad := lat2*math.Pi/180, lon2*math.Pi/180

	dlat := lat2Rad - lat1Rad
	dlon := lon2Rad - lon1Rad

	a := math.Sin(dlat/2)*math.Sin(dlat/2) +
		math.Cos(lat1Rad)*math.Cos(lat2Rad)*math.Sin(dlon/2)*math.Sin(dlon/2)
	c := 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))

	return R * c
}

// Fungsi untuk menulis hasil ke file output
func writeResults(filePath string, results chan []string) error {
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Tulis header
	writer.Write([]string{
		"nama_titik_data1", "latitude_data1", "longitude_data1",
		"nama_titik_terdekat_data2", "latitude_terdekat_data2", "longitude_terdekat_data2", "distance_meters",
	})

	// Tulis hasil
	for result := range results {
		writer.Write(result)
	}

	return nil
}
