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

type Result struct {
	Data1Name string
	Lat1      float64
	Lon1      float64
	Data2Name string
	Lat2      float64
	Lon2      float64
	Distance  float64
}

// Fungsi utama
func main() {
	startTime := time.Now()

	// Maksimalkan penggunaan CPU
	numCPU := runtime.NumCPU()
	runtime.GOMAXPROCS(numCPU)
	fmt.Printf("Menggunakan %d core CPU\n", numCPU)

	// Nama file input dan output
	data1File := "data1.csv"
	data2File := "data2.csv"
	outputFile := "all_combinations_distances.csv"

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

	// Channel untuk hasil
	results := make(chan Result, len(data1)*len(data2))

	// Worker pool untuk memproses data secara paralel
	var wg sync.WaitGroup
	numWorkers := 8
	chunkSize := len(data1) / numWorkers
	for i := 0; i < numWorkers; i++ {
		start := i * chunkSize
		end := start + chunkSize
		if i == numWorkers-1 {
			end = len(data1)
		}
		wg.Add(1)
		go worker(data1[start:end], data2, results, &wg)
	}

	// Tunggu semua worker selesai
	go func() {
		wg.Wait()
		close(results)
	}()

	// Tulis hasil ke file output
	if err := writeResults(outputFile, results); err != nil {
		fmt.Println("Gagal menulis file output:", err)
		return
	}

	duration := time.Since(startTime)
	fmt.Printf("Proses selesai. File output tersimpan di %s.\n", outputFile)
	fmt.Printf("Lama waktu eksekusi: %s\n", duration)
}

// Fungsi untuk membaca file CSV dengan separator otomatis
func loadCSVWithAutoSeparator(filePath string) ([]Point, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

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

	_, err = file.Seek(0, io.SeekStart)
	if err != nil {
		return nil, err
	}

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
func worker(data1, data2 []Point, results chan Result, wg *sync.WaitGroup) {
	defer wg.Done()

	for _, p1 := range data1 {
		for _, p2 := range data2 {
			distance := haversine(p1.Latitude, p1.Longitude, p2.Latitude, p2.Longitude)
			results <- Result{
				Data1Name: p1.Name,
				Lat1:      p1.Latitude,
				Lon1:      p1.Longitude,
				Data2Name: p2.Name,
				Lat2:      p2.Latitude,
				Lon2:      p2.Longitude,
				Distance:  distance,
			}
		}
	}
}

// Fungsi Haversine untuk menghitung jarak
func haversine(lat1, lon1, lat2, lon2 float64) float64 {
	const R = 6371000
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
func writeResults(filePath string, results chan Result) error {
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Tulis header
	writer.Write([]string{
		"data1_name", "lat1", "lon1", "data2_name", "lat2", "lon2", "distance_meters",
	})

	// Tulis hasil
	for result := range results {
		writer.Write([]string{
			result.Data1Name,
			fmt.Sprintf("%.6f", result.Lat1),
			fmt.Sprintf("%.6f", result.Lon1),
			result.Data2Name,
			fmt.Sprintf("%.6f", result.Lat2),
			fmt.Sprintf("%.6f", result.Lon2),
			fmt.Sprintf("%.2f", result.Distance),
		})
	}

	return nil
}
