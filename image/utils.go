package image

import (
	"image"
	"math/rand"
	"runtime"
	"sync"
)

func parallel(start, stop int, fn func(<-chan int)) {
	count := stop - start
	if count < 1 {
		return
	}

	procs := runtime.GOMAXPROCS(0)
	if procs > count {
		procs = count
	}

	c := make(chan int, count)
	for i := start; i < stop; i++ {
		c <- i
	}
	close(c)

	var wg sync.WaitGroup
	for i := 0; i < procs; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			fn(c)
		}()
	}
	wg.Wait()
}

func clamp(x float64) uint8 {
	v := int64(x + 0.5)
	if v > 255 {
		return 255
	}
	if v > 0 {
		return uint8(v)
	}
	return 0
}


func toNRGBA(img image.Image) *image.NRGBA {
	if img, ok := img.(*image.NRGBA); ok {
		return &image.NRGBA{
			Pix:    img.Pix,
			Stride: img.Stride,
			Rect:   img.Rect.Sub(img.Rect.Min),
		}
	}
	return Clone(img)
}

func min3(x, y, z uint8) (m uint8) {
	m = x
	if y < m {
		m = y
	}
	if z < m {
		m = z
	}
	return
}

func max3(x, y, z uint8) (m uint8) {
	m = x
	if y > m {
		m = y
	}
	if z > m {
		m = z
	}
	return
}

func randIntn(n int) int {
	return rand.Intn(n)
}

func randInt(from, to int) int {
	return rand.Intn(to + 1 - from) + from
}

func randFloat(from, to float64) float64 {
	return (to - from) * rand.Float64() + from
}