package image

import (
	"image"
	"math"
)

type indexWeight struct {
	index  int
	weight float64
}

func precomputeWeights(dstSize, srcSize int, filter ResampleFilter) [][]indexWeight {
	du := float64(srcSize) / float64(dstSize)
	scale := du
	if scale < 1.0 {
		scale = 1.0
	}
	ru := math.Ceil(scale * filter.Support)

	out := make([][]indexWeight, dstSize)
	tmp := make([]indexWeight, 0, dstSize*int(ru+2)*2)

	for v := 0; v < dstSize; v++ {
		fu := (float64(v)+0.5)*du - 0.5

		begin := int(math.Ceil(fu - ru))
		if begin < 0 {
			begin = 0
		}
		end := int(math.Floor(fu + ru))
		if end > srcSize-1 {
			end = srcSize - 1
		}

		var sum float64
		for u := begin; u <= end; u++ {
			w := filter.Kernel((float64(u) - fu) / scale)
			if w != 0 {
				sum += w
				tmp = append(tmp, indexWeight{index: u, weight: w})
			}
		}
		if sum != 0 {
			for i := range tmp {
				tmp[i].weight /= sum
			}
		}

		out[v] = tmp
		tmp = tmp[len(tmp):]
	}

	return out
}

func Resize(img image.Image, width, height int, filter ResampleFilter) *image.NRGBA {
	dstW, dstH := width, height
	if dstW < 0 || dstH < 0 {
		return &image.NRGBA{}
	}
	if dstW == 0 && dstH == 0 {
		return &image.NRGBA{}
	}

	srcW := img.Bounds().Dx()
	srcH := img.Bounds().Dy()
	if srcW <= 0 || srcH <= 0 {
		return &image.NRGBA{}
	}

	if dstW == 0 {
		tmpW := float64(dstH) * float64(srcW) / float64(srcH)
		dstW = int(math.Max(1.0, math.Floor(tmpW+0.5)))
	}
	if dstH == 0 {
		tmpH := float64(dstW) * float64(srcH) / float64(srcW)
		dstH = int(math.Max(1.0, math.Floor(tmpH+0.5)))
	}

	if filter.Support <= 0 {
		return resizeNearest(img, dstW, dstH)
	}

	if srcW != dstW && srcH != dstH {
		return resizeVertical(resizeHorizontal(img, dstW, filter), dstH, filter)
	}
	if srcW != dstW {
		return resizeHorizontal(img, dstW, filter)
	}
	if srcH != dstH {
		return resizeVertical(img, dstH, filter)
	}
	return Clone(img)
}

func resizeHorizontal(img image.Image, width int, filter ResampleFilter) *image.NRGBA {
	src := newScanner(img)
	dst := image.NewNRGBA(image.Rect(0, 0, width, src.h))
	weights := precomputeWeights(width, src.w, filter)
	parallel(0, src.h, func(ys <-chan int) {
		scanLine := make([]uint8, src.w*4)
		for y := range ys {
			src.scan(0, y, src.w, y+1, scanLine)
			j0 := y * dst.Stride
			for x := range weights {
				var r, g, b, a float64
				for _, w := range weights[x] {
					i := w.index * 4
					s := scanLine[i : i+4 : i+4]
					aw := float64(s[3]) * w.weight
					r += float64(s[0]) * aw
					g += float64(s[1]) * aw
					b += float64(s[2]) * aw
					a += aw
				}
				if a != 0 {
					aInv := 1 / a
					j := j0 + x*4
					d := dst.Pix[j : j+4 : j+4]
					d[0] = clamp(r * aInv)
					d[1] = clamp(g * aInv)
					d[2] = clamp(b * aInv)
					d[3] = clamp(a)
				}
			}
		}
	})
	return dst
}

func resizeVertical(img image.Image, height int, filter ResampleFilter) *image.NRGBA {
	src := newScanner(img)
	dst := image.NewNRGBA(image.Rect(0, 0, src.w, height))
	weights := precomputeWeights(height, src.h, filter)
	parallel(0, src.w, func(xs <-chan int) {
		scanLine := make([]uint8, src.h*4)
		for x := range xs {
			src.scan(x, 0, x+1, src.h, scanLine)
			for y := range weights {
				var r, g, b, a float64
				for _, w := range weights[y] {
					i := w.index * 4
					s := scanLine[i : i+4 : i+4]
					aw := float64(s[3]) * w.weight
					r += float64(s[0]) * aw
					g += float64(s[1]) * aw
					b += float64(s[2]) * aw
					a += aw
				}
				if a != 0 {
					aInv := 1 / a
					j := y*dst.Stride + x*4
					d := dst.Pix[j : j+4 : j+4]
					d[0] = clamp(r * aInv)
					d[1] = clamp(g * aInv)
					d[2] = clamp(b * aInv)
					d[3] = clamp(a)
				}
			}
		}
	})
	return dst
}

func resizeNearest(img image.Image, width, height int) *image.NRGBA {
	dst := image.NewNRGBA(image.Rect(0, 0, width, height))
	dx := float64(img.Bounds().Dx()) / float64(width)
	dy := float64(img.Bounds().Dy()) / float64(height)

	if dx > 1 && dy > 1 {
		src := newScanner(img)
		parallel(0, height, func(ys <-chan int) {
			for y := range ys {
				srcY := int((float64(y) + 0.5) * dy)
				dstOff := y * dst.Stride
				for x := 0; x < width; x++ {
					srcX := int((float64(x) + 0.5) * dx)
					src.scan(srcX, srcY, srcX+1, srcY+1, dst.Pix[dstOff:dstOff+4])
					dstOff += 4
				}
			}
		})
	} else {
		src := toNRGBA(img)
		parallel(0, height, func(ys <-chan int) {
			for y := range ys {
				srcY := int((float64(y) + 0.5) * dy)
				srcOff0 := srcY * src.Stride
				dstOff := y * dst.Stride
				for x := 0; x < width; x++ {
					srcX := int((float64(x) + 0.5) * dx)
					srcOff := srcOff0 + srcX*4
					copy(dst.Pix[dstOff:dstOff+4], src.Pix[srcOff:srcOff+4])
					dstOff += 4
				}
			}
		})
	}

	return dst
}

func Fit(img image.Image, width, height int, filter ResampleFilter) *image.NRGBA {
	maxW, maxH := width, height

	if maxW <= 0 || maxH <= 0 {
		return &image.NRGBA{}
	}

	srcBounds := img.Bounds()
	srcW := srcBounds.Dx()
	srcH := srcBounds.Dy()

	if srcW <= 0 || srcH <= 0 {
		return &image.NRGBA{}
	}

	if srcW <= maxW && srcH <= maxH {
		return Clone(img)
	}

	srcAspectRatio := float64(srcW) / float64(srcH)
	maxAspectRatio := float64(maxW) / float64(maxH)

	var newW, newH int
	if srcAspectRatio > maxAspectRatio {
		newW = maxW
		newH = int(float64(newW) / srcAspectRatio)
	} else {
		newH = maxH
		newW = int(float64(newH) * srcAspectRatio)
	}

	return Resize(img, newW, newH, filter)
}

func Fill(img image.Image, width, height int, anchor int, filter ResampleFilter) *image.NRGBA {
	dstW, dstH := width, height

	if dstW <= 0 || dstH <= 0 {
		return &image.NRGBA{}
	}

	srcBounds := img.Bounds()
	srcW := srcBounds.Dx()
	srcH := srcBounds.Dy()

	if srcW <= 0 || srcH <= 0 {
		return &image.NRGBA{}
	}

	if srcW == dstW && srcH == dstH {
		return Clone(img)
	}

	if srcW >= 100 && srcH >= 100 {
		return cropAndResize(img, dstW, dstH, anchor, filter)
	}

	return resizeAndCrop(img, dstW, dstH, anchor, filter)
}

func cropAndResize(img image.Image, width, height int, anchor int, filter ResampleFilter) *image.NRGBA {
	dstW, dstH := width, height

	srcBounds := img.Bounds()
	srcW := srcBounds.Dx()
	srcH := srcBounds.Dy()
	srcAspectRatio := float64(srcW) / float64(srcH)
	dstAspectRatio := float64(dstW) / float64(dstH)

	var tmp *image.NRGBA
	if srcAspectRatio < dstAspectRatio {
		cropH := float64(srcW) * float64(dstH) / float64(dstW)
		tmp = CropAnchor(img, srcW, int(math.Max(1, cropH)+0.5), anchor)
	} else {
		cropW := float64(srcH) * float64(dstW) / float64(dstH)
		tmp = CropAnchor(img, int(math.Max(1, cropW)+0.5), srcH, anchor)
	}

	return Resize(tmp, dstW, dstH, filter)
}

func resizeAndCrop(img image.Image, width, height int, anchor int, filter ResampleFilter) *image.NRGBA {
	dstW, dstH := width, height

	srcBounds := img.Bounds()
	srcW := srcBounds.Dx()
	srcH := srcBounds.Dy()
	srcAspectRatio := float64(srcW) / float64(srcH)
	dstAspectRatio := float64(dstW) / float64(dstH)

	var tmp *image.NRGBA
	if srcAspectRatio < dstAspectRatio {
		tmp = Resize(img, dstW, 0, filter)
	} else {
		tmp = Resize(img, 0, dstH, filter)
	}

	return CropAnchor(tmp, dstW, dstH, anchor)
}

type ResampleFilter struct {
	Support float64
	Kernel  func(float64) float64
}

var NearestNeighbor ResampleFilter
var Box ResampleFilter
var Linear ResampleFilter
var Hermite ResampleFilter
var MitchellNetravali ResampleFilter
var CatmullRom ResampleFilter
var BSpline ResampleFilter
var Gaussian ResampleFilter
var Bartlett ResampleFilter
var Lanczos ResampleFilter
var Hann ResampleFilter
var Hamming ResampleFilter
var Blackman ResampleFilter
var Welch ResampleFilter
var Cosine ResampleFilter

func bcspline(x, b, c float64) float64 {
	var y float64
	x = math.Abs(x)
	if x < 1.0 {
		y = ((12-9*b-6*c)*x*x*x + (-18+12*b+6*c)*x*x + (6 - 2*b)) / 6
	} else if x < 2.0 {
		y = ((-b-6*c)*x*x*x + (6*b+30*c)*x*x + (-12*b-48*c)*x + (8*b + 24*c)) / 6
	}
	return y
}

func sinc(x float64) float64 {
	if x == 0 {
		return 1
	}
	return math.Sin(math.Pi*x) / (math.Pi * x)
}

func init() {
	NearestNeighbor = ResampleFilter{
		Support: 0.0,
	}

	Box = ResampleFilter{
		Support: 0.5,
		Kernel: func(x float64) float64 {
			x = math.Abs(x)
			if x <= 0.5 {
				return 1.0
			}
			return 0
		},
	}

	Linear = ResampleFilter{
		Support: 1.0,
		Kernel: func(x float64) float64 {
			x = math.Abs(x)
			if x < 1.0 {
				return 1.0 - x
			}
			return 0
		},
	}

	Hermite = ResampleFilter{
		Support: 1.0,
		Kernel: func(x float64) float64 {
			x = math.Abs(x)
			if x < 1.0 {
				return bcspline(x, 0.0, 0.0)
			}
			return 0
		},
	}

	MitchellNetravali = ResampleFilter{
		Support: 2.0,
		Kernel: func(x float64) float64 {
			x = math.Abs(x)
			if x < 2.0 {
				return bcspline(x, 1.0/3.0, 1.0/3.0)
			}
			return 0
		},
	}

	CatmullRom = ResampleFilter{
		Support: 2.0,
		Kernel: func(x float64) float64 {
			x = math.Abs(x)
			if x < 2.0 {
				return bcspline(x, 0.0, 0.5)
			}
			return 0
		},
	}

	BSpline = ResampleFilter{
		Support: 2.0,
		Kernel: func(x float64) float64 {
			x = math.Abs(x)
			if x < 2.0 {
				return bcspline(x, 1.0, 0.0)
			}
			return 0
		},
	}

	Gaussian = ResampleFilter{
		Support: 2.0,
		Kernel: func(x float64) float64 {
			x = math.Abs(x)
			if x < 2.0 {
				return math.Exp(-2 * x * x)
			}
			return 0
		},
	}

	Bartlett = ResampleFilter{
		Support: 3.0,
		Kernel: func(x float64) float64 {
			x = math.Abs(x)
			if x < 3.0 {
				return sinc(x) * (3.0 - x) / 3.0
			}
			return 0
		},
	}

	Lanczos = ResampleFilter{
		Support: 3.0,
		Kernel: func(x float64) float64 {
			x = math.Abs(x)
			if x < 3.0 {
				return sinc(x) * sinc(x/3.0)
			}
			return 0
		},
	}

	Hann = ResampleFilter{
		Support: 3.0,
		Kernel: func(x float64) float64 {
			x = math.Abs(x)
			if x < 3.0 {
				return sinc(x) * (0.5 + 0.5*math.Cos(math.Pi*x/3.0))
			}
			return 0
		},
	}

	Hamming = ResampleFilter{
		Support: 3.0,
		Kernel: func(x float64) float64 {
			x = math.Abs(x)
			if x < 3.0 {
				return sinc(x) * (0.54 + 0.46*math.Cos(math.Pi*x/3.0))
			}
			return 0
		},
	}

	Blackman = ResampleFilter{
		Support: 3.0,
		Kernel: func(x float64) float64 {
			x = math.Abs(x)
			if x < 3.0 {
				return sinc(x) * (0.42 - 0.5*math.Cos(math.Pi*x/3.0+math.Pi) + 0.08*math.Cos(2.0*math.Pi*x/3.0))
			}
			return 0
		},
	}

	Welch = ResampleFilter{
		Support: 3.0,
		Kernel: func(x float64) float64 {
			x = math.Abs(x)
			if x < 3.0 {
				return sinc(x) * (1.0 - (x * x / 9.0))
			}
			return 0
		},
	}

	Cosine = ResampleFilter{
		Support: 3.0,
		Kernel: func(x float64) float64 {
			x = math.Abs(x)
			if x < 3.0 {
				return sinc(x) * math.Cos((math.Pi/2.0)*(x/3.0))
			}
			return 0
		},
	}
}
