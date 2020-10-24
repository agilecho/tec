package image

import (
	"image"
)

func Clone(img image.Image) *image.NRGBA {
	src := newScanner(img)
	dst := image.NewNRGBA(image.Rect(0, 0, src.w, src.h))
	size := src.w * 4
	parallel(0, src.h, func(ys <-chan int) {
		for y := range ys {
			i := y * dst.Stride
			src.scan(0, y, src.w, y+1, dst.Pix[i:i+size])
		}
	})

	return dst
}

const (
	Center int = iota
	TopLeft
	Top
	TopRight
	Left
	Right
	BottomLeft
	Bottom
	BottomRight
)

func anchorPt(b image.Rectangle, w, h int, anchor int) image.Point {
	var x, y int
	switch anchor {
	case TopLeft:
		x = b.Min.X
		y = b.Min.Y
	case Top:
		x = b.Min.X + (b.Dx()-w)/2
		y = b.Min.Y
	case TopRight:
		x = b.Max.X - w
		y = b.Min.Y
	case Left:
		x = b.Min.X
		y = b.Min.Y + (b.Dy()-h)/2
	case Right:
		x = b.Max.X - w
		y = b.Min.Y + (b.Dy()-h)/2
	case BottomLeft:
		x = b.Min.X
		y = b.Max.Y - h
	case Bottom:
		x = b.Min.X + (b.Dx()-w)/2
		y = b.Max.Y - h
	case BottomRight:
		x = b.Max.X - w
		y = b.Max.Y - h
	default:
		x = b.Min.X + (b.Dx()-w)/2
		y = b.Min.Y + (b.Dy()-h)/2
	}
	return image.Pt(x, y)
}

func Crop(img image.Image, rect image.Rectangle) *image.NRGBA {
	r := rect.Intersect(img.Bounds()).Sub(img.Bounds().Min)
	if r.Empty() {
		return &image.NRGBA{}
	}
	src := newScanner(img)
	dst := image.NewNRGBA(image.Rect(0, 0, r.Dx(), r.Dy()))
	rowSize := r.Dx() * 4
	parallel(r.Min.Y, r.Max.Y, func(ys <-chan int) {
		for y := range ys {
			i := (y - r.Min.Y) * dst.Stride
			src.scan(r.Min.X, y, r.Max.X, y+1, dst.Pix[i:i+rowSize])
		}
	})
	return dst
}

func CropAnchor(img image.Image, width, height int, anchor int) *image.NRGBA {
	srcBounds := img.Bounds()
	pt := anchorPt(srcBounds, width, height, anchor)
	r := image.Rect(0, 0, width, height).Add(pt)
	b := srcBounds.Intersect(r)
	return Crop(img, b)
}
