package image

import (
	"github.com/agilecho/tec/image/freetype"
	"flag"
	"image"
	"image/color"
	"image/draw"
	"image/gif"
	"image/jpeg"
	"image/png"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
)

var dpi = flag.Float64("dpi", 72, "")

type circle struct {
	Point image.Point
	Radius int
}

func (this *circle) ColorModel() color.Model {
	return color.AlphaModel
}

func (this *circle) Bounds() image.Rectangle {
	return image.Rect(this.Point.X - this.Radius, this.Point.Y - this.Radius, this.Point.X + this.Radius, this.Point.Y + this.Radius)
}

func (this *circle) At(x, y int) color.Color {
	xx, yy, rr := float64(x - this.Point.X) + 0.5, float64(y - this.Point.Y) + 0.5, float64(this.Radius)
	if xx * xx + yy * yy < rr * rr {
		return color.Alpha{255}
	}

	return color.Alpha{0}
}

type Option struct {
	X int
	Y int
	Width int
	Height int

	Font string
	Size float64
	Color color.RGBA
	Circle bool

	Type int
	Target string
}

type Image struct {
	Backgroud string
	Width int
	Height int
	Type string

	image image.Image
	nrgba *image.NRGBA
}

func (this *Image) Circle(option Option) bool {
	ty := Center
	width := option.Width

	tmp := Fill(this.image, width, width, ty, Lanczos)
	if tmp == nil {
		return false
	}

	this.nrgba = this.makeCircleNRGBA(width, tmp)

	if option.Target != "" {
		return this.Save(option.Target)
	}

	return true
}

func (this *Image) makeCircleNRGBA(width int, tmp *image.NRGBA) *image.NRGBA {
	nrgba := image.NewNRGBA(image.Rect(0, 0, width, width))
	for h := nrgba.Rect.Min.Y; h < nrgba.Rect.Max.Y; h++ {
		for v := nrgba.Rect.Min.X; v < nrgba.Rect.Max.X; v++ {
			nrgba.Set(v, h, color.RGBA{0, 0, 0, 0})
		}
	}

	circle := &circle{
		Point: image.Point{width / 2, width / 2},
		Radius: width / 2,
	}

	draw.DrawMask(nrgba, nrgba.Bounds(), tmp, image.ZP, circle, image.ZP, draw.Over)
	return nrgba
}

func (this *Image) Merge(source string, option Option) bool {
	file, err := os.Open(source)
	if err != nil {
		return false
	}

	defer file.Close()

	tmp, _, err := image.Decode(file)
	if err != nil {
		return false
	}

	X := 0
	Y := 0

	X = option.X
	Y = option.Y

	if option.Width > 0 {
		if option.Circle {
			tmp = this.makeCircleNRGBA(option.Width, Fill(tmp, option.Width, option.Width, Center, Lanczos))
		} else {
			tmp = Fill(tmp, option.Width, option.Height, Center, Lanczos)
		}

		if tmp == nil {
			return false
		}
	}

	draw.Draw(this.nrgba, this.nrgba.Bounds().Add(image.Pt(X, Y)), tmp, image.Point{X:0, Y:0}, draw.Over)
	return true
}

func (this *Image) Text(data string, option Option) bool {
	if option.Font == "" {
		return false
	}

	fontBytes, err := ioutil.ReadFile(option.Font)
	if err != nil {
		return false
	}

	font, err := freetype.ParseFont(fontBytes)
	if err != nil {
		return false
	}

	if option.Color.R == 0 {
		option.Color = color.RGBA{0, 0, 0, 255}
	}

	context := freetype.NewContext()
	context.SetDPI(*dpi)
	context.SetFont(font)
	context.SetFontSize(option.Size)
	context.SetClip(this.nrgba.Bounds())
	context.SetDst(this.nrgba)
	context.SetSrc(&image.Uniform{option.Color})

	_, err = context.DrawString(data, freetype.Pt(option.X, option.Y))
	if err != nil {
		return false
	}

	return true
}

func (this *Image) Thumb(option Option) bool {
	width := option.Width
	height := option.Height
	ty := option.Type

	this.nrgba = Fill(this.image, width, height, ty, Lanczos)
	if this.nrgba == nil {
		return false
	}

	if option.Target != "" {
		return this.Save(option.Target)
	}

	return true
}

func (this *Image) Save(args ...string) bool {
	var path string
	if len(args) > 0 && args[0] != "" {
		path = args[0]
	} else {
		path = this.Backgroud
	}

	if path == "" {
		return false
	}

	file, err := os.OpenFile(path, os.O_SYNC | os.O_RDWR | os.O_CREATE, 0666)
	if err != nil {
		return false
	}

	defer file.Close()
	ext := filepath.Ext(path)

	if strings.EqualFold(ext, ".jpg") || strings.EqualFold(ext, ".jpeg") {
		err = jpeg.Encode(file, this.nrgba, &jpeg.Options{Quality: 80})
	} else if strings.EqualFold(ext, ".png") {
		err = png.Encode(file, this.nrgba)
	} else if strings.EqualFold(ext, ".gif") {
		err = gif.Encode(file, this.nrgba, &gif.Options{NumColors: 256})
	}

	if err != nil {
		return false
	}

	return true
}

func New(path string) *Image {
	img := Image{Backgroud:path}

	if path != "" {
		file, err := os.Open(path)
		if err != nil {
			return &img
		}

		defer file.Close()

		tmp, filetype, err := image.Decode(file)
		if err != nil {
			return &img
		}

		img.Width = tmp.Bounds().Max.X
		img.Height = tmp.Bounds().Max.Y

		img.Type = filetype
		img.image = tmp
	}

	return &img
}