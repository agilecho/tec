package image

import (
	"bytes"
	"image"
	"image/color"
	"image/png"
	"math"
	"strconv"
)

const (
	maxSkew = 0.35
	circleCount = 20
)

func randomPalette() color.Palette {
	p := make([]color.Color, circleCount+1)
	p[0] = color.RGBA{0xFF, 0xFF, 0xFF, 0x00}
	prim := color.RGBA{
		uint8(randIntn(129)),
		uint8(randIntn(129)),
		uint8(randIntn(129)),
		0xFF,
	}
	p[1] = prim
	for i := 2; i <= circleCount; i++ {
		p[i] = randomBrightness(prim, 255)
	}
	return p
}

func randomBrightness(c color.RGBA, max uint8) color.RGBA {
	minc := min3(c.R, c.G, c.B)
	maxc := max3(c.R, c.G, c.B)

	if maxc > max {
		return c
	}

	n := randIntn(int(max-maxc)) - int(minc)

	return color.RGBA{
		uint8(int(c.R) + n),
		uint8(int(c.G) + n),
		uint8(int(c.B) + n),
		uint8(c.A),
	}
}

type Captcha struct {
	*image.Paletted
	numWidth  int
	numHeight int
	dotSize   int
}

func (this *Captcha) calculateSizes(width, height, ncount int) {
	var border int
	if width > height {
		border = height / 4
	} else {
		border = width / 4
	}

	w := float64(width - border*2)
	h := float64(height - border*2)

	fw := float64(FONT_WIDTH + 1)
	fh := float64(FONT_HEIGHT)
	nc := float64(ncount)

	nw := w / nc
	nh := nw * fh / fw

	if nh > h {
		nh = h
		nw = fw / fh * nh
	}

	this.dotSize = int(nh / fh)
	this.numWidth = int(nw) - this.dotSize
	this.numHeight = int(nh)
}

func (this *Captcha) drawHorizLine(fromX, toX, y int, colorIdx uint8) {
	for x := fromX; x <= toX; x++ {
		this.SetColorIndex(x, y, colorIdx)
	}
}

func (this *Captcha) drawCircle(x, y, radius int, colorIdx uint8) {
	f := 1 - radius
	dfx := 1
	dfy := -2 * radius
	xo := 0
	yo := radius

	this.SetColorIndex(x, y+radius, colorIdx)
	this.SetColorIndex(x, y-radius, colorIdx)
	this.drawHorizLine(x-radius, x+radius, y, colorIdx)

	for xo < yo {
		if f >= 0 {
			yo--
			dfy += 2
			f += dfy
		}

		xo++
		dfx += 2
		f += dfx

		this.drawHorizLine(x-xo, x+xo, y+yo, colorIdx)
		this.drawHorizLine(x-xo, x+xo, y-yo, colorIdx)
		this.drawHorizLine(x-yo, x+yo, y+xo, colorIdx)
		this.drawHorizLine(x-yo, x+yo, y-xo, colorIdx)
	}
}

func (this *Captcha) fillWithCircles(n, maxradius int) {
	maxx := this.Bounds().Max.X
	maxy := this.Bounds().Max.Y

	for i := 0; i < n; i++ {
		colorIdx := uint8(randInt(1, circleCount-1))
		r := randInt(1, maxradius)
		this.drawCircle(randInt(r, maxx - r), randInt(r, maxy - r), r, colorIdx)
	}
}

func (this *Captcha) strikeThrough() {
	maxx := this.Bounds().Max.X
	maxy := this.Bounds().Max.Y

	y := randInt(maxy/3, maxy-maxy/3)

	amplitude := randFloat(5, 20)
	period := randFloat(80, 180)

	dx := 2.0 * math.Pi / period

	for x := 0; x < maxx; x++ {
		xo := amplitude * math.Cos(float64(y)*dx)
		yo := amplitude * math.Sin(float64(x)*dx)

		for yn := 0; yn < this.dotSize; yn++ {
			r := randInt(0, this.dotSize)
			this.drawCircle(x + int(xo), y + int(yo) + (yn * this.dotSize), r/2, 1)
		}
	}
}

func (this *Captcha) drawDigit(digit []byte, x, y int) {
	skf := randFloat(-maxSkew, maxSkew)
	xs := float64(x)
	r := this.dotSize / 2
	y += randInt(-r, r)

	for yo := 0; yo < FONT_HEIGHT; yo++ {
		for xo := 0; xo < FONT_WIDTH; xo++ {
			if digit[yo * FONT_WIDTH + xo] != BLACK_CHAR {
				continue
			}

			this.drawCircle(x + xo * this.dotSize, y + yo * this.dotSize, r, 1)
		}

		xs += skf
		x = int(xs)
	}
}

func (this *Captcha) distort(amplude float64, period float64) {
	w := this.Bounds().Max.X
	h := this.Bounds().Max.Y

	oldm := this.Paletted
	newm := image.NewPaletted(image.Rect(0, 0, w, h), oldm.Palette)

	dx := 2.0 * math.Pi / period
	for x := 0; x < w; x++ {
		for y := 0; y < h; y++ {
			xo := amplude * math.Sin(float64(y)*dx)
			yo := amplude * math.Cos(float64(x)*dx)

			newm.SetColorIndex(x, y, oldm.ColorIndexAt(x + int(xo), y + int(yo)))
		}
	}

	this.Paletted = newm
}

func (this *Captcha) PNG() []byte {
	var buf bytes.Buffer
	_ = png.Encode(&buf, this.Paletted)
	return buf.Bytes()
}

func NewCaptcha(random string, width, height int) *Captcha {
	digits := []byte{}
	for i := 0; i < len(random); i++ {
		tmp, _ := strconv.Atoi(string(random[i]))
		digits = append(digits, byte(tmp))
	}

	captch := Captcha{}
	captch.Paletted = image.NewPaletted(image.Rect(0, 0, width, height), randomPalette())
	captch.calculateSizes(width, height, len(digits))

	maxx := width - (captch.numWidth + captch.dotSize) * len(digits) - captch.dotSize
	maxy := height - captch.numHeight - captch.dotSize*2

	var border int
	if width > height {
		border = height / 5
	} else {
		border = width / 5
	}

	x := randInt(border, maxx-border)
	y := randInt(border, maxy-border)

	for _, n := range digits {
		captch.drawDigit(FONTS[n], x, y)
		x += captch.numWidth + captch.dotSize
	}

	captch.strikeThrough()
	captch.distort(randFloat(5, 10), randFloat(100, 200))
	captch.fillWithCircles(circleCount, captch.dotSize)

	return &captch
}
