package raster

import (
	"image"
	"image/color"
	"image/draw"
	"math"
)

type Span struct {
	Y, X0, X1 int
	Alpha     uint32
}

type Painter interface {
	Paint(ss []Span, done bool)
}

type PainterFunc func(ss []Span, done bool)

func (f PainterFunc) Paint(ss []Span, done bool) { f(ss, done) }

type AlphaOverPainter struct {
	Image *image.Alpha
}

func (r AlphaOverPainter) Paint(ss []Span, done bool) {
	b := r.Image.Bounds()
	for _, s := range ss {
		if s.Y < b.Min.Y {
			continue
		}

		if s.Y >= b.Max.Y {
			return
		}

		if s.X0 < b.Min.X {
			s.X0 = b.Min.X
		}

		if s.X1 > b.Max.X {
			s.X1 = b.Max.X
		}

		if s.X0 >= s.X1 {
			continue
		}

		base := (s.Y-r.Image.Rect.Min.Y)*r.Image.Stride - r.Image.Rect.Min.X
		p := r.Image.Pix[base+s.X0 : base+s.X1]
		a := int(s.Alpha >> 8)
		for i, c := range p {
			v := int(c)
			p[i] = uint8((v*255 + (255-v)*a) / 255)
		}
	}
}

type AlphaSrcPainter struct {
	Image *image.Alpha
}

func (r AlphaSrcPainter) Paint(ss []Span, done bool) {
	b := r.Image.Bounds()
	for _, s := range ss {
		if s.Y < b.Min.Y {
			continue
		}
		if s.Y >= b.Max.Y {
			return
		}
		if s.X0 < b.Min.X {
			s.X0 = b.Min.X
		}
		if s.X1 > b.Max.X {
			s.X1 = b.Max.X
		}
		if s.X0 >= s.X1 {
			continue
		}
		base := (s.Y-r.Image.Rect.Min.Y)*r.Image.Stride - r.Image.Rect.Min.X
		p := r.Image.Pix[base+s.X0 : base+s.X1]
		color := uint8(s.Alpha >> 8)
		for i := range p {
			p[i] = color
		}
	}
}

func NewAlphaSrcPainter(m *image.Alpha) AlphaSrcPainter {
	return AlphaSrcPainter{m}
}

type RGBAPainter struct {
	Image *image.RGBA
	Op draw.Op
	cr, cg, cb, ca uint32
}

func (r *RGBAPainter) Paint(ss []Span, done bool) {
	b := r.Image.Bounds()
	for _, s := range ss {
		if s.Y < b.Min.Y {
			continue
		}

		if s.Y >= b.Max.Y {
			return
		}

		if s.X0 < b.Min.X {
			s.X0 = b.Min.X
		}

		if s.X1 > b.Max.X {
			s.X1 = b.Max.X
		}

		if s.X0 >= s.X1 {
			continue
		}

		ma := s.Alpha
		const m = 1<<16 - 1
		i0 := (s.Y-r.Image.Rect.Min.Y)*r.Image.Stride + (s.X0-r.Image.Rect.Min.X)*4
		i1 := i0 + (s.X1-s.X0)*4

		if r.Op == draw.Over {
			for i := i0; i < i1; i += 4 {
				dr := uint32(r.Image.Pix[i+0])
				dg := uint32(r.Image.Pix[i+1])
				db := uint32(r.Image.Pix[i+2])
				da := uint32(r.Image.Pix[i+3])
				a := (m - (r.ca * ma / m)) * 0x101
				r.Image.Pix[i+0] = uint8((dr*a + r.cr*ma) / m >> 8)
				r.Image.Pix[i+1] = uint8((dg*a + r.cg*ma) / m >> 8)
				r.Image.Pix[i+2] = uint8((db*a + r.cb*ma) / m >> 8)
				r.Image.Pix[i+3] = uint8((da*a + r.ca*ma) / m >> 8)
			}
		} else {
			for i := i0; i < i1; i += 4 {
				r.Image.Pix[i+0] = uint8(r.cr * ma / m >> 8)
				r.Image.Pix[i+1] = uint8(r.cg * ma / m >> 8)
				r.Image.Pix[i+2] = uint8(r.cb * ma / m >> 8)
				r.Image.Pix[i+3] = uint8(r.ca * ma / m >> 8)
			}
		}
	}
}

func (r *RGBAPainter) SetColor(c color.Color) {
	r.cr, r.cg, r.cb, r.ca = c.RGBA()
}

type MonochromePainter struct {
	Painter   Painter
	y, x0, x1 int
}

func (m *MonochromePainter) Paint(ss []Span, done bool) {
	j := 0

	for _, s := range ss {
		if s.Alpha >= 0x8000 {
			if m.y == s.Y && m.x1 == s.X0 {
				m.x1 = s.X1
			} else {
				ss[j] = Span{m.y, m.x0, m.x1, 1<<16 - 1}
				j++
				m.y, m.x0, m.x1 = s.Y, s.X0, s.X1
			}
		}
	}

	if done {
		finalSpan := Span{m.y, m.x0, m.x1, 1<<16 - 1}
		if j < len(ss) {
			ss[j] = finalSpan
			j++
			m.Painter.Paint(ss[:j], true)
		} else if j == len(ss) {
			m.Painter.Paint(ss, false)
			if cap(ss) > 0 {
				ss = ss[:1]
			} else {
				ss = make([]Span, 1)
			}
			ss[0] = finalSpan
			m.Painter.Paint(ss, true)
		} else {
			panic("unreachable")
		}

		m.y, m.x0, m.x1 = 0, 0, 0
	} else {
		m.Painter.Paint(ss[:j], false)
	}
}

type GammaCorrectionPainter struct {
	Painter Painter
	a [256]uint16
	gammaIsOne bool
}

func (g *GammaCorrectionPainter) Paint(ss []Span, done bool) {
	if !g.gammaIsOne {
		const n = 0x101
		for i, s := range ss {
			if s.Alpha == 0 || s.Alpha == 0xffff {
				continue
			}

			p, q := s.Alpha/n, s.Alpha%n
			a := uint32(g.a[p])*(n-q) + uint32(g.a[p+1])*q
			ss[i].Alpha = (a + n/2) / n
		}
	}

	g.Painter.Paint(ss, done)
}

func (g *GammaCorrectionPainter) SetGamma(gamma float64) {
	g.gammaIsOne = gamma == 1
	if g.gammaIsOne {
		return
	}
	for i := 0; i < 256; i++ {
		a := float64(i) / 0xff
		a = math.Pow(a, gamma)
		g.a[i] = uint16(0xffff * a)
	}
}