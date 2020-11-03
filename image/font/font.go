package font

import (
	"github.com/agilecho/tec/image/fixed"
	"image"
	"image/draw"
	"io"
	"unicode/utf8"
)

type Face interface {
	io.Closer
	Glyph(dot fixed.Point26_6, r rune) (dr image.Rectangle, mask image.Image, maskp image.Point, advance fixed.Int26_6, ok bool)
	GlyphBounds(r rune) (bounds fixed.Rectangle26_6, advance fixed.Int26_6, ok bool)
	GlyphAdvance(r rune) (advance fixed.Int26_6, ok bool)
	Kern(r0, r1 rune) fixed.Int26_6
	Metrics() Metrics
}

type Metrics struct {
	Height fixed.Int26_6
	Ascent fixed.Int26_6
	Descent fixed.Int26_6
	XHeight fixed.Int26_6
	CapHeight fixed.Int26_6
	CaretSlope image.Point
}

type Drawer struct {
	Dst draw.Image
	Src image.Image
	Face Face
	Dot fixed.Point26_6
}

func (d *Drawer) DrawBytes(s []byte) {
	prevC := rune(-1)
	for len(s) > 0 {
		c, size := utf8.DecodeRune(s)
		s = s[size:]
		if prevC >= 0 {
			d.Dot.X += d.Face.Kern(prevC, c)
		}
		dr, mask, maskp, advance, ok := d.Face.Glyph(d.Dot, c)
		if !ok {
			continue
		}
		draw.DrawMask(d.Dst, dr, d.Src, image.Point{}, mask, maskp, draw.Over)
		d.Dot.X += advance
		prevC = c
	}
}

func (d *Drawer) DrawString(s string) {
	prevC := rune(-1)
	for _, c := range s {
		if prevC >= 0 {
			d.Dot.X += d.Face.Kern(prevC, c)
		}
		dr, mask, maskp, advance, ok := d.Face.Glyph(d.Dot, c)
		if !ok {
			continue
		}
		draw.DrawMask(d.Dst, dr, d.Src, image.Point{}, mask, maskp, draw.Over)
		d.Dot.X += advance
		prevC = c
	}
}

func (d *Drawer) BoundBytes(s []byte) (bounds fixed.Rectangle26_6, advance fixed.Int26_6) {
	bounds, advance = BoundBytes(d.Face, s)
	bounds.Min = bounds.Min.Add(d.Dot)
	bounds.Max = bounds.Max.Add(d.Dot)
	return
}

func (d *Drawer) BoundString(s string) (bounds fixed.Rectangle26_6, advance fixed.Int26_6) {
	bounds, advance = BoundString(d.Face, s)
	bounds.Min = bounds.Min.Add(d.Dot)
	bounds.Max = bounds.Max.Add(d.Dot)
	return
}

func (d *Drawer) MeasureBytes(s []byte) (advance fixed.Int26_6) {
	return MeasureBytes(d.Face, s)
}

func (d *Drawer) MeasureString(s string) (advance fixed.Int26_6) {
	return MeasureString(d.Face, s)
}

func BoundBytes(f Face, s []byte) (bounds fixed.Rectangle26_6, advance fixed.Int26_6) {
	prevC := rune(-1)
	for len(s) > 0 {
		c, size := utf8.DecodeRune(s)
		s = s[size:]
		if prevC >= 0 {
			advance += f.Kern(prevC, c)
		}
		b, a, ok := f.GlyphBounds(c)
		if !ok {
			continue
		}
		b.Min.X += advance
		b.Max.X += advance
		bounds = bounds.Union(b)
		advance += a
		prevC = c
	}
	return
}

func BoundString(f Face, s string) (bounds fixed.Rectangle26_6, advance fixed.Int26_6) {
	prevC := rune(-1)
	for _, c := range s {
		if prevC >= 0 {
			advance += f.Kern(prevC, c)
		}
		b, a, ok := f.GlyphBounds(c)
		if !ok {
			continue
		}
		b.Min.X += advance
		b.Max.X += advance
		bounds = bounds.Union(b)
		advance += a
		prevC = c
	}
	return
}

func MeasureBytes(f Face, s []byte) (advance fixed.Int26_6) {
	prevC := rune(-1)
	for len(s) > 0 {
		c, size := utf8.DecodeRune(s)
		s = s[size:]
		if prevC >= 0 {
			advance += f.Kern(prevC, c)
		}
		a, ok := f.GlyphAdvance(c)
		if !ok {
			continue
		}
		advance += a
		prevC = c
	}
	return advance
}

func MeasureString(f Face, s string) (advance fixed.Int26_6) {
	prevC := rune(-1)
	for _, c := range s {
		if prevC >= 0 {
			advance += f.Kern(prevC, c)
		}
		a, ok := f.GlyphAdvance(c)
		if !ok {
			continue
		}
		advance += a
		prevC = c
	}
	return advance
}

type Hinting int

const (
	HintingNone Hinting = iota
	HintingVertical
	HintingFull
)

type Stretch int
type Style int
type Weight int
