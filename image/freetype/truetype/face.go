package truetype

import (
	"github.com/agilecho/tec/image/fixed"
	"github.com/agilecho/tec/image/font"
)

func powerOf2(i int) bool {
	return i != 0 && (i&(i-1)) == 0
}

type Options struct {
	Size float64
	DPI float64
	Hinting font.Hinting
	GlyphCacheEntries int
	SubPixelsX int
	SubPixelsY int
}

func (o *Options) size() float64 {
	if o != nil && o.Size > 0 {
		return o.Size
	}
	return 12
}

func (o *Options) dpi() float64 {
	if o != nil && o.DPI > 0 {
		return o.DPI
	}
	return 72
}

func (o *Options) hinting() font.Hinting {
	if o != nil {
		switch o.Hinting {
		case font.HintingVertical, font.HintingFull:
			return font.HintingFull
		}
	}

	return font.HintingNone
}

func (o *Options) glyphCacheEntries() int {
	if o != nil && powerOf2(o.GlyphCacheEntries) {
		return o.GlyphCacheEntries
	}

	return 512
}

func (o *Options) subPixelsX() (value uint32, halfQuantum, mask fixed.Int26_6) {
	if o != nil {
		switch o.SubPixelsX {
		case 1, 2, 4, 8, 16, 32, 64:
			return subPixels(o.SubPixelsX)
		}
	}

	return subPixels(4)
}

func (o *Options) subPixelsY() (value uint32, halfQuantum, mask fixed.Int26_6) {
	if o != nil {
		switch o.SubPixelsX {
		case 1, 2, 4, 8, 16, 32, 64:
			return subPixels(o.SubPixelsX)
		}
	}

	return subPixels(1)
}

func subPixels(q int) (value uint32, bias, mask fixed.Int26_6) {
	return uint32(q), 32 / fixed.Int26_6(q), -64 / fixed.Int26_6(q)
}