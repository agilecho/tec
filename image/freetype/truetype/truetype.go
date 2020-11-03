package truetype

import (
	"fmt"
	"github.com/agilecho/tec/image/fixed"
)

type Index uint16
type NameID uint16

const (
	unicodeEncodingBMPOnly  = 0x00000003
	unicodeEncodingFull     = 0x00000004
	microsoftSymbolEncoding = 0x00030000
	microsoftUCS2Encoding   = 0x00030001
	microsoftUCS4Encoding   = 0x0003000a
)

type HMetric struct {
	AdvanceWidth, LeftSideBearing fixed.Int26_6
}

type VMetric struct {
	AdvanceHeight, TopSideBearing fixed.Int26_6
}

type FormatError string

func (e FormatError) Error() string {
	return "freetype: invalid TrueType format: " + string(e)
}

type UnsupportedError string

func (e UnsupportedError) Error() string {
	return "freetype: unsupported TrueType feature: " + string(e)
}

func u32(b []byte, i int) uint32 {
	return uint32(b[i])<<24 | uint32(b[i+1])<<16 | uint32(b[i+2])<<8 | uint32(b[i+3])
}

func u16(b []byte, i int) uint16 {
	return uint16(b[i])<<8 | uint16(b[i+1])
}

func readTable(ttf []byte, offsetLength []byte) ([]byte, error) {
	offset := int(u32(offsetLength, 0))
	if offset < 0 {
		return nil, FormatError(fmt.Sprintf("offset too large: %d", uint32(offset)))
	}
	length := int(u32(offsetLength, 4))
	if length < 0 {
		return nil, FormatError(fmt.Sprintf("length too large: %d", uint32(length)))
	}
	end := offset + length
	if end < 0 || end > len(ttf) {
		return nil, FormatError(fmt.Sprintf("offset + length too large: %d", uint32(offset)+uint32(length)))
	}
	return ttf[offset:end], nil
}

func parseSubtables(table []byte, name string, offset, size int, pred func([]byte) bool) (
	bestOffset int, bestPID uint32, retErr error) {

	if len(table) < 4 {
		return 0, 0, FormatError(name + " too short")
	}

	nSubtables := int(u16(table, 2))

	if len(table) < size*nSubtables+offset {
		return 0, 0, FormatError(name + " too short")
	}

	ok := false
	for i := 0; i < nSubtables; i, offset = i+1, offset+size {
		if pred != nil && !pred(table[offset:]) {
			continue
		}

		pidPsid := u32(table, offset)
		if pidPsid == unicodeEncodingBMPOnly || pidPsid == unicodeEncodingFull {
			bestOffset, bestPID, ok = offset, pidPsid>>16, true
			break
		} else if pidPsid == microsoftSymbolEncoding ||
			pidPsid == microsoftUCS2Encoding ||
			pidPsid == microsoftUCS4Encoding {
			bestOffset, bestPID, ok = offset, pidPsid>>16, true
		}
	}

	if !ok {
		return 0, 0, UnsupportedError(name + " encoding")
	}

	return bestOffset, bestPID, nil
}

const (
	locaOffsetFormatShort = 1
	locaOffsetFormatLong = 2
)

type cm struct {
	start, end, delta, offset uint32
}

type Font struct {
	cmap, cvt, fpgm, glyf, hdmx, head, hhea, hmtx, kern, loca, maxp, name, os2, prep, vmtx []byte
	cmapIndexes []byte
	cm                      []cm
	locaOffsetFormat        int
	nGlyph, nHMetric, nKern int
	fUnitsPerEm             int32
	ascent                  int32
	descent                 int32
	bounds                  fixed.Rectangle26_6
	maxTwilightPoints, maxStorage, maxFunctionDefs, maxStackElements uint16
}

func (f *Font) parseCmap() error {
	const (
		cmapFormat4         = 4
		cmapFormat12        = 12
		languageIndependent = 0
	)

	offset, _, err := parseSubtables(f.cmap, "cmap", 4, 8, nil)
	if err != nil {
		return err
	}
	offset = int(u32(f.cmap, offset+4))
	if offset <= 0 || offset > len(f.cmap) {
		return FormatError("bad cmap offset")
	}

	cmapFormat := u16(f.cmap, offset)
	switch cmapFormat {
	case cmapFormat4:
		language := u16(f.cmap, offset+4)
		if language != languageIndependent {
			return UnsupportedError(fmt.Sprintf("language: %d", language))
		}
		segCountX2 := int(u16(f.cmap, offset+6))

		if segCountX2%2 == 1 {
			return FormatError(fmt.Sprintf("bad segCountX2: %d", segCountX2))
		}

		segCount := segCountX2 / 2
		offset += 14
		f.cm = make([]cm, segCount)
		for i := 0; i < segCount; i++ {
			f.cm[i].end = uint32(u16(f.cmap, offset))
			offset += 2
		}

		offset += 2
		for i := 0; i < segCount; i++ {
			f.cm[i].start = uint32(u16(f.cmap, offset))
			offset += 2
		}

		for i := 0; i < segCount; i++ {
			f.cm[i].delta = uint32(u16(f.cmap, offset))
			offset += 2
		}

		for i := 0; i < segCount; i++ {
			f.cm[i].offset = uint32(u16(f.cmap, offset))
			offset += 2
		}

		f.cmapIndexes = f.cmap[offset:]
		return nil
	case cmapFormat12:
		if u16(f.cmap, offset+2) != 0 {
			return FormatError(fmt.Sprintf("cmap format: % x", f.cmap[offset:offset+4]))
		}
		length := u32(f.cmap, offset+4)
		language := u32(f.cmap, offset+8)

		if language != languageIndependent {
			return UnsupportedError(fmt.Sprintf("language: %d", language))
		}

		nGroups := u32(f.cmap, offset+12)
		if length != 12*nGroups+16 {
			return FormatError("inconsistent cmap length")
		}

		offset += 16
		f.cm = make([]cm, nGroups)
		for i := uint32(0); i < nGroups; i++ {
			f.cm[i].start = u32(f.cmap, offset+0)
			f.cm[i].end = u32(f.cmap, offset+4)
			f.cm[i].delta = u32(f.cmap, offset+8) - f.cm[i].start
			offset += 12
		}

		return nil
	}

	return UnsupportedError(fmt.Sprintf("cmap format: %d", cmapFormat))
}

func (f *Font) parseHead() error {
	if len(f.head) != 54 {
		return FormatError(fmt.Sprintf("bad head length: %d", len(f.head)))
	}

	f.fUnitsPerEm = int32(u16(f.head, 18))
	f.bounds.Min.X = fixed.Int26_6(int16(u16(f.head, 36)))
	f.bounds.Min.Y = fixed.Int26_6(int16(u16(f.head, 38)))
	f.bounds.Max.X = fixed.Int26_6(int16(u16(f.head, 40)))
	f.bounds.Max.Y = fixed.Int26_6(int16(u16(f.head, 42)))

	switch i := u16(f.head, 50); i {
	case 0:
		f.locaOffsetFormat = locaOffsetFormatShort
	case 1:
		f.locaOffsetFormat = locaOffsetFormatLong
	default:
		return FormatError(fmt.Sprintf("bad indexToLocFormat: %d", i))
	}

	return nil
}

func (f *Font) parseHhea() error {
	if len(f.hhea) != 36 {
		return FormatError(fmt.Sprintf("bad hhea length: %d", len(f.hhea)))
	}

	f.ascent = int32(int16(u16(f.hhea, 4)))
	f.descent = int32(int16(u16(f.hhea, 6)))
	f.nHMetric = int(u16(f.hhea, 34))

	if 4*f.nHMetric+2*(f.nGlyph-f.nHMetric) != len(f.hmtx) {
		return FormatError(fmt.Sprintf("bad hmtx length: %d", len(f.hmtx)))
	}

	return nil
}

func (f *Font) parseKern() error {
	if len(f.kern) == 0 {
		if f.nKern != 0 {
			return FormatError("bad kern table length")
		}

		return nil
	}

	if len(f.kern) < 18 {
		return FormatError("kern data too short")
	}

	version, offset := u16(f.kern, 0), 2
	if version != 0 {
		return UnsupportedError(fmt.Sprintf("kern version: %d", version))
	}

	n, offset := u16(f.kern, offset), offset+2
	if n == 0 {
		return UnsupportedError("kern nTables: 0")
	}

	offset += 2
	length, offset := int(u16(f.kern, offset)), offset+2
	coverage, offset := u16(f.kern, offset), offset+2
	if coverage != 0x0001 {
		return UnsupportedError(fmt.Sprintf("kern coverage: 0x%04x", coverage))
	}

	f.nKern, offset = int(u16(f.kern, offset)), offset+2
	if 6*f.nKern != length-14 {
		return FormatError("bad kern table length")
	}

	return nil
}

func (f *Font) parseMaxp() error {
	if len(f.maxp) != 32 {
		return FormatError(fmt.Sprintf("bad maxp length: %d", len(f.maxp)))
	}

	f.nGlyph = int(u16(f.maxp, 4))
	f.maxTwilightPoints = u16(f.maxp, 16)
	f.maxStorage = u16(f.maxp, 18)
	f.maxFunctionDefs = u16(f.maxp, 20)
	f.maxStackElements = u16(f.maxp, 24)

	return nil
}

func (f *Font) scale(x fixed.Int26_6) fixed.Int26_6 {
	if x >= 0 {
		x += fixed.Int26_6(f.fUnitsPerEm) / 2
	} else {
		x -= fixed.Int26_6(f.fUnitsPerEm) / 2
	}

	return x / fixed.Int26_6(f.fUnitsPerEm)
}

func (f *Font) Bounds(scale fixed.Int26_6) fixed.Rectangle26_6 {
	b := f.bounds
	b.Min.X = f.scale(scale * b.Min.X)
	b.Min.Y = f.scale(scale * b.Min.Y)
	b.Max.X = f.scale(scale * b.Max.X)
	b.Max.Y = f.scale(scale * b.Max.Y)

	return b
}

func (f *Font) FUnitsPerEm() int32 {
	return f.fUnitsPerEm
}

func (f *Font) Index(x rune) Index {
	c := uint32(x)

	for i, j := 0, len(f.cm); i < j; {
		h := i + (j-i)/2
		cm := &f.cm[h]
		if c < cm.start {
			j = h
		} else if cm.end < c {
			i = h + 1
		} else if cm.offset == 0 {
			return Index(c + cm.delta)
		} else {
			offset := int(cm.offset) + 2*(h-len(f.cm)+int(c-cm.start))
			return Index(u16(f.cmapIndexes, offset))
		}
	}

	return 0
}

func (f *Font) Name(id NameID) string {
	x, platformID, err := parseSubtables(f.name, "name", 6, 12, func(b []byte) bool {
		return NameID(u16(b, 6)) == id
	})

	if err != nil {
		return ""
	}

	offset, length := u16(f.name, 4)+u16(f.name, x+10), u16(f.name, x+8)

	src := f.name[offset : offset+length]
	var dst []byte
	if platformID != 1 {
		if len(src)&1 != 0 {
			return ""
		}
		dst = make([]byte, len(src)/2)
		for i := range dst {
			dst[i] = printable(u16(src, 2*i))
		}
	} else {
		dst = make([]byte, len(src))
		for i, c := range src {
			dst[i] = printable(uint16(c))
		}
	}

	return string(dst)
}

func printable(r uint16) byte {
	if 0x20 <= r && r < 0x7f {
		return byte(r)
	}
	return '?'
}

func (f *Font) unscaledHMetric(i Index) (h HMetric) {
	j := int(i)
	if j < 0 || f.nGlyph <= j {
		return HMetric{}
	}
	if j >= f.nHMetric {
		p := 4 * (f.nHMetric - 1)
		return HMetric{
			AdvanceWidth:    fixed.Int26_6(u16(f.hmtx, p)),
			LeftSideBearing: fixed.Int26_6(int16(u16(f.hmtx, p+2*(j-f.nHMetric)+4))),
		}
	}
	return HMetric{
		AdvanceWidth:    fixed.Int26_6(u16(f.hmtx, 4*j)),
		LeftSideBearing: fixed.Int26_6(int16(u16(f.hmtx, 4*j+2))),
	}
}

func (f *Font) HMetric(scale fixed.Int26_6, i Index) HMetric {
	h := f.unscaledHMetric(i)
	h.AdvanceWidth = f.scale(scale * h.AdvanceWidth)
	h.LeftSideBearing = f.scale(scale * h.LeftSideBearing)
	return h
}

func (f *Font) unscaledVMetric(i Index, yMax fixed.Int26_6) (v VMetric) {
	j := int(i)

	if j < 0 || f.nGlyph <= j {
		return VMetric{}
	}

	if 4*j+4 <= len(f.vmtx) {
		return VMetric{
			AdvanceHeight:  fixed.Int26_6(u16(f.vmtx, 4*j)),
			TopSideBearing: fixed.Int26_6(int16(u16(f.vmtx, 4*j+2))),
		}
	}

	if len(f.os2) >= 72 {
		sTypoAscender := fixed.Int26_6(int16(u16(f.os2, 68)))
		sTypoDescender := fixed.Int26_6(int16(u16(f.os2, 70)))

		return VMetric{
			AdvanceHeight:  sTypoAscender - sTypoDescender,
			TopSideBearing: sTypoAscender - yMax,
		}
	}

	return VMetric{
		AdvanceHeight:  fixed.Int26_6(f.fUnitsPerEm),
		TopSideBearing: 0,
	}
}

func (f *Font) VMetric(scale fixed.Int26_6, i Index) VMetric {
	v := f.unscaledVMetric(i, 0)
	v.AdvanceHeight = f.scale(scale * v.AdvanceHeight)
	v.TopSideBearing = f.scale(scale * v.TopSideBearing)

	return v
}

func (f *Font) Kern(scale fixed.Int26_6, i0, i1 Index) fixed.Int26_6 {
	if f.nKern == 0 {
		return 0
	}

	g := uint32(i0)<<16 | uint32(i1)
	lo, hi := 0, f.nKern

	for lo < hi {
		i := (lo + hi) / 2
		ig := u32(f.kern, 18+6*i)

		if ig < g {
			lo = i + 1
		} else if ig > g {
			hi = i
		} else {
			return f.scale(scale * fixed.Int26_6(int16(u16(f.kern, 22+6*i))))
		}
	}

	return 0
}

func Parse(ttf []byte) (font *Font, err error) {
	return parse(ttf, 0)
}

func parse(ttf []byte, offset int) (font *Font, err error) {
	if len(ttf)-offset < 12 {
		err = FormatError("TTF data is too short")
		return
	}

	originalOffset := offset
	magic, offset := u32(ttf, offset), offset+4
	switch magic {
	case 0x00010000:
	case 0x74746366:
		if originalOffset != 0 {
			err = FormatError("recursive TTC")
			return
		}

		ttcVersion, offset := u32(ttf, offset), offset+4
		if ttcVersion != 0x00010000 && ttcVersion != 0x00020000 {
			err = FormatError("bad TTC version")
			return
		}

		numFonts, offset := int(u32(ttf, offset)), offset+4
		if numFonts <= 0 {
			err = FormatError("bad number of TTC fonts")
			return
		}

		if len(ttf[offset:])/4 < numFonts {
			err = FormatError("TTC offset table is too short")
			return
		}

		offset = int(u32(ttf, offset))
		if offset <= 0 || offset > len(ttf) {
			err = FormatError("bad TTC offset")
			return
		}
		return parse(ttf, offset)
	default:
		err = FormatError("bad TTF version")
		return
	}

	n, offset := int(u16(ttf, offset)), offset+2
	offset += 6
	if len(ttf) < 16*n+offset {
		err = FormatError("TTF data is too short")
		return
	}

	f := new(Font)

	for i := 0; i < n; i++ {
		x := 16*i + offset
		switch string(ttf[x : x+4]) {
		case "cmap":
			f.cmap, err = readTable(ttf, ttf[x+8:x+16])
		case "cvt ":
			f.cvt, err = readTable(ttf, ttf[x+8:x+16])
		case "fpgm":
			f.fpgm, err = readTable(ttf, ttf[x+8:x+16])
		case "glyf":
			f.glyf, err = readTable(ttf, ttf[x+8:x+16])
		case "hdmx":
			f.hdmx, err = readTable(ttf, ttf[x+8:x+16])
		case "head":
			f.head, err = readTable(ttf, ttf[x+8:x+16])
		case "hhea":
			f.hhea, err = readTable(ttf, ttf[x+8:x+16])
		case "hmtx":
			f.hmtx, err = readTable(ttf, ttf[x+8:x+16])
		case "kern":
			f.kern, err = readTable(ttf, ttf[x+8:x+16])
		case "loca":
			f.loca, err = readTable(ttf, ttf[x+8:x+16])
		case "maxp":
			f.maxp, err = readTable(ttf, ttf[x+8:x+16])
		case "name":
			f.name, err = readTable(ttf, ttf[x+8:x+16])
		case "OS/2":
			f.os2, err = readTable(ttf, ttf[x+8:x+16])
		case "prep":
			f.prep, err = readTable(ttf, ttf[x+8:x+16])
		case "vmtx":
			f.vmtx, err = readTable(ttf, ttf[x+8:x+16])
		}

		if err != nil {
			return
		}
	}

	if err = f.parseHead(); err != nil {
		return
	}

	if err = f.parseMaxp(); err != nil {
		return
	}

	if err = f.parseCmap(); err != nil {
		return
	}

	if err = f.parseKern(); err != nil {
		return
	}

	if err = f.parseHhea(); err != nil {
		return
	}

	font = f
	return
}
