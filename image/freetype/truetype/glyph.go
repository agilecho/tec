package truetype

import (
	"tec/image/fixed"
	"tec/image/font"
)

type Point struct {
	X, Y fixed.Int26_6
	Flags uint32
}

type GlyphBuf struct {
	AdvanceWidth fixed.Int26_6
	Bounds fixed.Rectangle26_6
	Points, Unhinted, InFontUnits []Point
	Ends []int
	font    *Font
	scale   fixed.Int26_6
	hinting font.Hinting
	hinter  hinter
	phantomPoints [4]Point
	pp1x fixed.Int26_6
	metricsSet bool
	tmp []Point
}

const (
	flagOnCurve = 1 << iota
	flagXShortVector
	flagYShortVector
	flagRepeat
	flagPositiveXShortVector
	flagPositiveYShortVector
	flagTouchedX
	flagTouchedY
)

const (
	flagThisXIsSame = flagPositiveXShortVector
	flagThisYIsSame = flagPositiveYShortVector
)

func (g *GlyphBuf) Load(f *Font, scale fixed.Int26_6, i Index, h font.Hinting) error {
	g.Points = g.Points[:0]
	g.Unhinted = g.Unhinted[:0]
	g.InFontUnits = g.InFontUnits[:0]
	g.Ends = g.Ends[:0]
	g.font = f
	g.hinting = h
	g.scale = scale
	g.pp1x = 0
	g.phantomPoints = [4]Point{}
	g.metricsSet = false

	if h != font.HintingNone {
		if err := g.hinter.init(f, scale); err != nil {
			return err
		}
	}

	if err := g.load(0, i, true); err != nil {
		return err
	}

	pp1x := g.pp1x
	if h != font.HintingNone {
		pp1x = g.phantomPoints[0].X
	}

	if pp1x != 0 {
		for i := range g.Points {
			g.Points[i].X -= pp1x
		}
	}

	advanceWidth := g.phantomPoints[1].X - g.phantomPoints[0].X
	if h != font.HintingNone {
		if len(f.hdmx) >= 8 {
			if n := u32(f.hdmx, 4); n > 3+uint32(i) {
				for hdmx := f.hdmx[8:]; uint32(len(hdmx)) >= n; hdmx = hdmx[n:] {
					if fixed.Int26_6(hdmx[0]) == scale>>6 {
						advanceWidth = fixed.Int26_6(hdmx[2+i]) << 6
						break
					}
				}
			}
		}

		advanceWidth = (advanceWidth + 32) &^ 63
	}

	g.AdvanceWidth = advanceWidth

	if len(g.Points) == 0 {
		g.Bounds = fixed.Rectangle26_6{}
	} else {
		p := g.Points[0]

		g.Bounds.Min.X = p.X
		g.Bounds.Max.X = p.X
		g.Bounds.Min.Y = p.Y
		g.Bounds.Max.Y = p.Y

		for _, p := range g.Points[1:] {
			if g.Bounds.Min.X > p.X {
				g.Bounds.Min.X = p.X
			} else if g.Bounds.Max.X < p.X {
				g.Bounds.Max.X = p.X
			}
			if g.Bounds.Min.Y > p.Y {
				g.Bounds.Min.Y = p.Y
			} else if g.Bounds.Max.Y < p.Y {
				g.Bounds.Max.Y = p.Y
			}
		}

		if h != font.HintingNone {
			g.Bounds.Min.X &^= 63
			g.Bounds.Min.Y &^= 63
			g.Bounds.Max.X += 63
			g.Bounds.Max.X &^= 63
			g.Bounds.Max.Y += 63
			g.Bounds.Max.Y &^= 63
		}
	}

	return nil
}

func (g *GlyphBuf) load(recursion uint32, i Index, useMyMetrics bool) (err error) {
	if recursion >= 32 {
		return UnsupportedError("excessive compound glyph recursion")
	}

	var g0, g1 uint32

	if g.font.locaOffsetFormat == locaOffsetFormatShort {
		g0 = 2 * uint32(u16(g.font.loca, 2*int(i)))
		g1 = 2 * uint32(u16(g.font.loca, 2*int(i)+2))
	} else {
		g0 = u32(g.font.loca, 4*int(i))
		g1 = u32(g.font.loca, 4*int(i)+4)
	}

	glyf, ne, boundsXMin, boundsYMax := []byte(nil), 0, fixed.Int26_6(0), fixed.Int26_6(0)
	if g0+10 <= g1 {
		glyf = g.font.glyf[g0:g1]
		ne = int(int16(u16(glyf, 0)))
		boundsXMin = fixed.Int26_6(int16(u16(glyf, 2)))
		boundsYMax = fixed.Int26_6(int16(u16(glyf, 8)))
	}

	uhm, pp1x := g.font.unscaledHMetric(i), fixed.Int26_6(0)
	uvm := g.font.unscaledVMetric(i, boundsYMax)

	g.phantomPoints = [4]Point{
		{X: boundsXMin - uhm.LeftSideBearing},
		{X: boundsXMin - uhm.LeftSideBearing + uhm.AdvanceWidth},
		{X: uhm.AdvanceWidth / 2, Y: boundsYMax + uvm.TopSideBearing},
		{X: uhm.AdvanceWidth / 2, Y: boundsYMax + uvm.TopSideBearing - uvm.AdvanceHeight},
	}

	if len(glyf) == 0 {
		g.addPhantomsAndScale(len(g.Points), len(g.Points), true, true)
		copy(g.phantomPoints[:], g.Points[len(g.Points)-4:])
		g.Points = g.Points[:len(g.Points)-4]

		return nil
	}

	if ne < 0 {
		if ne != -1 {
			return UnsupportedError("negative number of contours")
		}

		pp1x = g.font.scale(g.scale * (boundsXMin - uhm.LeftSideBearing))

		if err := g.loadCompound(recursion, uhm, i, glyf, useMyMetrics); err != nil {
			return err
		}

	} else {
		np0, ne0 := len(g.Points), len(g.Ends)
		program := g.loadSimple(glyf, ne)
		g.addPhantomsAndScale(np0, np0, true, true)
		pp1x = g.Points[len(g.Points)-4].X

		if g.hinting != font.HintingNone {
			if len(program) != 0 {
				err := g.hinter.run(
					program,
					g.Points[np0:],
					g.Unhinted[np0:],
					g.InFontUnits[np0:],
					g.Ends[ne0:],
				)
				if err != nil {
					return err
				}
			}

			g.InFontUnits = g.InFontUnits[:len(g.InFontUnits)-4]
			g.Unhinted = g.Unhinted[:len(g.Unhinted)-4]
		}

		if useMyMetrics {
			copy(g.phantomPoints[:], g.Points[len(g.Points)-4:])
		}

		g.Points = g.Points[:len(g.Points)-4]
		if np0 != 0 {
			for i := ne0; i < len(g.Ends); i++ {
				g.Ends[i] += np0
			}
		}
	}

	if useMyMetrics && !g.metricsSet {
		g.metricsSet = true
		g.pp1x = pp1x
	}

	return nil
}

const loadOffset = 10

func (g *GlyphBuf) loadSimple(glyf []byte, ne int) (program []byte) {
	offset := loadOffset
	for i := 0; i < ne; i++ {
		g.Ends = append(g.Ends, 1+int(u16(glyf, offset)))
		offset += 2
	}

	instrLen := int(u16(glyf, offset))
	offset += 2
	program = glyf[offset : offset+instrLen]
	offset += instrLen

	if ne == 0 {
		return program
	}

	np0 := len(g.Points)
	np1 := np0 + int(g.Ends[len(g.Ends)-1])

	for i := np0; i < np1; {
		c := uint32(glyf[offset])
		offset++
		g.Points = append(g.Points, Point{Flags: c})
		i++
		if c&flagRepeat != 0 {
			count := glyf[offset]
			offset++
			for ; count > 0; count-- {
				g.Points = append(g.Points, Point{Flags: c})
				i++
			}
		}
	}

	var x int16
	for i := np0; i < np1; i++ {
		f := g.Points[i].Flags
		if f&flagXShortVector != 0 {
			dx := int16(glyf[offset])
			offset++
			if f&flagPositiveXShortVector == 0 {
				x -= dx
			} else {
				x += dx
			}
		} else if f&flagThisXIsSame == 0 {
			x += int16(u16(glyf, offset))
			offset += 2
		}

		g.Points[i].X = fixed.Int26_6(x)
	}

	var y int16
	for i := np0; i < np1; i++ {
		f := g.Points[i].Flags
		if f&flagYShortVector != 0 {
			dy := int16(glyf[offset])
			offset++
			if f&flagPositiveYShortVector == 0 {
				y -= dy
			} else {
				y += dy
			}
		} else if f&flagThisYIsSame == 0 {
			y += int16(u16(glyf, offset))
			offset += 2
		}

		g.Points[i].Y = fixed.Int26_6(y)
	}

	return program
}

func (g *GlyphBuf) loadCompound(recursion uint32, uhm HMetric, i Index, glyf []byte, useMyMetrics bool) error {
	const (
		flagArg1And2AreWords = 1 << iota
		flagArgsAreXYValues
		flagRoundXYToGrid
		flagWeHaveAScale
		flagMoreComponents
		flagWeHaveAnXAndYScale
		flagWeHaveATwoByTwo
		flagUseMyMetrics
	)

	np0, ne0 := len(g.Points), len(g.Ends)
	offset := loadOffset

	for {
		flags := u16(glyf, offset)
		component := Index(u16(glyf, offset+2))
		dx, dy, transform, hasTransform := fixed.Int26_6(0), fixed.Int26_6(0), [4]int16{}, false

		if flags&flagArg1And2AreWords != 0 {
			dx = fixed.Int26_6(int16(u16(glyf, offset+4)))
			dy = fixed.Int26_6(int16(u16(glyf, offset+6)))
			offset += 8
		} else {
			dx = fixed.Int26_6(int16(int8(glyf[offset+4])))
			dy = fixed.Int26_6(int16(int8(glyf[offset+5])))
			offset += 6
		}

		if flags&flagArgsAreXYValues == 0 {
			return UnsupportedError("compound glyph transform vector")
		}

		if flags&(flagWeHaveAScale|flagWeHaveAnXAndYScale|flagWeHaveATwoByTwo) != 0 {
			hasTransform = true
			switch {
			case flags&flagWeHaveAScale != 0:
				transform[0] = int16(u16(glyf, offset+0))
				transform[3] = transform[0]
				offset += 2
			case flags&flagWeHaveAnXAndYScale != 0:
				transform[0] = int16(u16(glyf, offset+0))
				transform[3] = int16(u16(glyf, offset+2))
				offset += 4
			case flags&flagWeHaveATwoByTwo != 0:
				transform[0] = int16(u16(glyf, offset+0))
				transform[1] = int16(u16(glyf, offset+2))
				transform[2] = int16(u16(glyf, offset+4))
				transform[3] = int16(u16(glyf, offset+6))
				offset += 8
			}
		}

		savedPP := g.phantomPoints
		np0 := len(g.Points)
		componentUMM := useMyMetrics && (flags&flagUseMyMetrics != 0)

		if err := g.load(recursion+1, component, componentUMM); err != nil {
			return err
		}

		if flags&flagUseMyMetrics == 0 {
			g.phantomPoints = savedPP
		}

		if hasTransform {
			for j := np0; j < len(g.Points); j++ {
				p := &g.Points[j]
				newX := 0 +
					fixed.Int26_6((int64(p.X)*int64(transform[0])+1<<13)>>14) +
					fixed.Int26_6((int64(p.Y)*int64(transform[2])+1<<13)>>14)
				newY := 0 +
					fixed.Int26_6((int64(p.X)*int64(transform[1])+1<<13)>>14) +
					fixed.Int26_6((int64(p.Y)*int64(transform[3])+1<<13)>>14)
				p.X, p.Y = newX, newY
			}
		}

		dx = g.font.scale(g.scale * dx)
		dy = g.font.scale(g.scale * dy)

		if flags&flagRoundXYToGrid != 0 {
			dx = (dx + 32) &^ 63
			dy = (dy + 32) &^ 63
		}

		for j := np0; j < len(g.Points); j++ {
			p := &g.Points[j]
			p.X += dx
			p.Y += dy
		}

		if flags&flagMoreComponents == 0 {
			break
		}
	}

	instrLen := 0
	if g.hinting != font.HintingNone && offset+2 <= len(glyf) {
		instrLen = int(u16(glyf, offset))
		offset += 2
	}

	g.addPhantomsAndScale(np0, len(g.Points), false, instrLen > 0)
	points, ends := g.Points[np0:], g.Ends[ne0:]
	g.Points = g.Points[:len(g.Points)-4]
	for j := range points {
		points[j].Flags &^= flagTouchedX | flagTouchedY
	}

	if instrLen == 0 {
		if !g.metricsSet {
			copy(g.phantomPoints[:], points[len(points)-4:])
		}
		return nil
	}

	program := glyf[offset : offset+instrLen]
	if np0 != 0 {
		for i := range ends {
			ends[i] -= np0
		}
	}

	g.tmp = append(g.tmp[:0], points...)
	if err := g.hinter.run(program, points, g.tmp, g.tmp, ends); err != nil {
		return err
	}

	if np0 != 0 {
		for i := range ends {
			ends[i] += np0
		}
	}

	if !g.metricsSet {
		copy(g.phantomPoints[:], points[len(points)-4:])
	}

	return nil
}

func (g *GlyphBuf) addPhantomsAndScale(np0, np1 int, simple, adjust bool) {
	g.Points = append(g.Points, g.phantomPoints[:]...)

	if simple && g.hinting != font.HintingNone {
		g.InFontUnits = append(g.InFontUnits, g.Points[np1:]...)
	}

	for i := np1; i < len(g.Points); i++ {
		p := &g.Points[i]
		p.X = g.font.scale(g.scale * p.X)
		p.Y = g.font.scale(g.scale * p.Y)
	}

	if g.hinting == font.HintingNone {
		return
	}

	if adjust {
		pp1x := g.Points[len(g.Points)-4].X
		if dx := ((pp1x + 32) &^ 63) - pp1x; dx != 0 {
			for i := np0; i < len(g.Points); i++ {
				g.Points[i].X += dx
			}
		}
	}

	if simple {
		g.Unhinted = append(g.Unhinted, g.Points[np1:]...)
	}

	p := &g.Points[len(g.Points)-3]
	p.X = (p.X + 32) &^ 63
	p = &g.Points[len(g.Points)-1]
	p.Y = (p.Y + 32) &^ 63
}
