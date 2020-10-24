package raster

import (
	"tec/image/fixed"
	"strconv"
)

type cell struct {
	xi          int
	area, cover int
	next        int
}

type Rasterizer struct {
	UseNonZeroWinding bool
	Dx, Dy int
	width int
	splitScale2, splitScale3 int
	a fixed.Point26_6
	xi, yi      int
	area, cover int
	cell []cell
	cellIndex []int
	cellBuf      [256]cell
	cellIndexBuf [64]int
	spanBuf      [64]Span
}

func (r *Rasterizer) findCell() int {
	if r.yi < 0 || r.yi >= len(r.cellIndex) {
		return -1
	}
	xi := r.xi
	if xi < 0 {
		xi = -1
	} else if xi > r.width {
		xi = r.width
	}
	i, prev := r.cellIndex[r.yi], -1
	for i != -1 && r.cell[i].xi <= xi {
		if r.cell[i].xi == xi {
			return i
		}
		i, prev = r.cell[i].next, i
	}
	c := len(r.cell)
	if c == cap(r.cell) {
		buf := make([]cell, c, 4*c)
		copy(buf, r.cell)
		r.cell = buf[0 : c+1]
	} else {
		r.cell = r.cell[0 : c+1]
	}
	r.cell[c] = cell{xi, 0, 0, i}
	if prev == -1 {
		r.cellIndex[r.yi] = c
	} else {
		r.cell[prev].next = c
	}
	return c
}

func (r *Rasterizer) saveCell() {
	if r.area != 0 || r.cover != 0 {
		i := r.findCell()
		if i != -1 {
			r.cell[i].area += r.area
			r.cell[i].cover += r.cover
		}
		r.area = 0
		r.cover = 0
	}
}

func (r *Rasterizer) setCell(xi, yi int) {
	if r.xi != xi || r.yi != yi {
		r.saveCell()
		r.xi, r.yi = xi, yi
	}
}

func (r *Rasterizer) scan(yi int, x0, y0f, x1, y1f fixed.Int26_6) {
	x0i := int(x0) / 64
	x0f := x0 - fixed.Int26_6(64*x0i)
	x1i := int(x1) / 64
	x1f := x1 - fixed.Int26_6(64*x1i)

	if y0f == y1f {
		r.setCell(x1i, yi)
		return
	}
	dx, dy := x1-x0, y1f-y0f

	if x0i == x1i {
		r.area += int((x0f + x1f) * dy)
		r.cover += int(dy)
		return
	}

	var (
		p, q, edge0, edge1 fixed.Int26_6
		xiDelta            int
	)

	if dx > 0 {
		p, q = (64-x0f)*dy, dx
		edge0, edge1, xiDelta = 0, 64, 1
	} else {
		p, q = x0f*dy, -dx
		edge0, edge1, xiDelta = 64, 0, -1
	}

	yDelta, yRem := p/q, p%q
	if yRem < 0 {
		yDelta -= 1
		yRem += q
	}

	xi, y := x0i, y0f
	r.area += int((x0f + edge1) * yDelta)
	r.cover += int(yDelta)
	xi, y = xi+xiDelta, y+yDelta
	r.setCell(xi, yi)

	if xi != x1i {
		p = 64 * (y1f - y + yDelta)
		fullDelta, fullRem := p/q, p%q
		if fullRem < 0 {
			fullDelta -= 1
			fullRem += q
		}
		yRem -= q
		for xi != x1i {
			yDelta = fullDelta
			yRem += fullRem
			if yRem >= 0 {
				yDelta += 1
				yRem -= q
			}
			r.area += int(64 * yDelta)
			r.cover += int(yDelta)
			xi, y = xi+xiDelta, y+yDelta
			r.setCell(xi, yi)
		}
	}

	yDelta = y1f - y
	r.area += int((edge0 + x1f) * yDelta)
	r.cover += int(yDelta)
}

func (r *Rasterizer) Start(a fixed.Point26_6) {
	r.setCell(int(a.X/64), int(a.Y/64))
	r.a = a
}

func (r *Rasterizer) Add1(b fixed.Point26_6) {
	x0, y0 := r.a.X, r.a.Y
	x1, y1 := b.X, b.Y
	dx, dy := x1-x0, y1-y0

	y0i := int(y0) / 64
	y0f := y0 - fixed.Int26_6(64*y0i)
	y1i := int(y1) / 64
	y1f := y1 - fixed.Int26_6(64*y1i)

	if y0i == y1i {
		r.scan(y0i, x0, y0f, x1, y1f)
	} else if dx == 0 {
		var (
			edge0, edge1 fixed.Int26_6
			yiDelta      int
		)

		if dy > 0 {
			edge0, edge1, yiDelta = 0, 64, 1
		} else {
			edge0, edge1, yiDelta = 64, 0, -1
		}

		x0i, yi := int(x0)/64, y0i
		x0fTimes2 := (int(x0) - (64 * x0i)) * 2

		dcover := int(edge1 - y0f)
		darea := int(x0fTimes2 * dcover)
		r.area += darea
		r.cover += dcover
		yi += yiDelta
		r.setCell(x0i, yi)

		dcover = int(edge1 - edge0)
		darea = int(x0fTimes2 * dcover)
		for yi != y1i {
			r.area += darea
			r.cover += dcover
			yi += yiDelta
			r.setCell(x0i, yi)
		}

		dcover = int(y1f - edge0)
		darea = int(x0fTimes2 * dcover)
		r.area += darea
		r.cover += dcover
	} else {
		var (
			p, q, edge0, edge1 fixed.Int26_6
			yiDelta            int
		)

		if dy > 0 {
			p, q = (64-y0f)*dx, dy
			edge0, edge1, yiDelta = 0, 64, 1
		} else {
			p, q = y0f*dx, -dy
			edge0, edge1, yiDelta = 64, 0, -1
		}

		xDelta, xRem := p/q, p%q
		if xRem < 0 {
			xDelta -= 1
			xRem += q
		}

		x, yi := x0, y0i
		r.scan(yi, x, y0f, x+xDelta, edge1)
		x, yi = x+xDelta, yi+yiDelta
		r.setCell(int(x)/64, yi)
		if yi != y1i {
			p = 64 * dx
			fullDelta, fullRem := p/q, p%q

			if fullRem < 0 {
				fullDelta -= 1
				fullRem += q
			}

			xRem -= q
			for yi != y1i {
				xDelta = fullDelta
				xRem += fullRem
				if xRem >= 0 {
					xDelta += 1
					xRem -= q
				}
				r.scan(yi, x, edge0, x+xDelta, edge1)
				x, yi = x+xDelta, yi+yiDelta
				r.setCell(int(x)/64, yi)
			}
		}

		r.scan(yi, x, edge0, x1, y1f)
	}

	r.a = b
}

func (r *Rasterizer) Add2(b, c fixed.Point26_6) {
	dev := maxAbs(r.a.X-2*b.X+c.X, r.a.Y-2*b.Y+c.Y) / fixed.Int26_6(r.splitScale2)
	nsplit := 0
	for dev > 0 {
		dev /= 4
		nsplit++
	}

	const maxNsplit = 16
	if nsplit > maxNsplit {
		panic("freetype/raster: Add2 nsplit too large: " + strconv.Itoa(nsplit))
	}

	var (
		pStack [2*maxNsplit + 3]fixed.Point26_6
		sStack [maxNsplit + 1]int
		i      int
	)
	sStack[0] = nsplit
	pStack[0] = c
	pStack[1] = b
	pStack[2] = r.a
	for i >= 0 {
		s := sStack[i]
		p := pStack[2*i:]
		if s > 0 {
			mx := p[1].X
			p[4].X = p[2].X
			p[3].X = (p[4].X + mx) / 2
			p[1].X = (p[0].X + mx) / 2
			p[2].X = (p[1].X + p[3].X) / 2
			my := p[1].Y
			p[4].Y = p[2].Y
			p[3].Y = (p[4].Y + my) / 2
			p[1].Y = (p[0].Y + my) / 2
			p[2].Y = (p[1].Y + p[3].Y) / 2
			sStack[i] = s - 1
			sStack[i+1] = s - 1
			i++
		} else {
			midx := (p[0].X + 2*p[1].X + p[2].X) / 4
			midy := (p[0].Y + 2*p[1].Y + p[2].Y) / 4
			r.Add1(fixed.Point26_6{midx, midy})
			r.Add1(p[0])
			i--
		}
	}
}

func (r *Rasterizer) Add3(b, c, d fixed.Point26_6) {
	dev2 := maxAbs(r.a.X-3*(b.X+c.X)+d.X, r.a.Y-3*(b.Y+c.Y)+d.Y) / fixed.Int26_6(r.splitScale2)
	dev3 := maxAbs(r.a.X-2*b.X+d.X, r.a.Y-2*b.Y+d.Y) / fixed.Int26_6(r.splitScale3)
	nsplit := 0

	for dev2 > 0 || dev3 > 0 {
		dev2 /= 8
		dev3 /= 4
		nsplit++
	}

	const maxNsplit = 16
	if nsplit > maxNsplit {
		panic("freetype/raster: Add3 nsplit too large: " + strconv.Itoa(nsplit))
	}

	var (
		pStack [3*maxNsplit + 4]fixed.Point26_6
		sStack [maxNsplit + 1]int
		i      int
	)

	sStack[0] = nsplit
	pStack[0] = d
	pStack[1] = c
	pStack[2] = b
	pStack[3] = r.a
	for i >= 0 {
		s := sStack[i]
		p := pStack[3*i:]
		if s > 0 {
			m01x := (p[0].X + p[1].X) / 2
			m12x := (p[1].X + p[2].X) / 2
			m23x := (p[2].X + p[3].X) / 2
			p[6].X = p[3].X
			p[5].X = m23x
			p[1].X = m01x
			p[2].X = (m01x + m12x) / 2
			p[4].X = (m12x + m23x) / 2
			p[3].X = (p[2].X + p[4].X) / 2
			m01y := (p[0].Y + p[1].Y) / 2
			m12y := (p[1].Y + p[2].Y) / 2
			m23y := (p[2].Y + p[3].Y) / 2
			p[6].Y = p[3].Y
			p[5].Y = m23y
			p[1].Y = m01y
			p[2].Y = (m01y + m12y) / 2
			p[4].Y = (m12y + m23y) / 2
			p[3].Y = (p[2].Y + p[4].Y) / 2
			sStack[i] = s - 1
			sStack[i+1] = s - 1
			i++
		} else {
			midx := (p[0].X + 3*(p[1].X+p[2].X) + p[3].X) / 8
			midy := (p[0].Y + 3*(p[1].Y+p[2].Y) + p[3].Y) / 8
			r.Add1(fixed.Point26_6{midx, midy})
			r.Add1(p[0])
			i--
		}
	}
}

func (r *Rasterizer) AddPath(p Path) {
	for i := 0; i < len(p); {
		switch p[i] {
		case 0:
			r.Start(
				fixed.Point26_6{p[i+1], p[i+2]},
			)
			i += 4
		case 1:
			r.Add1(
				fixed.Point26_6{p[i+1], p[i+2]},
			)
			i += 4
		case 2:
			r.Add2(
				fixed.Point26_6{p[i+1], p[i+2]},
				fixed.Point26_6{p[i+3], p[i+4]},
			)
			i += 6
		case 3:
			r.Add3(
				fixed.Point26_6{p[i+1], p[i+2]},
				fixed.Point26_6{p[i+3], p[i+4]},
				fixed.Point26_6{p[i+5], p[i+6]},
			)
			i += 8
		default:
			panic("freetype/raster: bad path")
		}
	}
}

func (r *Rasterizer) AddStroke(q Path, width fixed.Int26_6, cr Capper, jr Joiner) {
	Stroke(r, q, width, cr, jr)
}

func (r *Rasterizer) areaToAlpha(area int) uint32 {
	a := (area + 1) >> 1
	if a < 0 {
		a = -a
	}

	alpha := uint32(a)
	if r.UseNonZeroWinding {
		if alpha > 0x0fff {
			alpha = 0x0fff
		}
	} else {
		alpha &= 0x1fff
		if alpha > 0x1000 {
			alpha = 0x2000 - alpha
		} else if alpha == 0x1000 {
			alpha = 0x0fff
		}
	}

	return alpha<<4 | alpha>>8
}

func (r *Rasterizer) Rasterize(p Painter) {
	r.saveCell()

	s := 0

	for yi := 0; yi < len(r.cellIndex); yi++ {
		xi, cover := 0, 0
		for c := r.cellIndex[yi]; c != -1; c = r.cell[c].next {
			if cover != 0 && r.cell[c].xi > xi {
				alpha := r.areaToAlpha(cover * 64 * 2)
				if alpha != 0 {
					xi0, xi1 := xi, r.cell[c].xi
					if xi0 < 0 {
						xi0 = 0
					}

					if xi1 >= r.width {
						xi1 = r.width
					}

					if xi0 < xi1 {
						r.spanBuf[s] = Span{yi + r.Dy, xi0 + r.Dx, xi1 + r.Dx, alpha}
						s++
					}
				}
			}

			cover += r.cell[c].cover
			alpha := r.areaToAlpha(cover*64*2 - r.cell[c].area)

			xi = r.cell[c].xi + 1

			if alpha != 0 {
				xi0, xi1 := r.cell[c].xi, xi
				if xi0 < 0 {
					xi0 = 0
				}

				if xi1 >= r.width {
					xi1 = r.width
				}

				if xi0 < xi1 {
					r.spanBuf[s] = Span{yi + r.Dy, xi0 + r.Dx, xi1 + r.Dx, alpha}
					s++
				}
			}

			if s > len(r.spanBuf)-2 {
				p.Paint(r.spanBuf[:s], false)
				s = 0
			}
		}
	}
	p.Paint(r.spanBuf[:s], true)
}

func (r *Rasterizer) Clear() {
	r.a = fixed.Point26_6{}
	r.xi = 0
	r.yi = 0
	r.area = 0
	r.cover = 0
	r.cell = r.cell[:0]
	for i := 0; i < len(r.cellIndex); i++ {
		r.cellIndex[i] = -1
	}
}

func (r *Rasterizer) SetBounds(width, height int) {
	if width < 0 {
		width = 0
	}

	if height < 0 {
		height = 0
	}

	ss2, ss3 := 32, 16
	if width > 24 || height > 24 {
		ss2, ss3 = 2*ss2, 2*ss3
		if width > 120 || height > 120 {
			ss2, ss3 = 2*ss2, 2*ss3
		}
	}

	r.width = width
	r.splitScale2 = ss2
	r.splitScale3 = ss3
	r.cell = r.cellBuf[:0]

	if height > len(r.cellIndexBuf) {
		r.cellIndex = make([]int, height)
	} else {
		r.cellIndex = r.cellIndexBuf[:height]
	}

	r.Clear()
}

func NewRasterizer(width, height int) *Rasterizer {
	r := new(Rasterizer)
	r.SetBounds(width, height)
	return r
}
