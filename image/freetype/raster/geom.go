package raster

import (
	"github.com/agilecho/tec/image/fixed"
	"fmt"
	"math"
)

func maxAbs(a, b fixed.Int26_6) fixed.Int26_6 {
	if a < 0 {
		a = -a
	}

	if b < 0 {
		b = -b
	}

	if a < b {
		return b
	}

	return a
}

func pNeg(p fixed.Point26_6) fixed.Point26_6 {
	return fixed.Point26_6{-p.X, -p.Y}
}

func pDot(p fixed.Point26_6, q fixed.Point26_6) fixed.Int52_12 {
	px, py := int64(p.X), int64(p.Y)
	qx, qy := int64(q.X), int64(q.Y)

	return fixed.Int52_12(px*qx + py*qy)
}

func pLen(p fixed.Point26_6) fixed.Int26_6 {
	x := float64(p.X)
	y := float64(p.Y)

	return fixed.Int26_6(math.Sqrt(x*x + y*y))
}

func pNorm(p fixed.Point26_6, length fixed.Int26_6) fixed.Point26_6 {
	d := pLen(p)
	if d == 0 {
		return fixed.Point26_6{}
	}

	s, t := int64(length), int64(d)
	x := int64(p.X) * s / t
	y := int64(p.Y) * s / t

	return fixed.Point26_6{fixed.Int26_6(x), fixed.Int26_6(y)}
}


func pRot45CW(p fixed.Point26_6) fixed.Point26_6 {
	px, py := int64(p.X), int64(p.Y)
	qx := (+px - py) * 181 / 256
	qy := (+px + py) * 181 / 256

	return fixed.Point26_6{fixed.Int26_6(qx), fixed.Int26_6(qy)}
}

func pRot90CW(p fixed.Point26_6) fixed.Point26_6 {
	return fixed.Point26_6{-p.Y, p.X}
}

func pRot45CCW(p fixed.Point26_6) fixed.Point26_6 {
	px, py := int64(p.X), int64(p.Y)
	qx := (+px + py) * 181 / 256
	qy := (-px + py) * 181 / 256

	return fixed.Point26_6{fixed.Int26_6(qx), fixed.Int26_6(qy)}
}

func pRot90CCW(p fixed.Point26_6) fixed.Point26_6 {
	return fixed.Point26_6{p.Y, -p.X}
}

type Adder interface {
	Start(a fixed.Point26_6)
	Add1(b fixed.Point26_6)
	Add2(b, c fixed.Point26_6)
	Add3(b, c, d fixed.Point26_6)
}

type Path []fixed.Int26_6

func (p Path) String() string {
	s := ""
	for i := 0; i < len(p); {
		if i != 0 {
			s += " "
		}
		switch p[i] {
		case 0:
			s += "S0" + fmt.Sprint([]fixed.Int26_6(p[i+1:i+3]))
			i += 4
		case 1:
			s += "A1" + fmt.Sprint([]fixed.Int26_6(p[i+1:i+3]))
			i += 4
		case 2:
			s += "A2" + fmt.Sprint([]fixed.Int26_6(p[i+1:i+5]))
			i += 6
		case 3:
			s += "A3" + fmt.Sprint([]fixed.Int26_6(p[i+1:i+7]))
			i += 8
		default:
			panic("freetype/raster: bad path")
		}
	}

	return s
}

func (p *Path) Clear() {
	*p = (*p)[:0]
}

func (p *Path) Start(a fixed.Point26_6) {
	*p = append(*p, 0, a.X, a.Y, 0)
}

func (p *Path) Add1(b fixed.Point26_6) {
	*p = append(*p, 1, b.X, b.Y, 1)
}

func (p *Path) Add2(b, c fixed.Point26_6) {
	*p = append(*p, 2, b.X, b.Y, c.X, c.Y, 2)
}

func (p *Path) Add3(b, c, d fixed.Point26_6) {
	*p = append(*p, 3, b.X, b.Y, c.X, c.Y, d.X, d.Y, 3)
}

func (p *Path) AddPath(q Path) {
	*p = append(*p, q...)
}

func (p *Path) AddStroke(q Path, width fixed.Int26_6, cr Capper, jr Joiner) {
	Stroke(p, q, width, cr, jr)
}

func (p Path) firstPoint() fixed.Point26_6 {
	return fixed.Point26_6{p[1], p[2]}
}

func (p Path) lastPoint() fixed.Point26_6 {
	return fixed.Point26_6{p[len(p)-3], p[len(p)-2]}
}

func addPathReversed(p Adder, q Path) {
	if len(q) == 0 {
		return
	}

	i := len(q) - 1
	for {
		switch q[i] {
		case 0:
			return
		case 1:
			i -= 4
			p.Add1(
				fixed.Point26_6{q[i-2], q[i-1]},
			)
		case 2:
			i -= 6
			p.Add2(
				fixed.Point26_6{q[i+2], q[i+3]},
				fixed.Point26_6{q[i-2], q[i-1]},
			)
		case 3:
			i -= 8
			p.Add3(
				fixed.Point26_6{q[i+4], q[i+5]},
				fixed.Point26_6{q[i+2], q[i+3]},
				fixed.Point26_6{q[i-2], q[i-1]},
			)
		default:
			panic("freetype/raster: bad path")
		}
	}
}
