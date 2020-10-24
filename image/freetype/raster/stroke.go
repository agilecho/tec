package raster

import "tec/image/fixed"

const epsilon = fixed.Int52_12(1024)

type Capper interface {
	Cap(p Adder, halfWidth fixed.Int26_6, pivot, n1 fixed.Point26_6)
}

type CapperFunc func(Adder, fixed.Int26_6, fixed.Point26_6, fixed.Point26_6)

func (f CapperFunc) Cap(p Adder, halfWidth fixed.Int26_6, pivot, n1 fixed.Point26_6) {
	f(p, halfWidth, pivot, n1)
}

type Joiner interface {
	Join(lhs, rhs Adder, halfWidth fixed.Int26_6, pivot, n0, n1 fixed.Point26_6)
}

type JoinerFunc func(lhs, rhs Adder, halfWidth fixed.Int26_6, pivot, n0, n1 fixed.Point26_6)

func (f JoinerFunc) Join(lhs, rhs Adder, halfWidth fixed.Int26_6, pivot, n0, n1 fixed.Point26_6) {
	f(lhs, rhs, halfWidth, pivot, n0, n1)
}

var RoundCapper Capper = CapperFunc(roundCapper)

func roundCapper(p Adder, halfWidth fixed.Int26_6, pivot, n1 fixed.Point26_6) {
	const k = 35
	e := pRot90CCW(n1)
	side := pivot.Add(e)
	start, end := pivot.Sub(n1), pivot.Add(n1)
	d, e := n1.Mul(k), e.Mul(k)
	p.Add3(start.Add(e), side.Sub(d), side)
	p.Add3(side.Add(d), end.Add(e), end)
}

var RoundJoiner Joiner = JoinerFunc(roundJoiner)

func roundJoiner(lhs, rhs Adder, haflWidth fixed.Int26_6, pivot, n0, n1 fixed.Point26_6) {
	dot := pDot(pRot90CW(n0), n1)
	if dot >= 0 {
		addArc(lhs, pivot, n0, n1)
		rhs.Add1(pivot.Sub(n1))
	} else {
		lhs.Add1(pivot.Add(n1))
		addArc(rhs, pivot, pNeg(n0), pNeg(n1))
	}
}

func addArc(p Adder, pivot, n0, n1 fixed.Point26_6) {
	r2 := pDot(n0, n0)
	if r2 < epsilon {
		p.Add1(pivot.Add(n1))
		return
	}

	const tpo8 = 27
	var s fixed.Point26_6
	m0 := pRot45CW(n0)
	m1 := pRot90CW(n0)
	m2 := pRot90CW(m0)
	if pDot(m1, n1) >= 0 {
		if pDot(n0, n1) >= 0 {
			if pDot(m2, n1) <= 0 {
				s = n0
			} else {
				p.Add2(pivot.Add(n0).Add(m1.Mul(tpo8)), pivot.Add(m0))
				s = m0
			}
		} else {
			pm1, n0t := pivot.Add(m1), n0.Mul(tpo8)
			p.Add2(pivot.Add(n0).Add(m1.Mul(tpo8)), pivot.Add(m0))
			p.Add2(pm1.Add(n0t), pm1)
			if pDot(m0, n1) >= 0 {
				s = m1
			} else {
				p.Add2(pm1.Sub(n0t), pivot.Add(m2))
				s = m2
			}
		}
	} else {
		if pDot(n0, n1) >= 0 {
			if pDot(m0, n1) >= 0 {
				s = n0
			} else {
				p.Add2(pivot.Add(n0).Sub(m1.Mul(tpo8)), pivot.Sub(m2))
				s = pNeg(m2)
			}
		} else {
			pm1, n0t := pivot.Sub(m1), n0.Mul(tpo8)
			p.Add2(pivot.Add(n0).Sub(m1.Mul(tpo8)), pivot.Sub(m2))
			p.Add2(pm1.Add(n0t), pm1)
			if pDot(m2, n1) <= 0 {
				s = pNeg(m1)
			} else {
				p.Add2(pm1.Sub(n0t), pivot.Sub(m0))
				s = pNeg(m0)
			}
		}
	}

	d := 256 * pDot(s, n1) / r2
	multiple := fixed.Int26_6(150-(150-128)*(d-181)/(256-181)) >> 2
	p.Add2(pivot.Add(s.Add(n1).Mul(multiple)), pivot.Add(n1))
}

func midpoint(a, b fixed.Point26_6) fixed.Point26_6 {
	return fixed.Point26_6{(a.X + b.X) / 2, (a.Y + b.Y) / 2}
}

func angleGreaterThan45(v0, v1 fixed.Point26_6) bool {
	v := pRot45CCW(v0)
	return pDot(v, v1) < 0 || pDot(pRot90CW(v), v1) < 0
}

func interpolate(a, b fixed.Point26_6, t fixed.Int52_12) fixed.Point26_6 {
	s := 1<<12 - t
	x := s*fixed.Int52_12(a.X) + t*fixed.Int52_12(b.X)
	y := s*fixed.Int52_12(a.Y) + t*fixed.Int52_12(b.Y)
	return fixed.Point26_6{fixed.Int26_6(x >> 12), fixed.Int26_6(y >> 12)}
}

func curviest2(a, b, c fixed.Point26_6) fixed.Int52_12 {
	dx := int64(b.X - a.X)
	dy := int64(b.Y - a.Y)
	ex := int64(c.X - 2*b.X + a.X)
	ey := int64(c.Y - 2*b.Y + a.Y)
	if ex == 0 && ey == 0 {
		return 2048
	}
	return fixed.Int52_12(-4096 * (dx*ex + dy*ey) / (ex*ex + ey*ey))
}

type stroker struct {
	p Adder
	u fixed.Int26_6
	cr Capper
	jr Joiner
	r Path
	a, anorm fixed.Point26_6
}

func (k *stroker) addNonCurvy2(b, c fixed.Point26_6) {
	const maxDepth = 5
	var (
		ds [maxDepth + 1]int
		ps [2*maxDepth + 3]fixed.Point26_6
		t  int
	)

	ds[0] = 0
	ps[2] = k.a
	ps[1] = b
	ps[0] = c
	anorm := k.anorm
	var cnorm fixed.Point26_6

	for {
		depth := ds[t]
		a := ps[2*t+2]
		b := ps[2*t+1]
		c := ps[2*t+0]
		ab := b.Sub(a)
		bc := c.Sub(b)
		abIsSmall := pDot(ab, ab) < fixed.Int52_12(1<<12)
		bcIsSmall := pDot(bc, bc) < fixed.Int52_12(1<<12)
		if abIsSmall && bcIsSmall {
			cnorm = pRot90CCW(pNorm(bc, k.u))
			mac := midpoint(a, c)
			addArc(k.p, mac, anorm, cnorm)
			addArc(&k.r, mac, pNeg(anorm), pNeg(cnorm))
		} else if depth < maxDepth && angleGreaterThan45(ab, bc) {
			mab := midpoint(a, b)
			mbc := midpoint(b, c)
			t++
			ds[t+0] = depth + 1
			ds[t-1] = depth + 1
			ps[2*t+2] = a
			ps[2*t+1] = mab
			ps[2*t+0] = midpoint(mab, mbc)
			ps[2*t-1] = mbc
			continue
		} else {
			bnorm := pRot90CCW(pNorm(c.Sub(a), k.u))
			cnorm = pRot90CCW(pNorm(bc, k.u))
			k.p.Add2(b.Add(bnorm), c.Add(cnorm))
			k.r.Add2(b.Sub(bnorm), c.Sub(cnorm))
		}

		if t == 0 {
			k.a, k.anorm = c, cnorm
			return
		}

		t--
		anorm = cnorm
	}
}

func (k *stroker) Add1(b fixed.Point26_6) {
	bnorm := pRot90CCW(pNorm(b.Sub(k.a), k.u))
	if len(k.r) == 0 {
		k.p.Start(k.a.Add(bnorm))
		k.r.Start(k.a.Sub(bnorm))
	} else {
		k.jr.Join(k.p, &k.r, k.u, k.a, k.anorm, bnorm)
	}
	k.p.Add1(b.Add(bnorm))
	k.r.Add1(b.Sub(bnorm))
	k.a, k.anorm = b, bnorm
}

func (k *stroker) Add2(b, c fixed.Point26_6) {
	ab := b.Sub(k.a)
	bc := c.Sub(b)
	abnorm := pRot90CCW(pNorm(ab, k.u))

	if len(k.r) == 0 {
		k.p.Start(k.a.Add(abnorm))
		k.r.Start(k.a.Sub(abnorm))
	} else {
		k.jr.Join(k.p, &k.r, k.u, k.a, k.anorm, abnorm)
	}

	abIsSmall := pDot(ab, ab) < epsilon
	bcIsSmall := pDot(bc, bc) < epsilon
	if abIsSmall || bcIsSmall {
		acnorm := pRot90CCW(pNorm(c.Sub(k.a), k.u))
		k.p.Add1(c.Add(acnorm))
		k.r.Add1(c.Sub(acnorm))
		k.a, k.anorm = c, acnorm
		return
	}

	t := curviest2(k.a, b, c)
	if t <= 0 || 4096 <= t {
		k.addNonCurvy2(b, c)
		return
	}

	mab := interpolate(k.a, b, t)
	mbc := interpolate(b, c, t)
	mabc := interpolate(mab, mbc, t)
	bcnorm := pRot90CCW(pNorm(bc, k.u))

	if pDot(abnorm, bcnorm) < -fixed.Int52_12(k.u)*fixed.Int52_12(k.u)*2047/2048 {
		pArc := pDot(abnorm, bc) < 0

		k.p.Add1(mabc.Add(abnorm))
		if pArc {
			z := pRot90CW(abnorm)
			addArc(k.p, mabc, abnorm, z)
			addArc(k.p, mabc, z, bcnorm)
		}
		k.p.Add1(mabc.Add(bcnorm))
		k.p.Add1(c.Add(bcnorm))

		k.r.Add1(mabc.Sub(abnorm))
		if !pArc {
			z := pRot90CW(abnorm)
			addArc(&k.r, mabc, pNeg(abnorm), z)
			addArc(&k.r, mabc, z, pNeg(bcnorm))
		}
		k.r.Add1(mabc.Sub(bcnorm))
		k.r.Add1(c.Sub(bcnorm))

		k.a, k.anorm = c, bcnorm
		return
	}

	k.addNonCurvy2(mab, mabc)
	k.addNonCurvy2(mbc, c)
}

func (k *stroker) Add3(b, c, d fixed.Point26_6) {
	panic("freetype/raster: stroke unimplemented for cubic segments")
}

func (k *stroker) stroke(q Path) {
	k.r = make(Path, 0, len(q))
	k.a = fixed.Point26_6{q[1], q[2]}
	for i := 4; i < len(q); {
		switch q[i] {
		case 1:
			k.Add1(
				fixed.Point26_6{q[i+1], q[i+2]},
			)
			i += 4
		case 2:
			k.Add2(
				fixed.Point26_6{q[i+1], q[i+2]},
				fixed.Point26_6{q[i+3], q[i+4]},
			)
			i += 6
		case 3:
			k.Add3(
				fixed.Point26_6{q[i+1], q[i+2]},
				fixed.Point26_6{q[i+3], q[i+4]},
				fixed.Point26_6{q[i+5], q[i+6]},
			)
			i += 8
		default:
			panic("freetype/raster: bad path")
		}
	}

	if len(k.r) == 0 {
		return
	}

	k.cr.Cap(k.p, k.u, q.lastPoint(), pNeg(k.anorm))
	addPathReversed(k.p, k.r)
	pivot := q.firstPoint()
	k.cr.Cap(k.p, k.u, pivot, pivot.Sub(fixed.Point26_6{k.r[1], k.r[2]}))
}

func Stroke(p Adder, q Path, width fixed.Int26_6, cr Capper, jr Joiner) {
	if len(q) == 0 {
		return
	}

	if cr == nil {
		cr = RoundCapper
	}

	if jr == nil {
		jr = RoundJoiner
	}

	if q[0] != 0 {
		panic("freetype/raster: bad path")
	}

	s := stroker{p: p, u: width / 2, cr: cr, jr: jr}
	i := 0

	for j := 4; j < len(q); {
		switch q[j] {
		case 0:
			s.stroke(q[i:j])
			i, j = j, j+4
		case 1:
			j += 4
		case 2:
			j += 6
		case 3:
			j += 8
		default:
			panic("freetype/raster: bad path")
		}
	}

	s.stroke(q[i:])
}
