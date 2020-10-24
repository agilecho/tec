package fixed

import "fmt"

func I(i int) Int26_6 {
	return Int26_6(i << 6)
}

type Int26_6 int32

func (x Int26_6) String() string {
	const shift, mask = 6, 1<<6 - 1
	if x >= 0 {
		return fmt.Sprintf("%d:%02d", int32(x>>shift), int32(x&mask))
	}

	x = -x
	if x >= 0 {
		return fmt.Sprintf("-%d:%02d", int32(x>>shift), int32(x&mask))
	}
	return "-33554432:00"
}

func (x Int26_6) Floor() int { return int((x + 0x00) >> 6) }
func (x Int26_6) Round() int { return int((x + 0x20) >> 6) }
func (x Int26_6) Ceil() int { return int((x + 0x3f) >> 6) }
func (x Int26_6) Mul(y Int26_6) Int26_6 {
	return Int26_6((int64(x)*int64(y) + 1<<5) >> 6)
}

type Int52_12 int64

func (x Int52_12) String() string {
	const shift, mask = 12, 1<<12 - 1
	if x >= 0 {
		return fmt.Sprintf("%d:%04d", int64(x>>shift), int64(x&mask))
	}

	x = -x
	if x >= 0 {
		return fmt.Sprintf("-%d:%04d", int64(x>>shift), int64(x&mask))
	}

	return "-2251799813685248:0000"
}

func (x Int52_12) Floor() int { return int((x + 0x000) >> 12) }
func (x Int52_12) Round() int { return int((x + 0x800) >> 12) }
func (x Int52_12) Ceil() int { return int((x + 0xfff) >> 12) }

func (x Int52_12) Mul(y Int52_12) Int52_12 {
	const M, N = 52, 12

	lo, hi := muli64(int64(x), int64(y))
	ret := Int52_12(hi<<M | lo>>N)
	ret += Int52_12((lo >> (N - 1)) & 1)

	return ret
}

func muli64(u, v int64) (lo, hi uint64) {
	const (
		s    = 32
		mask = 1<<s - 1
	)

	u1 := uint64(u >> s)
	u0 := uint64(u & mask)
	v1 := uint64(v >> s)
	v0 := uint64(v & mask)

	w0 := u0 * v0
	t := u1*v0 + w0>>s
	w1 := t & mask
	w2 := uint64(int64(t) >> s)
	w1 += u0 * v1

	return uint64(u) * uint64(v), u1*v1 + w2 + uint64(int64(w1)>>s)
}

func P(x, y int) Point26_6 {
	return Point26_6{Int26_6(x << 6), Int26_6(y << 6)}
}

type Point26_6 struct {
	X, Y Int26_6
}

func (p Point26_6) Add(q Point26_6) Point26_6 {
	return Point26_6{p.X + q.X, p.Y + q.Y}
}

func (p Point26_6) Sub(q Point26_6) Point26_6 {
	return Point26_6{p.X - q.X, p.Y - q.Y}
}

func (p Point26_6) Mul(k Int26_6) Point26_6 {
	return Point26_6{p.X * k / 64, p.Y * k / 64}
}

func (p Point26_6) Div(k Int26_6) Point26_6 {
	return Point26_6{p.X * 64 / k, p.Y * 64 / k}
}

func (p Point26_6) In(r Rectangle26_6) bool {
	return r.Min.X <= p.X && p.X < r.Max.X && r.Min.Y <= p.Y && p.Y < r.Max.Y
}

type Point52_12 struct {
	X, Y Int52_12
}

func (p Point52_12) Add(q Point52_12) Point52_12 {
	return Point52_12{p.X + q.X, p.Y + q.Y}
}

func (p Point52_12) Sub(q Point52_12) Point52_12 {
	return Point52_12{p.X - q.X, p.Y - q.Y}
}

func (p Point52_12) Mul(k Int52_12) Point52_12 {
	return Point52_12{p.X * k / 4096, p.Y * k / 4096}
}

func (p Point52_12) Div(k Int52_12) Point52_12 {
	return Point52_12{p.X * 4096 / k, p.Y * 4096 / k}
}

func (p Point52_12) In(r Rectangle52_12) bool {
	return r.Min.X <= p.X && p.X < r.Max.X && r.Min.Y <= p.Y && p.Y < r.Max.Y
}

func R(minX, minY, maxX, maxY int) Rectangle26_6 {
	if minX > maxX {
		minX, maxX = maxX, minX
	}

	if minY > maxY {
		minY, maxY = maxY, minY
	}

	return Rectangle26_6{
		Point26_6{
			Int26_6(minX << 6),
			Int26_6(minY << 6),
		},
		Point26_6{
			Int26_6(maxX << 6),
			Int26_6(maxY << 6),
		},
	}
}

type Rectangle26_6 struct {
	Min, Max Point26_6
}

func (r Rectangle26_6) Add(p Point26_6) Rectangle26_6 {
	return Rectangle26_6{
		Point26_6{r.Min.X + p.X, r.Min.Y + p.Y},
		Point26_6{r.Max.X + p.X, r.Max.Y + p.Y},
	}
}

func (r Rectangle26_6) Sub(p Point26_6) Rectangle26_6 {
	return Rectangle26_6{
		Point26_6{r.Min.X - p.X, r.Min.Y - p.Y},
		Point26_6{r.Max.X - p.X, r.Max.Y - p.Y},
	}
}

func (r Rectangle26_6) Intersect(s Rectangle26_6) Rectangle26_6 {
	if r.Min.X < s.Min.X {
		r.Min.X = s.Min.X
	}

	if r.Min.Y < s.Min.Y {
		r.Min.Y = s.Min.Y
	}

	if r.Max.X > s.Max.X {
		r.Max.X = s.Max.X
	}

	if r.Max.Y > s.Max.Y {
		r.Max.Y = s.Max.Y
	}

	if r.Empty() {
		return Rectangle26_6{}
	}

	return r
}

func (r Rectangle26_6) Union(s Rectangle26_6) Rectangle26_6 {
	if r.Empty() {
		return s
	}

	if s.Empty() {
		return r
	}

	if r.Min.X > s.Min.X {
		r.Min.X = s.Min.X
	}

	if r.Min.Y > s.Min.Y {
		r.Min.Y = s.Min.Y
	}

	if r.Max.X < s.Max.X {
		r.Max.X = s.Max.X
	}

	if r.Max.Y < s.Max.Y {
		r.Max.Y = s.Max.Y
	}

	return r
}

func (r Rectangle26_6) Empty() bool {
	return r.Min.X >= r.Max.X || r.Min.Y >= r.Max.Y
}

func (r Rectangle26_6) In(s Rectangle26_6) bool {
	if r.Empty() {
		return true
	}

	return s.Min.X <= r.Min.X && r.Max.X <= s.Max.X && s.Min.Y <= r.Min.Y && r.Max.Y <= s.Max.Y
}

type Rectangle52_12 struct {
	Min, Max Point52_12
}

func (r Rectangle52_12) Add(p Point52_12) Rectangle52_12 {
	return Rectangle52_12{
		Point52_12{r.Min.X + p.X, r.Min.Y + p.Y},
		Point52_12{r.Max.X + p.X, r.Max.Y + p.Y},
	}
}

func (r Rectangle52_12) Sub(p Point52_12) Rectangle52_12 {
	return Rectangle52_12{
		Point52_12{r.Min.X - p.X, r.Min.Y - p.Y},
		Point52_12{r.Max.X - p.X, r.Max.Y - p.Y},
	}
}

func (r Rectangle52_12) Intersect(s Rectangle52_12) Rectangle52_12 {
	if r.Min.X < s.Min.X {
		r.Min.X = s.Min.X
	}

	if r.Min.Y < s.Min.Y {
		r.Min.Y = s.Min.Y
	}

	if r.Max.X > s.Max.X {
		r.Max.X = s.Max.X
	}

	if r.Max.Y > s.Max.Y {
		r.Max.Y = s.Max.Y
	}

	if r.Empty() {
		return Rectangle52_12{}
	}

	return r
}

func (r Rectangle52_12) Union(s Rectangle52_12) Rectangle52_12 {
	if r.Empty() {
		return s
	}

	if s.Empty() {
		return r
	}

	if r.Min.X > s.Min.X {
		r.Min.X = s.Min.X
	}

	if r.Min.Y > s.Min.Y {
		r.Min.Y = s.Min.Y
	}

	if r.Max.X < s.Max.X {
		r.Max.X = s.Max.X
	}

	if r.Max.Y < s.Max.Y {
		r.Max.Y = s.Max.Y
	}

	return r
}

func (r Rectangle52_12) Empty() bool {
	return r.Min.X >= r.Max.X || r.Min.Y >= r.Max.Y
}

func (r Rectangle52_12) In(s Rectangle52_12) bool {
	if r.Empty() {
		return true
	}

	return s.Min.X <= r.Min.X && r.Max.X <= s.Max.X && s.Min.Y <= r.Min.Y && r.Max.Y <= s.Max.Y
}
