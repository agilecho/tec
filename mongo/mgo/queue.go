package mgo

type queue struct {
	elems []interface{}
	nelems, popi, pushi int
}

func (q *queue) Len() int {
	return q.nelems
}

func (q *queue) Push(elem interface{}) {
	if q.nelems == len(q.elems) {
		q.expand()
	}
	q.elems[q.pushi] = elem
	q.nelems++
	q.pushi = (q.pushi + 1) % len(q.elems)
}

func (q *queue) Pop() (elem interface{}) {
	if q.nelems == 0 {
		return nil
	}
	elem = q.elems[q.popi]
	q.elems[q.popi] = nil
	q.nelems--
	q.popi = (q.popi + 1) % len(q.elems)
	return elem
}

func (q *queue) expand() {
	curcap := len(q.elems)
	var newcap int
	if curcap == 0 {
		newcap = 8
	} else if curcap < 1024 {
		newcap = curcap * 2
	} else {
		newcap = curcap + (curcap / 4)
	}
	elems := make([]interface{}, newcap)

	if q.popi == 0 {
		copy(elems, q.elems)
		q.pushi = curcap
	} else {
		newpopi := newcap - (curcap - q.popi)
		copy(elems, q.elems[:q.popi])
		copy(elems[newpopi:], q.elems[q.popi:])
		q.popi = newpopi
	}
	for i := range q.elems {
		q.elems[i] = nil
	}
	q.elems = elems
}
