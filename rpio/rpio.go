package rpio

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"reflect"
	"sync"
	"syscall"
	"time"
	"unsafe"
)

type Mode uint8
type Pin uint8
type State uint8
type Pull uint8
type Edge uint8

const (
	bcm2835Base = 0x20000000
	gpioOffset  = 0x200000
	clkOffset   = 0x101000
	pwmOffset   = 0x20C000
	spiOffset   = 0x204000
	intrOffset  = 0x00B000

	memLength = 4096
)

const (
	GPPUPPDN0 = 57
	GPPUPPDN1 = 58
	GPPUPPDN2 = 59
	GPPUPPDN3 = 60
)

var (
	gpioBase int64
	clkBase  int64
	pwmBase  int64
	spiBase  int64
	intrBase int64

	irqsBackup uint64
)

func init() {
	base := getBase()
	gpioBase = base + gpioOffset
	clkBase = base + clkOffset
	pwmBase = base + pwmOffset
	spiBase = base + spiOffset
	intrBase = base + intrOffset
}

const (
	Input Mode = iota
	Output
	Clock
	Pwm
	Spi
	Alt0
	Alt1
	Alt2
	Alt3
	Alt4
	Alt5
)

const (
	Low State = iota
	High
)

const (
	PullOff Pull = iota
	PullDown
	PullUp
	PullNone
)

const (
	NoEdge Edge = iota
	RiseEdge
	FallEdge
	AnyEdge = RiseEdge | FallEdge
)

var (
	memlock  sync.Mutex
	gpioMem  []uint32
	clkMem   []uint32
	pwmMem   []uint32
	spiMem   []uint32
	intrMem  []uint32
	gpioMem8 []uint8
	clkMem8  []uint8
	pwmMem8  []uint8
	spiMem8  []uint8
	intrMem8 []uint8
)

func (pin Pin) Input() {
	PinMode(pin, Input)
}

func (pin Pin) Output() {
	PinMode(pin, Output)
}

func (pin Pin) Clock() {
	PinMode(pin, Clock)
}

func (pin Pin) Pwm() {
	PinMode(pin, Pwm)
}

func (pin Pin) High() {
	WritePin(pin, High)
}

func (pin Pin) Low() {
	WritePin(pin, Low)
}

func (pin Pin) Toggle() {
	TogglePin(pin)
}

func (pin Pin) Freq(freq int) {
	SetFreq(pin, freq)
}

func (pin Pin) DutyCycle(dutyLen, cycleLen uint32) {
	SetDutyCycle(pin, dutyLen, cycleLen)
}

func (pin Pin) Mode(mode Mode) {
	PinMode(pin, mode)
}

func (pin Pin) Write(state State) {
	WritePin(pin, state)
}

func (pin Pin) Read() State {
	return ReadPin(pin)
}

func (pin Pin) Pull(pull Pull) {
	PullMode(pin, pull)
}

func (pin Pin) PullUp() {
	PullMode(pin, PullUp)
}

func (pin Pin) PullDown() {
	PullMode(pin, PullDown)
}

func (pin Pin) PullOff() {
	PullMode(pin, PullOff)
}

func (pin Pin) ReadPull() Pull {
	if !isBCM2711() {
		return PullNone
	}

	reg := GPPUPPDN0 + (uint8(pin) >> 4)
	bits := gpioMem[reg] >> ((uint8(pin) & 0xf) << 1) & 0x3
	switch bits {
	case 0:
		return PullOff
	case 1:
		return PullUp
	case 2:
		return PullDown
	default:
		return PullNone
	}
}

func (pin Pin) Detect(edge Edge) {
	DetectEdge(pin, edge)
}

func (pin Pin) EdgeDetected() bool {
	return EdgeDetected(pin)
}

func PinMode(pin Pin, mode Mode) {
	fselReg := uint8(pin) / 10
	shift := (uint8(pin) % 10) * 3
	f := uint32(0)

	const in = 0
	const out = 1
	const alt0 = 4
	const alt1 = 5
	const alt2 = 6
	const alt3 = 7
	const alt4 = 3
	const alt5 = 2

	switch mode {
	case Input:
		f = in
	case Output:
		f = out
	case Clock:
		switch pin {
		case 4, 5, 6, 32, 34, 42, 43, 44:
			f = alt0
		case 20, 21:
			f = alt5
		default:
			return
		}
	case Pwm:
		switch pin {
		case 12, 13, 40, 41, 45:
			f = alt0
		case 18, 19:
			f = alt5
		default:
			return
		}
	case Spi:
		switch pin {
		case 7, 8, 9, 10, 11:
			f = alt0
		case 35, 36, 37, 38, 39:
			f = alt0
		case 16, 17, 18, 19, 20, 21:
			f = alt4
		case 40, 41, 42, 43, 44, 45:
			f = alt4
		default:
			return
		}
	case Alt0:
		f = alt0
	case Alt1:
		f = alt1
	case Alt2:
		f = alt2
	case Alt3:
		f = alt3
	case Alt4:
		f = alt4
	case Alt5:
		f = alt5
	}

	memlock.Lock()
	defer memlock.Unlock()

	const pinMask = 7

	gpioMem[fselReg] = (gpioMem[fselReg] &^ (pinMask << shift)) | (f << shift)
}

func WritePin(pin Pin, state State) {
	p := uint8(pin)

	setReg := p/32 + 7
	clearReg := p/32 + 10

	memlock.Lock()

	if state == Low {
		gpioMem[clearReg] = 1 << (p & 31)
	} else {
		gpioMem[setReg] = 1 << (p & 31)
	}
	memlock.Unlock()
}

func ReadPin(pin Pin) State {
	levelReg := uint8(pin)/32 + 13

	if (gpioMem[levelReg] & (1 << uint8(pin&31))) != 0 {
		return High
	}

	return Low
}

func TogglePin(pin Pin) {
	p := uint8(pin)

	setReg := p/32 + 7
	clearReg := p/32 + 10
	levelReg := p/32 + 13

	bit := uint32(1 << (p & 31))

	memlock.Lock()

	if (gpioMem[levelReg] & bit) != 0 {
		gpioMem[clearReg] = bit
	} else {
		gpioMem[setReg] = bit
	}
	memlock.Unlock()
}

func DetectEdge(pin Pin, edge Edge) {
	if edge != NoEdge {
		DisableIRQs(1<<49 | 1<<52)
	}

	p := uint8(pin)

	renReg := p/32 + 19
	fenReg := p/32 + 22
	edsReg := p/32 + 16

	bit := uint32(1 << (p & 31))

	if edge&RiseEdge > 0 {
		gpioMem[renReg] |= bit
	} else {
		gpioMem[renReg] &^= bit
	}
	if edge&FallEdge > 0 {
		gpioMem[fenReg] |= bit
	} else {
		gpioMem[fenReg] &^= bit
	}

	gpioMem[edsReg] = bit
}

func EdgeDetected(pin Pin) bool {
	p := uint8(pin)

	edsReg := p/32 + 16

	test := gpioMem[edsReg] & (1 << (p & 31))
	gpioMem[edsReg] = test
	return test != 0
}

func PullMode(pin Pin, pull Pull) {
	memlock.Lock()
	defer memlock.Unlock()

	if isBCM2711() {
		pullreg := GPPUPPDN0 + (pin >> 4)
		pullshift := (pin & 0xf) << 1

		var p uint32

		switch pull {
		case PullOff:
			p = 0
		case PullUp:
			p = 1
		case PullDown:
			p = 2;
		}

		pullbits := gpioMem[pullreg]
		pullbits &= ^(3 << pullshift)
		pullbits |= (p << pullshift)
		gpioMem[pullreg]= pullbits
	} else {
		pullClkReg := pin/32 + 38
		pullReg := 37
		shift := pin % 32

		switch pull {
		case PullDown, PullUp:
			gpioMem[pullReg] |= uint32(pull)
		case PullOff:
			gpioMem[pullReg] &^= 3
		}

		time.Sleep(time.Microsecond)

		gpioMem[pullClkReg] = 1 << shift

		time.Sleep(time.Microsecond)

		gpioMem[pullReg] &^= 3
		gpioMem[pullClkReg] = 0
	}
}

func SetFreq(pin Pin, freq int) {
	const sourceFreq = 19200000
	const divMask = 4095

	divi := uint32(sourceFreq / freq)
	divf := uint32(((sourceFreq % freq) << 12) / freq)

	divi &= divMask
	divf &= divMask

	clkCtlReg := 28
	clkDivReg := 28
	switch pin {
	case 4, 20, 32, 34:
		clkCtlReg += 0
		clkDivReg += 1
	case 5, 21, 42, 44:
		clkCtlReg += 2
		clkDivReg += 3
	case 6, 43:
		clkCtlReg += 4
		clkDivReg += 5
	case 12, 13, 40, 41, 45, 18, 19:
		clkCtlReg += 12
		clkDivReg += 13
		StopPwm()
		defer StartPwm()
	default:
		return
	}

	mash := uint32(1 << 9)
	if divi < 2 || divf == 0 {
		mash = 0
	}

	memlock.Lock()
	defer memlock.Unlock()

	const PASSWORD = 0x5A000000
	const busy = 1 << 7
	const enab = 1 << 4
	const src = 1 << 0

	clkMem[clkCtlReg] = PASSWORD | (clkMem[clkCtlReg] &^ enab)
	for clkMem[clkCtlReg]&busy != 0 {
		time.Sleep(time.Microsecond * 10)
	}

	clkMem[clkCtlReg] = PASSWORD | mash | src
	clkMem[clkDivReg] = PASSWORD | (divi << 12) | divf

	time.Sleep(time.Microsecond * 10)

	clkMem[clkCtlReg] = PASSWORD | mash | src | enab
}

func SetDutyCycle(pin Pin, dutyLen, cycleLen uint32) {
	const pwmCtlReg = 0
	var (
		pwmDatReg uint
		pwmRngReg uint
		shift     uint
	)

	switch pin {
	case 12, 18, 40:
		pwmRngReg = 4
		pwmDatReg = 5
		shift = 0
	case 13, 19, 41, 45:
		pwmRngReg = 8
		pwmDatReg = 9
		shift = 8
	default:
		return
	}

	const ctlMask = 255
	const pwen = 1 << 0
	const msen = 1 << 7

	pwmMem[pwmCtlReg] = pwmMem[pwmCtlReg]&^(ctlMask<<shift) | msen<<shift | pwen<<shift
	pwmMem[pwmDatReg] = dutyLen
	pwmMem[pwmRngReg] = cycleLen

	time.Sleep(time.Microsecond * 10)
}

func StopPwm() {
	const pwmCtlReg = 0
	const pwen = 1
	pwmMem[pwmCtlReg] &^= pwen<<8 | pwen
}

func StartPwm() {
	const pwmCtlReg = 0
	const pwen = 1
	pwmMem[pwmCtlReg] |= pwen<<8 | pwen
}

func EnableIRQs(irqs uint64) {
	const irqEnable1 = 0x210 / 4
	const irqEnable2 = 0x214 / 4
	intrMem[irqEnable1] = uint32(irqs)
	intrMem[irqEnable2] = uint32(irqs >> 32)
}

func DisableIRQs(irqs uint64) {
	const irqDisable1 = 0x21C / 4
	const irqDisable2 = 0x220 / 4
	intrMem[irqDisable1] = uint32(irqs)
	intrMem[irqDisable2] = uint32(irqs >> 32)
}

func backupIRQs() {
	const irqEnable1 = 0x210 / 4
	const irqEnable2 = 0x214 / 4
	irqsBackup = uint64(intrMem[irqEnable2])<<32 | uint64(intrMem[irqEnable1])
}

func Open() (err error) {
	var file *os.File

	file, err = os.OpenFile("/dev/mem", os.O_RDWR|os.O_SYNC, 0)
	if os.IsPermission(err) {
		file, err = os.OpenFile("/dev/gpiomem", os.O_RDWR|os.O_SYNC, 0)
	}
	if err != nil {
		return
	}
	defer file.Close()

	memlock.Lock()
	defer memlock.Unlock()

	gpioMem, gpioMem8, err = memMap(file.Fd(), gpioBase)
	if err != nil {
		return
	}

	clkMem, clkMem8, err = memMap(file.Fd(), clkBase)
	if err != nil {
		return
	}

	pwmMem, pwmMem8, err = memMap(file.Fd(), pwmBase)
	if err != nil {
		return
	}

	spiMem, spiMem8, err = memMap(file.Fd(), spiBase)
	if err != nil {
		return
	}

	intrMem, intrMem8, err = memMap(file.Fd(), intrBase)
	if err != nil {
		return
	}

	backupIRQs()

	return nil
}

func memMap(fd uintptr, base int64) (mem []uint32, mem8 []byte, err error) {
	mem8, err = syscall.Mmap(
		int(fd),
		base,
		memLength,
		syscall.PROT_READ|syscall.PROT_WRITE,
		syscall.MAP_SHARED,
	)
	if err != nil {
		return
	}

	header := *(*reflect.SliceHeader)(unsafe.Pointer(&mem8))
	header.Len /= (32 / 8)
	header.Cap /= (32 / 8)
	mem = *(*[]uint32)(unsafe.Pointer(&header))
	return
}

func Close() error {
	EnableIRQs(irqsBackup)

	memlock.Lock()
	defer memlock.Unlock()
	for _, mem8 := range [][]uint8{gpioMem8, clkMem8, pwmMem8, spiMem8, intrMem8} {
		if err := syscall.Munmap(mem8); err != nil {
			return err
		}
	}
	return nil
}

func readBase(offset int64) (int64, error) {
	ranges, err := os.Open("/proc/device-tree/soc/ranges")
	defer ranges.Close()
	if err != nil {
		return 0, err
	}
	b := make([]byte, 4)
	n, err := ranges.ReadAt(b, offset)
	if n != 4 || err != nil {
		return 0, err
	}
	buf := bytes.NewReader(b)
	var out uint32
	err = binary.Read(buf, binary.BigEndian, &out)
	if err != nil {
		return 0, err
	}

	if out == 0 {
		return 0, errors.New("rpio: GPIO base address not found")
	}
	return int64(out), nil
}

func getBase() int64 {
	b, err := readBase(4)
	if err == nil {
		return b
	}

	b, err = readBase(8)
	if err == nil {
		return b
	}

	return int64(bcm2835Base)
}

func isBCM2711() bool {
	return gpioMem[GPPUPPDN3] != 0x6770696f
}