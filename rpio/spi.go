package rpio

import (
	"errors"
)

type SpiDev int

const (
	Spi0 SpiDev = iota
	Spi1
	Spi2
)

const (
	csReg     = 0
	fifoReg   = 1
	clkDivReg = 2
)

var (
	SpiMapError = errors.New("SPI registers not mapped correctly - are you root?")
)

func SpiBegin(dev SpiDev) error {
	spiMem[csReg] = 0
	if spiMem[csReg] == 0 {
		return SpiMapError
	}

	for _, pin := range getSpiPins(dev) {
		pin.Mode(Spi)
	}

	clearSpiTxRxFifo()
	setSpiDiv(128)
	return nil
}

func SpiEnd(dev SpiDev) {
	var pins = getSpiPins(dev)
	for _, pin := range pins {
		pin.Mode(Input)
	}
}

func SpiSpeed(speed int) {
	const baseFreq = 250 * 1000000
	cdiv := uint32(baseFreq / speed)
	setSpiDiv(cdiv)
}

func SpiChipSelect(chip uint8) {
	const csMask = 3

	cs := uint32(chip & csMask)

	spiMem[csReg] = spiMem[csReg]&^csMask | cs
}

func SpiChipSelectPolarity(chip uint8, polarity uint8) {
	if chip > 2 {
		return
	}
	cspol := uint32(1 << (21 + chip))

	if polarity == 0 {
		spiMem[csReg] &^= cspol
	} else {
		spiMem[csReg] |= cspol
	}
}

func SpiMode(polarity uint8, phase uint8) {
	const cpol = 1 << 3
	const cpha = 1 << 2

	if polarity == 0 {
		spiMem[csReg] &^= cpol
	} else {
		spiMem[csReg] |= cpol
	}

	if phase == 0 {
		spiMem[csReg] &^= cpha
	} else {
		spiMem[csReg] |= cpha
	}
}

func SpiTransmit(data ...byte) {
	SpiExchange(append(data[:0:0], data...))
}

func SpiReceive(n int) []byte {
	data := make([]byte, n, n)
	SpiExchange(data)
	return data
}

func SpiExchange(data []byte) {
	const ta = 1 << 7
	const txd = 1 << 18
	const rxd = 1 << 17
	const done = 1 << 16

	clearSpiTxRxFifo()

	spiMem[csReg] |= ta

	for i := range data {
		for spiMem[csReg]&txd == 0 {
		}

		spiMem[fifoReg] = uint32(data[i])

		for spiMem[csReg]&rxd == 0 {
		}

		data[i] = byte(spiMem[fifoReg])
	}

	for spiMem[csReg]&done == 0 {
	}

	spiMem[csReg] &^= ta
}

func setSpiDiv(div uint32) {
	const divMask = 1<<16 - 1 - 1
	spiMem[clkDivReg] = div & divMask
}

func clearSpiTxRxFifo() {
	const clearTxRx = 1<<5 | 1<<4
	spiMem[csReg] |= clearTxRx
}

func getSpiPins(dev SpiDev) []Pin {
	switch dev {
	case Spi0:
		return []Pin{7, 8, 9, 10, 11}
	case Spi1:
		return []Pin{16, 17, 18, 19, 20, 21}
	case Spi2:
		return []Pin{40, 41, 42, 43, 44, 45}
	default:
		return []Pin{}
	}
}
