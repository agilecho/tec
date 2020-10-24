package truetype

const (
	opSVTCA0    = 0x00
	opSVTCA1    = 0x01
	opSPVTCA0   = 0x02
	opSPVTCA1   = 0x03
	opSFVTCA0   = 0x04
	opSFVTCA1   = 0x05
	opSPVTL0    = 0x06
	opSPVTL1    = 0x07
	opSFVTL0    = 0x08
	opSFVTL1    = 0x09
	opSPVFS     = 0x0a
	opSFVFS     = 0x0b
	opGPV       = 0x0c
	opGFV       = 0x0d
	opSFVTPV    = 0x0e
	opISECT     = 0x0f
	opSRP0      = 0x10
	opSRP1      = 0x11
	opSRP2      = 0x12
	opSZP0      = 0x13
	opSZP1      = 0x14
	opSZP2      = 0x15
	opSZPS      = 0x16
	opSLOOP     = 0x17
	opRTG       = 0x18
	opRTHG      = 0x19
	opSMD       = 0x1a
	opELSE      = 0x1b
	opJMPR      = 0x1c
	opSCVTCI    = 0x1d
	opSSWCI     = 0x1e
	opSSW       = 0x1f
	opDUP       = 0x20
	opPOP       = 0x21
	opCLEAR     = 0x22
	opSWAP      = 0x23
	opDEPTH     = 0x24
	opCINDEX    = 0x25
	opMINDEX    = 0x26
	opALIGNPTS  = 0x27
	opUTP       = 0x29
	opLOOPCALL  = 0x2a
	opCALL      = 0x2b
	opFDEF      = 0x2c
	opENDF      = 0x2d
	opMDAP0     = 0x2e
	opMDAP1     = 0x2f
	opIUP0      = 0x30
	opIUP1      = 0x31
	opSHP0      = 0x32
	opSHP1      = 0x33
	opSHC0      = 0x34
	opSHC1      = 0x35
	opSHZ0      = 0x36
	opSHZ1      = 0x37
	opSHPIX     = 0x38
	opIP        = 0x39
	opMSIRP0    = 0x3a
	opMSIRP1    = 0x3b
	opALIGNRP   = 0x3c
	opRTDG      = 0x3d
	opMIAP0     = 0x3e
	opMIAP1     = 0x3f
	opNPUSHB    = 0x40
	opNPUSHW    = 0x41
	opWS        = 0x42
	opRS        = 0x43
	opWCVTP     = 0x44
	opRCVT      = 0x45
	opGC0       = 0x46
	opGC1       = 0x47
	opSCFS      = 0x48
	opMD0       = 0x49
	opMD1       = 0x4a
	opMPPEM     = 0x4b
	opMPS       = 0x4c
	opFLIPON    = 0x4d
	opFLIPOFF   = 0x4e
	opDEBUG     = 0x4f
	opLT        = 0x50
	opLTEQ      = 0x51
	opGT        = 0x52
	opGTEQ      = 0x53
	opEQ        = 0x54
	opNEQ       = 0x55
	opODD       = 0x56
	opEVEN      = 0x57
	opIF        = 0x58
	opEIF       = 0x59
	opAND       = 0x5a
	opOR        = 0x5b
	opNOT       = 0x5c
	opDELTAP1   = 0x5d
	opSDB       = 0x5e
	opSDS       = 0x5f
	opADD       = 0x60
	opSUB       = 0x61
	opDIV       = 0x62
	opMUL       = 0x63
	opABS       = 0x64
	opNEG       = 0x65
	opFLOOR     = 0x66
	opCEILING   = 0x67
	opROUND00   = 0x68
	opROUND01   = 0x69
	opROUND10   = 0x6a
	opROUND11   = 0x6b
	opNROUND00  = 0x6c
	opNROUND01  = 0x6d
	opNROUND10  = 0x6e
	opNROUND11  = 0x6f
	opWCVTF     = 0x70
	opDELTAP2   = 0x71
	opDELTAP3   = 0x72
	opDELTAC1   = 0x73
	opDELTAC2   = 0x74
	opDELTAC3   = 0x75
	opSROUND    = 0x76
	opS45ROUND  = 0x77
	opJROT      = 0x78
	opJROF      = 0x79
	opROFF      = 0x7a
	opRUTG      = 0x7c
	opRDTG      = 0x7d
	opSANGW     = 0x7e
	opAA        = 0x7f
	opFLIPPT    = 0x80
	opFLIPRGON  = 0x81
	opFLIPRGOFF = 0x82
	opSCANCTRL  = 0x85
	opSDPVTL0   = 0x86
	opSDPVTL1   = 0x87
	opGETINFO   = 0x88
	opIDEF      = 0x89
	opROLL      = 0x8a
	opMAX       = 0x8b
	opMIN       = 0x8c
	opSCANTYPE  = 0x8d
	opINSTCTRL  = 0x8e
	opPUSHB000  = 0xb0
	opPUSHB001  = 0xb1
	opPUSHB010  = 0xb2
	opPUSHB011  = 0xb3
	opPUSHB100  = 0xb4
	opPUSHB101  = 0xb5
	opPUSHB110  = 0xb6
	opPUSHB111  = 0xb7
	opPUSHW000  = 0xb8
	opPUSHW001  = 0xb9
	opPUSHW010  = 0xba
	opPUSHW011  = 0xbb
	opPUSHW100  = 0xbc
	opPUSHW101  = 0xbd
	opPUSHW110  = 0xbe
	opPUSHW111  = 0xbf
	opMDRP00000 = 0xc0
	opMIRP00000 = 0xe0
)

var popCount = [256]uint8{
	0, 0, 0, 0, 0, 0, 2, 2, 2, 2, 2, 2, 0, 0, 0, 5,
	1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 1, 0, 1, 1, 1, 1,
	1, 1, 0, 2, 0, 1, 1, 2, 0, 1, 2, 1, 1, 0, 1, 1,
	0, 0, 0, 0, 1, 1, 1, 1, 1, 0, 2, 2, 0, 0, 2, 2,
	0, 0, 2, 1, 2, 1, 1, 1, 2, 2, 2, 0, 0, 0, 0, 0,
	2, 2, 2, 2, 2, 2, 1, 1, 1, 0, 2, 2, 1, 1, 1, 1,
	2, 2, 2, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
	2, 1, 1, 1, 1, 1, 1, 1, 2, 2, 0, 0, 0, 0, 1, 1,
	0, 2, 2, 0, 0, 1, 2, 2, 1, 1, 3, 2, 2, 1, 2, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
	2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
	2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
}
