package bloom

type storage interface {
	Append(*[]uint)
	Save()
}

func NewStorage(label string, size uint) (store storage, err error) {
	switch label {
	case "bitset":
		store = NewBitsetStorage(size)
		break
	}

	return
}
