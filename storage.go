package bloom

type storage interface {
	Append(uint)
	Save()
	Exists(uint) (bool, error)
}
