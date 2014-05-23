package bloom

// storage is an interface every bloom filter backend storage needs to implement.
type storage interface {
	Append(uint)
	Save()
	Exists(uint) (bool, error)
}
