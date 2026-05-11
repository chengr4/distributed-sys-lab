package minirpc

type RemoteRequester interface {
	CallRemote(serviceMethod string, args any, reply any) error
}

type Dialer interface {
	Dial(addr string) (RemoteRequester, error)
}
